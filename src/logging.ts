import { mkdir, open, readdir, rename, stat, unlink, utimes, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogService = "app" | "collector";
export interface LogFields { operation: string; message: string; sourceId?: string; participantId?: string; jobId?: string; outcome?: string; metadata?: Record<string, unknown>; }
export interface LoggerOptions { service: LogService; dataDirectory?: string; minimumLevel?: LogLevel; maxFileBytes?: number; maxMetadataBytes?: number; maxRecordBytes?: number; maxQueueEntries?: number; maxRetainedFiles?: number; retentionDays?: number; }
export interface LoggerStatus { destinationAvailable: boolean; pendingEntries: number; droppedEntries: number; lastFailure?: string; lastFailureAt?: string; }
export interface Logger { log(level: LogLevel, fields: LogFields): Promise<void>; flush(): Promise<void>; getStatus(): LoggerStatus; }
interface SinkConfig { maxFileBytes: number; maxMetadataBytes: number; maxRecordBytes: number; maxQueueEntries: number; maxRetainedFiles: number; retentionMs: number; }
interface Sink { config: SinkConfig; directory: string; filename: string; path: string; service: LogService; tail: Promise<void>; pendingEntries: number; droppedEntries: number; destinationAvailable: boolean; lastFailure?: string; lastFailureAt?: string; lastFallbackAt: number; prepared: boolean; rotationSequence: number; activeStartedAt?: number; }

const levelRank: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
const secretKey = /authorization|token|secret|password|api[-_]?key|credential|cookie|session/i;
const routineOutputKey = /prompt|transcript|source.?output|captured.?output|raw.?output/i;
const sinks = new Map<string, Sink>();
const fallbackStreams = new WeakSet<object>();
const maxDepth = 4;
const maxStartupReadBytes = 64 * 1024;
const utf8 = (value: string) => Buffer.byteLength(value, "utf8");

function boundedText(value: unknown, limit: number): string {
  let text: string;
  try { text = typeof value === "string" ? value : String(value); } catch { text = "[UNAVAILABLE]"; }
  return text.length <= limit ? text : `${text.slice(0, Math.max(0, limit - 12))}[TRUNCATED]`;
}

function redactText(value: string): string {
  return value
    .replace(/\bBearer\s+[^\s,;]+/gi, "Bearer [REDACTED]")
    .replace(/\bBasic\s+[^\s,;]+/gi, "Basic [REDACTED]")
    .replace(/\b(?:x[i-]?api[-_]?key|api[-_]?key|authorization)\s*:\s*[^\s,;]+/gi, (match) => `${match.slice(0, match.indexOf(":"))}: [REDACTED]`)
    .replace(/([?&](?:access[_-]?token|refresh[_-]?token|token|key|secret|password|api[_-]?key|xi[-_]?api[-_]?key|credential|cookie|session)=)[^&\s]+/gi, "$1[REDACTED]")
    .replace(/([a-z][a-z0-9+.-]*:\/\/[^\s/:]+:)[^@\s/]+@/gi, "$1[REDACTED]@")
    .replace(/(["'](?:authorization|access[_-]?token|refresh[_-]?token|token|secret|password|api[-_]?key|xi[-_]?api[-_]?key|credential|cookie|session)["']\s*:\s*["'])[^"']*(?=["']|$)/gi, "$1[REDACTED]")
    .replace(/\b(?:access[_-]?token|refresh[_-]?token|api[_-]?key|xi[-_]?api[-_]?key|token|secret|password|credential|cookie|session)\s*[:=]\s*[^\s,;]+/gi, (match) => match.replace(/[:=].*/, ": [REDACTED]"));
}

function safeDiagnostic(value: unknown): string { return redactText(boundedText(value instanceof Error ? value.message : value, 256)).replace(/[\r\n]+/g, " "); }
function installFallbackProtection(): void {
  const stream = process.stderr as unknown as object & { on?: (event: string, listener: () => void) => unknown };
  if (fallbackStreams.has(stream)) return;
  fallbackStreams.add(stream);
  try { stream.on?.("error", () => undefined); } catch { /* stderr is optional diagnostic infrastructure. */ }
}
function fallback(message: string): void {
  installFallbackProtection();
  try { process.stderr.write(`${redactText(boundedText(message, 512)).replace(/[\r\n]+/g, " ")}\n`, () => undefined); } catch { /* Logging must never make observation fail. */ }
}
function diagnostic(sink: Sink, error: unknown, unavailable: boolean): void {
  if (unavailable) sink.destinationAvailable = false;
  sink.lastFailure = safeDiagnostic(error);
  sink.lastFailureAt = new Date().toISOString();
  if (Date.now() - sink.lastFallbackAt >= 30_000) { sink.lastFallbackAt = Date.now(); fallback(`logger diagnostic: ${sink.lastFailure}`); }
}
function validateInteger(name: string, value: number, minimum: number): number {
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < minimum) throw new TypeError(`${name} must be a finite integer of at least ${minimum}`);
  return value;
}
function sinkConfig(options: LoggerOptions): SinkConfig {
  const maxRecordBytes = validateInteger("maxRecordBytes", options.maxRecordBytes ?? Math.min(8_192, options.maxFileBytes ?? 1_048_576), 128);
  const maxFileBytes = validateInteger("maxFileBytes", options.maxFileBytes ?? 1_048_576, maxRecordBytes);
  const retentionDays = options.retentionDays ?? 7;
  if (!Number.isFinite(retentionDays) || retentionDays < 0) throw new TypeError("retentionDays must be finite and non-negative");
  return { maxRecordBytes, maxFileBytes, maxMetadataBytes: validateInteger("maxMetadataBytes", options.maxMetadataBytes ?? 4_096, 16), maxQueueEntries: validateInteger("maxQueueEntries", options.maxQueueEntries ?? 256, 1), maxRetainedFiles: validateInteger("maxRetainedFiles", options.maxRetainedFiles ?? 7, 0), retentionMs: retentionDays * 86_400_000 };
}
function sameConfig(left: SinkConfig, right: SinkConfig): boolean { return Object.entries(left).every(([key, value]) => right[key as keyof SinkConfig] === value); }

function sanitize(value: unknown, budget: number, depth = 0): unknown {
  if (value === null || typeof value === "boolean" || typeof value === "number") return value;
  if (typeof value === "string") return redactText(boundedText(value, Math.min(512, budget)));
  if (typeof value !== "object") return redactText(boundedText(value, Math.min(128, budget)));
  if (depth >= maxDepth || budget < 8) return "[TRUNCATED]";
  if (Array.isArray(value)) {
    const result: unknown[] = [];
    for (const item of value.slice(0, Math.min(20, Math.max(1, Math.floor(budget / 8))))) {
      const next = sanitize(item, Math.max(8, budget - utf8(JSON.stringify(result))), depth + 1);
      if (utf8(JSON.stringify([...result, next])) > budget) break;
      result.push(next);
    }
    return result;
  }
  const result: Record<string, unknown> = {};
  const maximumProperties = Math.min(100, Math.max(1, Math.floor(budget / 16)));
  let seen = 0;
  try {
    for (const rawKey in value as Record<string, unknown>) {
      if (seen >= maximumProperties) break;
      seen += 1;
      const key = boundedText(rawKey, 80);
      let item: unknown;
      try { item = (value as Record<string, unknown>)[rawKey]; } catch { item = "[UNAVAILABLE]"; }
      const next = secretKey.test(rawKey) ? "[REDACTED]" : routineOutputKey.test(rawKey) ? "[OMITTED]" : sanitize(item, Math.max(8, budget - utf8(JSON.stringify(result))), depth + 1);
      if (utf8(JSON.stringify({ ...result, [key]: next })) > budget) break;
      result[key] = next;
    }
  } catch { return "[UNAVAILABLE]"; }
  return result;
}

function encodeRecord(service: LogService, level: LogLevel, fields: LogFields, config: SinkConfig): string | undefined {
  const metadata = sanitize(fields.metadata ?? {}, config.maxMetadataBytes);
  const record: Record<string, unknown> = { timestamp: new Date().toISOString(), level, service, operation: redactText(boundedText(fields.operation, 120)), message: redactText(boundedText(fields.message, 1_024)) };
  if (fields.sourceId) record.sourceId = redactText(boundedText(fields.sourceId, 128));
  if (fields.participantId) record.participantId = redactText(boundedText(fields.participantId, 128));
  if (fields.jobId) record.jobId = redactText(boundedText(fields.jobId, 128));
  if (fields.outcome) record.outcome = redactText(boundedText(fields.outcome, 80));
  if (typeof metadata === "object" && metadata !== null && Object.keys(metadata).length > 0) record.metadata = metadata;
  const optional = ["metadata", "outcome", "jobId", "participantId", "sourceId"];
  let line = `${JSON.stringify(record)}\n`;
  for (const key of optional) { if (utf8(line) <= config.maxRecordBytes) break; delete record[key]; line = `${JSON.stringify(record)}\n`; }
  for (const key of ["message", "operation"] as const) while (utf8(line) > config.maxRecordBytes && typeof record[key] === "string" && record[key].length > 0) { record[key] = record[key].slice(0, Math.floor(record[key].length / 2)); line = `${JSON.stringify(record)}\n`; }
  return utf8(line) <= config.maxRecordBytes ? line : undefined;
}

async function prune(sink: Sink): Promise<void> {
  const entries = await readdir(sink.directory, { withFileTypes: true });
  const rotations = (await Promise.all(entries.filter((entry) => entry.isFile() && entry.name !== sink.filename && entry.name.startsWith(`${sink.service}.`) && entry.name.endsWith(".jsonl")).map(async (entry) => ({ name: entry.name, stats: await stat(join(sink.directory, entry.name)) })))).sort((left, right) => right.stats.mtimeMs - left.stats.mtimeMs);
  const cutoff = Date.now() - sink.config.retentionMs;
  await Promise.all(rotations.filter((entry, index) => entry.stats.mtimeMs < cutoff || index >= sink.config.maxRetainedFiles).map((entry) => unlink(join(sink.directory, entry.name))));
}
async function readTail(path: string, size: number): Promise<{ text: string; startsAtBoundary: boolean }> {
  const bytes = Math.min(size, maxStartupReadBytes);
  const handle = await open(path, "r");
  try {
    const buffer = Buffer.alloc(bytes);
    await handle.read(buffer, 0, bytes, size - bytes);
    return { text: buffer.toString("utf8"), startsAtBoundary: size === bytes };
  } finally {
    await handle.close();
  }
}
function validLines(text: string, startsAtBoundary: boolean, sink: Sink): string[] {
  const lines = text.split("\n");
  if (!startsAtBoundary) lines.shift();
  if (!text.endsWith("\n")) lines.pop();
  const retained: string[] = [];
  let bytes = 0;
  for (const line of lines.reverse()) {
    if (!line || utf8(`${line}\n`) > sink.config.maxRecordBytes) continue;
    try { JSON.parse(line); } catch { continue; }
    if (bytes + utf8(`${line}\n`) > sink.config.maxFileBytes) break;
    retained.unshift(line);
    bytes += utf8(`${line}\n`);
  }
  return retained;
}
async function reconcileExistingFile(sink: Sink, path: string): Promise<number | undefined> {
  const existing = await stat(path);
  const { text, startsAtBoundary } = await readTail(path, existing.size);
  const lines = validLines(text, startsAtBoundary, sink);
  const repaired = `${lines.join("\n")}${lines.length ? "\n" : ""}`;
  const requiresRepair = existing.size > sink.config.maxFileBytes || !startsAtBoundary || utf8(repaired) !== existing.size;
  if (requiresRepair) {
    if (repaired) {
      await writeFile(path, repaired, "utf8");
      await utimes(path, existing.atime, existing.mtime);
    }
    else await unlink(path);
    diagnostic(sink, "reconciled existing log file to configured limits", false);
  }
  const first = lines[0];
  if (!first) return undefined;
  try {
    const timestamp = Date.parse(JSON.parse(first).timestamp);
    return Number.isFinite(timestamp) ? timestamp : existing.mtimeMs;
  } catch { return existing.mtimeMs; }
}
async function maintain(sink: Sink): Promise<void> {
  const cutoff = Date.now() - sink.config.retentionMs;
  if (sink.activeStartedAt !== undefined && sink.activeStartedAt < cutoff) {
    try { await unlink(sink.path); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    sink.activeStartedAt = undefined;
    diagnostic(sink, "expired active log file", false);
  }
  await prune(sink);
}
async function prepare(sink: Sink): Promise<void> {
  if (sink.prepared) return;
  await mkdir(sink.directory, { recursive: true });
  try {
    sink.activeStartedAt = await reconcileExistingFile(sink, sink.path);
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  const entries = await readdir(sink.directory, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isFile() && entry.name !== sink.filename && entry.name.startsWith(`${sink.service}.`) && entry.name.endsWith(".jsonl")) {
      await reconcileExistingFile(sink, join(sink.directory, entry.name));
    }
  }
  await maintain(sink);
  sink.prepared = true;
}
async function append(sink: Sink, line: string): Promise<void> {
  await prepare(sink);
  await maintain(sink);
  try {
    const current = await stat(sink.path);
    if (current.size + utf8(line) > sink.config.maxFileBytes) { const suffix = `${new Date().toISOString().replace(/[:.]/g, "-")}-${process.pid}-${sink.rotationSequence += 1}`; await rename(sink.path, join(sink.directory, `${sink.service}.${suffix}.jsonl`)); sink.activeStartedAt = undefined; await prune(sink); }
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  await writeFile(sink.path, line, { encoding: "utf8", flag: "a" });
  if (sink.activeStartedAt === undefined) {
    try { sink.activeStartedAt = Date.parse(JSON.parse(line).timestamp); } catch { sink.activeStartedAt = Date.now(); }
  }
  sink.destinationAvailable = true;
}
function getSink(options: LoggerOptions): Sink {
  const config = sinkConfig(options);
  const directory = resolve(options.dataDirectory ?? "data", "logs");
  const filename = `${options.service}.jsonl`;
  const path = join(directory, filename);
  const existing = sinks.get(path);
  if (existing) { if (!sameConfig(existing.config, config)) throw new TypeError(`incompatible logger configuration for ${path}`); return existing; }
  const sink: Sink = { config, directory, filename, path, service: options.service, tail: Promise.resolve(), pendingEntries: 0, droppedEntries: 0, destinationAvailable: true, lastFallbackAt: 0, prepared: false, rotationSequence: 0 };
  sinks.set(path, sink);
  return sink;
}

export function createLogger(options: LoggerOptions): Logger {
  const sink = getSink(options);
  const minimumLevel = options.minimumLevel ?? "info";
  if (!(minimumLevel in levelRank)) throw new TypeError("minimumLevel must be a supported level");
  function log(level: LogLevel, fields: LogFields): Promise<void> {
    if (!(level in levelRank) || levelRank[level] < levelRank[minimumLevel]) return Promise.resolve();
    if (sink.pendingEntries >= sink.config.maxQueueEntries) { sink.droppedEntries += 1; diagnostic(sink, "logger queue is full", false); return Promise.resolve(); }
    let line: string | undefined;
    try { line = encodeRecord(sink.service, level, fields, sink.config); } catch (error) { diagnostic(sink, error, false); return Promise.resolve(); }
    if (!line) { sink.droppedEntries += 1; diagnostic(sink, "logger record exceeds configured byte budget", false); return Promise.resolve(); }
    sink.pendingEntries += 1;
    const task = sink.tail.then(() => append(sink, line as string)).catch((error) => diagnostic(sink, error, true)).finally(() => { sink.pendingEntries -= 1; });
    sink.tail = task;
    return task;
  }
  return { log, flush: () => sink.tail, getStatus: () => ({ destinationAvailable: sink.destinationAvailable, pendingEntries: sink.pendingEntries, droppedEntries: sink.droppedEntries, lastFailure: sink.lastFailure, lastFailureAt: sink.lastFailureAt }) };
}
export function appLogger(options: Omit<LoggerOptions, "service"> = {}): Logger { return createLogger({ ...options, service: "app" }); }
export function collectorLogger(options: Omit<LoggerOptions, "service"> = {}): Logger { return createLogger({ ...options, service: "collector" }); }
