import { lstat, mkdir, open, readFile, readdir, rename, stat, unlink, writeFile, link } from "node:fs/promises";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import type { TraceContext } from "./shared";

export type SpanOutcome = "succeeded" | "failed" | "timed_out" | "cancelled" | "retried" | "rejected" | "missing" | "cached";
export interface TraceFields extends Partial<TraceContext> { sourceId?: string; participantId?: string; jobId?: string; announcementId?: string; clipId?: string; retry?: number; metadata?: Record<string, unknown>; }
export interface TracerOptions { dataDirectory?: string; maxFileBytes?: number; maxRetainedFiles?: number; retentionDays?: number; }
export interface TraceStatus { destinationAvailable: boolean; pendingEntries: number; droppedEntries: number; lastFailure?: string; lastFailureAt?: string; }
export interface Span { context: TraceContext; end(outcome: SpanOutcome, fields?: Omit<TraceFields, "traceId" | "spanId" | "parentSpanId">): Promise<void>; }
export interface Tracer { start(operation: string, fields?: TraceFields): Span; flush(): Promise<void>; getStatus(): TraceStatus; }

const sinks = new Map<string, Tracer>();
const size = (value: string) => Buffer.byteLength(value, "utf8");
const text = (value: unknown, limit: number) => { let result: string; try { result = typeof value === "string" ? value : String(value); } catch { result = "[UNAVAILABLE]"; } return result.length <= limit ? result : `${result.slice(0, limit - 12)}[TRUNCATED]`; };
const redact = (value: string) => value.replace(/\bBearer\s+[^\s,;]+/gi, "Bearer [REDACTED]").replace(/\bBasic\s+[^\s,;]+/gi, "Basic [REDACTED]").replace(/\b(?:x[i-]?api[-_]?key|api[-_]?key|authorization)\s*:\s*[^\s,;]+/gi, (match) => `${match.slice(0, match.indexOf(":"))}: [REDACTED]`).replace(/([?&](?:access[_-]?token|refresh[_-]?token|token|key|secret|password|api[_-]?key|credential|cookie|session)=)[^&\s]+/gi, "$1[REDACTED]").replace(/([a-z][a-z0-9+.-]*:\/\/[^\s/:]+:)[^@\s/]+@/gi, "$1[REDACTED]@").replace(/(["'](?:authorization|access[_-]?token|refresh[_-]?token|token|secret|password|api[-_]?key|xi[-_]?api[-_]?key|credential|cookie|session)["']\s*:\s*["'])[^"']*(?=["']|$)/gi, "$1[REDACTED]").replace(/\b(?:access[_-]?token|refresh[_-]?token|api[-_]?key|xi[-_]?api[-_]?key|token|secret|password|credential|cookie|session)\s*[:=]\s*[^\s,;]+/gi, (match) => match.replace(/[:=].*/, ": [REDACTED]"));
const safe = (value: unknown, limit = 128) => redact(text(value, limit)).replace(/[\r\n]+/g, " ");
const id = () => crypto.randomUUID().replace(/-/g, "");
const startupReadBytes = 64 * 1024;
const require = createRequire(import.meta.url);
const flock = require("../native/flock_guard.node") as { lock(fd: number): number };

export function createTracer(options: TracerOptions = {}): Tracer {
  const directory = resolve(options.dataDirectory ?? "data", "traces"); const path = join(directory, "calls.jsonl");
  const prior = sinks.get(path); if (prior) return prior;
  const maxFileBytes = options.maxFileBytes ?? 1_048_576; const retained = options.maxRetainedFiles ?? 7; const retentionMs = (options.retentionDays ?? 7) * 86_400_000;
  let tail = Promise.resolve(); let prepared = false; let fallbackAt = 0;
  const status: TraceStatus = { destinationAvailable: true, pendingEntries: 0, droppedEntries: 0 };
  const diagnostic = (error: unknown) => { status.destinationAvailable = false; status.lastFailure = safe(error instanceof Error ? error.message : error, 240); status.lastFailureAt = new Date().toISOString(); if (Date.now() - fallbackAt > 30_000) { fallbackAt = Date.now(); try { process.stderr.write(`trace diagnostic: ${status.lastFailure}\n`, () => undefined); } catch { /* Diagnostics are nonthrowing. */ } } };
  const prune = async () => { const cutoff = Date.now() - retentionMs; const entries = (await Promise.all((await readdir(directory, { withFileTypes: true })).filter((entry) => entry.isFile() && entry.name.startsWith("calls.") && entry.name.endsWith(".jsonl")).map(async (entry) => ({ entry, info: await stat(join(directory, entry.name)) })))) .sort((a, b) => b.info.mtimeMs - a.info.mtimeMs); await Promise.all(entries.filter((entry, index) => entry.info.mtimeMs < cutoff || index >= retained).map((entry) => unlink(join(directory, entry.entry.name)))); };
  const readTail = async (target: string) => { const handle = await open(target, "r"); try { const info = await handle.stat(); const length = Math.min(info.size, startupReadBytes); const buffer = Buffer.alloc(length); await handle.read(buffer, 0, length, info.size - length); return { buffer, complete: info.size === length }; } finally { await handle.close(); } };
  const reconcile = async (target: string) => { const { buffer, complete } = await readTail(target); const rows = buffer.toString("latin1").split("\n"); if (!complete) rows.shift(); const decoder = new TextDecoder("utf-8", { fatal: true }); const cutoff = Date.now() - retentionMs; const valid = rows.filter(Boolean).flatMap((raw) => { try { const line = decoder.decode(Buffer.from(raw, "latin1")); const record = JSON.parse(line) as { endedAt?: string }; const ended = Date.parse(record.endedAt ?? ""); return Number.isFinite(ended) && ended >= cutoff && size(`${line}\n`) <= Math.min(4096, maxFileBytes) ? [line] : []; } catch { return []; } }); const kept: string[] = []; let total = 0; for (const line of valid.reverse()) { if (total + size(`${line}\n`) > maxFileBytes) break; kept.unshift(line); total += size(`${line}\n`); } await writeFile(target, `${kept.join("\n")}${kept.length ? "\n" : ""}`); };
  const prepare = async () => { if (prepared) return; await mkdir(directory, { recursive: true }); await prune(); try { await reconcile(path); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; } const archives = await readdir(directory, { withFileTypes: true }); for (const entry of archives) if (entry.isFile() && entry.name.startsWith("calls.") && entry.name.endsWith(".jsonl")) await reconcile(join(directory, entry.name)); await prune(); prepared = true; };
  const append = async (line: string) => {
    await mkdir(directory, { recursive: true });
    const canonical = join(directory, ".calls.lock");
    const guard = await open(join(directory, ".calls.guard"), "a+");
    let published = false;
    let candidate = "";
    let token = "";
    try {
      if (!(await guard.stat()).isFile()) throw new Error("trace guard is not a regular file");
      const deadline = performance.now() + 2_000;
      for (;;) {
        const result = flock.lock(guard.fd);
        if (result === 0) break;
        if (result !== 1) throw new Error(`trace guard lock failed: ${result}`);
        if (performance.now() >= deadline) throw new Error("trace guard is busy");
        await Bun.sleep(2);
      }
      const owner = async () => {
        try {
          if (!(await lstat(canonical)).isFile()) return undefined;
          const value = JSON.parse(await readFile(canonical, "utf8")) as { version?: unknown; pid?: unknown; token?: unknown };
          return value.version === 1 && Number.isInteger(value.pid) && (value.pid as number) > 0 && typeof value.token === "string" && value.token.length >= 16 ? { pid: value.pid as number, token: value.token } : undefined;
        } catch { return undefined; }
      };
      const existing = await owner();
      if (existing) {
        let live = true;
        try { process.kill(existing.pid, 0); } catch (error) { live = (error as NodeJS.ErrnoException).code !== "ESRCH"; }
        if (live) throw new Error("trace owner is live");
        await unlink(canonical);
      } else {
        try { await stat(canonical); throw new Error("trace owner is ambiguous"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      }
      token = id();
      candidate = join(directory, `.calls.owner.${process.pid}.${token}`);
      await writeFile(candidate, JSON.stringify({ version: 1, pid: process.pid, token }), { encoding: "utf8", flag: "wx" });
      await link(candidate, canonical);
      published = true;
      await unlink(candidate);
      candidate = "";
      await prepare();
      await prune();
      const archives = await readdir(directory, { withFileTypes: true });
      for (const entry of archives) if (entry.isFile() && entry.name !== "calls.jsonl" && entry.name.startsWith("calls.") && entry.name.endsWith(".jsonl")) await reconcile(join(directory, entry.name));
      try { await reconcile(path); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      await prune();
      try { if ((await stat(path)).size + size(line) > maxFileBytes) await rename(path, join(directory, `calls.${new Date().toISOString().replace(/[:.]/g, "-")}-${process.pid}-${id()}.jsonl`)); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      await writeFile(path, line, { encoding: "utf8", flag: "a" });
      status.destinationAvailable = true;
    } finally {
      if (candidate) await unlink(candidate).catch(() => undefined);
      if (published) {
        try {
          const current = await readFile(canonical, "utf8");
          if (JSON.parse(current).token === token) await unlink(canonical);
        } catch { /* Keep a changed or unreadable canonical owner conservative. */ }
      }
      await guard.close();
    }
  };
  const enqueue = (record: Record<string, unknown>) => { const line = `${JSON.stringify(record)}\n`; if (size(line) > Math.min(4096, maxFileBytes) || status.pendingEntries >= 256) { status.droppedEntries += 1; diagnostic("trace record exceeds bounded capacity"); return Promise.resolve(); } status.pendingEntries += 1; const task = tail.then(() => append(line)).catch((error) => { status.droppedEntries += 1; diagnostic(error); }).finally(() => { status.pendingEntries -= 1; }); tail = task; return task; };
  const tracer: Tracer = { start(operation, fields = {}) { const startedAt = new Date().toISOString(); const monotonic = performance.now(); const context: TraceContext = { traceId: fields.traceId ?? id(), spanId: id().slice(0, 16), ...(fields.spanId ? { parentSpanId: fields.spanId } : fields.parentSpanId ? { parentSpanId: fields.parentSpanId } : {}), ...Object.fromEntries(["sourceId", "participantId", "jobId", "announcementId", "clipId"].flatMap((key) => fields[key as keyof TraceFields] ? [[key, fields[key as keyof TraceFields]]] : [])) }; let ended = false; return { context, async end(outcome, extra = {}) { if (ended) return; ended = true; const record: Record<string, unknown> = { traceId: context.traceId, spanId: context.spanId, ...(context.parentSpanId ? { parentSpanId: context.parentSpanId } : {}), operation: safe(operation, 120), startedAt, endedAt: new Date().toISOString(), durationMs: Math.max(0, Math.round((performance.now() - monotonic) * 1000) / 1000), outcome, retry: extra.retry ?? fields.retry ?? 0 }; for (const key of ["sourceId", "participantId", "jobId", "announcementId", "clipId"] as const) { const value = extra[key] ?? fields[key] ?? context[key]; if (value) record[key] = safe(value); } const metadata = extra.metadata ?? fields.metadata; if (metadata) { const result: Record<string, string> = {}; for (const [key, value] of Object.entries(metadata).slice(0, 12)) if (!/authorization|token|secret|password|api[-_]?key|credential|cookie|session|prompt|evidence|transcript|stdout|stderr|header|body/i.test(key)) result[safe(key, 48)] = safe(value, 128); if (Object.keys(result).length) record.metadata = result; } await enqueue(record); } }; }, flush: () => tail, getStatus: () => ({ ...status }) };
  sinks.set(path, tracer); return tracer;
}
export const collectorTracer = (options: TracerOptions = {}) => createTracer(options);
export const appTracer = (options: TracerOptions = {}) => createTracer(options);
