import { createHash } from "node:crypto";
import { mkdir, open, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { captureStructured, captureTerminal, participantIdentity, reconcileTopology, topologyFromHerdr } from "./capture";
import type { Activity, Cursor, Participant, Topology } from "./shared";
import { summarize } from "./summarizer";
import { synthesize } from "./speech";
import { collectorLogger } from "./logging";
import { collectorTracer } from "./tracing";

export interface CollectorConfig {
  sourceNamespace: string;
  dataDir: string;
  excludePaneIds: string[];
  pollMs?: number;
}

interface PendingJob { activity: Activity; attempts: number; done: boolean; error?: string; }
interface FileCursor { path: string; size: number; headerHash: string; sessionFingerprint: string; device: number; inode: number; createdAtMs: number; }
interface SourceStatus { participantId: string; kind: "error" | "overflow" | "unavailable" | "limited"; message: string; observedAt: string; }
export interface CollectorState {
  topology?: Topology;
  cursors: Record<string, Cursor>;
  files: Record<string, FileCursor>;
  activityIds: Set<string>;
  jobs: PendingJob[];
  sourceStatuses: SourceStatus[];
}

const MAX_PENDING_JOBS = 100;
const MAX_RETAINED_EVIDENCE_BYTES = 512_000;
const MAX_TRANSCRIPT_READ_BYTES = 256_000;
const MAX_SUPPORTED_FRAME_BYTES = 2_000_000;
const MAX_OVERSIZED_SCAN_BYTES_PER_POLL = 256_000;
const MAX_DROPPED_FRAME_BYTES = 8_000_000;
const PROCESS_STOP_GRACE_MS = 250;

const stateFile = (dataDir: string) => join(dataDir, "collector-state.json");
const digest = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");

export const initialState = (): CollectorState => ({ cursors: {}, files: {}, activityIds: new Set(), jobs: [], sourceStatuses: [] });

export async function restore(dataDir: string): Promise<CollectorState> {
  try {
    const saved = JSON.parse(await readFile(stateFile(dataDir), "utf8")) as Omit<CollectorState, "activityIds"> & { activityIds: string[] };
    return { ...initialState(), ...saved, activityIds: new Set(saved.activityIds) };
  } catch { return initialState(); }
}

async function persist(dataDir: string, state: CollectorState): Promise<void> {
  await mkdir(dataDir, { recursive: true });
  const temporary = `${stateFile(dataDir)}.${crypto.randomUUID()}.tmp`;
  await writeFile(temporary, JSON.stringify({ ...state, activityIds: [...state.activityIds] }));
  await rename(temporary, stateFile(dataDir));
}

function cloneState(state: CollectorState): CollectorState {
  return {
    ...state,
    cursors: structuredClone(state.cursors),
    files: structuredClone(state.files),
    activityIds: new Set(state.activityIds),
    jobs: structuredClone(state.jobs),
    sourceStatuses: structuredClone(state.sourceStatuses),
  };
}

function addStatus(state: CollectorState, participantId: string, kind: SourceStatus["kind"], message: string): void {
  state.sourceStatuses = [...state.sourceStatuses.filter((status) => status.participantId !== participantId), { participantId, kind, message, observedAt: new Date().toISOString() }].slice(-100);
}

function canEnqueue(state: CollectorState, activities: Activity[]): boolean {
  const newActivities = activities.filter((activity) => !state.activityIds.has(activity.id));
  const pending = state.jobs.filter((job) => !job.done);
  const replacementIds = new Set(newActivities.flatMap((activity) => activity.revisionOf
    ? pending.filter((job) => job.activity.revisionOf === activity.revisionOf).map((job) => job.activity.id)
    : []));
  const retained = pending.filter((job) => !replacementIds.has(job.activity.id));
  const bytes = retained.reduce((total, job) => total + Buffer.byteLength(job.activity.text), 0)
    + newActivities.reduce((total, activity) => total + Buffer.byteLength(activity.text), 0);
  return retained.length + newActivities.length <= MAX_PENDING_JOBS && bytes <= MAX_RETAINED_EVIDENCE_BYTES;
}

const throwIfAborted = (signal?: AbortSignal) => {
  if (signal?.aborted) throw new Error("collector operation was cancelled");
};

async function drainCommand(stream: ReadableStream<Uint8Array>, limit: number, signal: AbortSignal, stop: () => void): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const cancel = () => { void reader.cancel(); };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    for (;;) {
      if (signal.aborted) throw new Error("collector operation was cancelled");
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > limit) {
        stop();
        throw new Error("observation output exceeded its limit");
      }
      chunks.push(next.value);
    }
  } finally {
    signal.removeEventListener("abort", cancel);
    reader.releaseLock();
  }
  return new TextDecoder().decode(Buffer.concat(chunks));
}

async function stopOwnedChild(child: ReturnType<typeof Bun.spawn>): Promise<void> {
  if (child.exitCode !== null) {
    await child.exited;
    return;
  }

  child.kill("SIGTERM");
  await Promise.race([
    child.exited,
    Bun.sleep(PROCESS_STOP_GRACE_MS),
  ]);

  if (child.exitCode === null) child.kill("SIGKILL");
  await child.exited;
}

async function runCommand(argv: string[], signal?: AbortSignal, timeoutMs = 5_000, trace?: ReturnType<typeof collectorTracer>["start"] extends (...args: never[]) => infer Span ? Span : never): Promise<string> {
  if (signal?.aborted) { await trace?.end("cancelled"); throwIfAborted(signal); }
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error("Observation timeout must be a positive finite value");
  const spawnChild = () => Bun.spawn(argv, { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  let child: ReturnType<typeof spawnChild>;
  try { child = spawnChild(); }
  catch (error) { await trace?.end("failed"); throw error; }
  const controller = new AbortController();
  let outputExceeded = false;
  const cancel = () => {
    controller.abort();
    void stopOwnedChild(child);
  };
  signal?.addEventListener("abort", cancel, { once: true });
  let timedOut = false;
  const timeout = setTimeout(() => { timedOut = true; cancel(); }, timeoutMs);
  const drains = [
    drainCommand(child.stdout, 2_000_000, controller.signal, () => { outputExceeded = true; cancel(); }),
    drainCommand(child.stderr, 200_000, controller.signal, () => { outputExceeded = true; cancel(); }),
  ];
  try {
    const [stdout] = await Promise.all(drains);
    await child.exited;
    throwIfAborted(signal);
    if (controller.signal.aborted || child.exitCode !== 0) throw new Error(`${argv[0]} observation failed`);
    await trace?.end("succeeded", { metadata: { exitCode: child.exitCode } });
    return stdout;
  } catch (error) {
    await trace?.end(timedOut ? "timed_out" : outputExceeded ? "failed" : controller.signal.aborted || signal?.aborted ? "cancelled" : "failed");
    throw error;
  } finally {
    clearTimeout(timeout);
    signal?.removeEventListener("abort", cancel);
    controller.abort();
    await stopOwnedChild(child);
    await Promise.allSettled(drains);
  }
}

function exclusionPaneIds(namespace: string, exclusions: string[]): Set<string> {
  return new Set(exclusions.map((value) => {
    if (value.startsWith("raw:")) return `${namespace}:${value.slice("raw:".length)}`;
    if (value.startsWith("qualified:")) return value.slice("qualified:".length);
    return `${namespace}:${value}`;
  }));
}

export async function readTopology(config: CollectorConfig, signal?: AbortSignal): Promise<Topology> {
  const tracer = collectorTracer({ dataDirectory: config.dataDir });
  const span = tracer.start("herdr.snapshot", { sourceId: config.sourceNamespace });
  try {
    const command = tracer.start("herdr.snapshot.command", span.context);
    const response = JSON.parse(await runCommand(["herdr", "api", "snapshot"], signal, 5_000, command)) as { result?: { snapshot?: unknown } };
    const topology = topologyFromHerdr(config.sourceNamespace, response.result?.snapshot as Parameters<typeof topologyFromHerdr>[1]);
    const excluded = exclusionPaneIds(config.sourceNamespace, config.excludePaneIds);
    await span.end("succeeded");
    return { ...topology, participants: topology.participants.filter((participant) => !excluded.has(participant.paneId)) };
  } catch (error) { await span.end(signal?.aborted ? "cancelled" : "failed"); throw error; }
}

async function transcriptPaths(kind: "claude" | "codex", sessionId: string, signal?: AbortSignal, dataDir?: string, fields: Parameters<ReturnType<typeof collectorTracer>["start"]>[1] = {}): Promise<string[]> {
  if (!process.env.HOME) throw new Error("HOME is required for installed CLI transcripts");
  const root = join(process.env.HOME, kind === "codex" ? ".codex/sessions" : ".claude/projects");
  const span = dataDir ? collectorTracer({ dataDirectory: dataDir }).start("transcript.lookup", fields) : undefined;
  return (await runCommand(["rg", "-F", "-l", "--glob", "*.jsonl", sessionId, root], signal, 5_000, span)).trim().split("\n").filter(Boolean);
}

async function readRange(path: string, offset: number, limit: number, signal?: AbortSignal): Promise<Uint8Array> {
  throwIfAborted(signal);
  const file = await open(path, "r");
  try {
    const size = Math.max(0, Math.min(limit, (await file.stat()).size - offset));
    const buffer = Buffer.alloc(size);
    const { bytesRead } = await file.read(buffer, 0, size, offset);
    throwIfAborted(signal);
    return buffer.subarray(0, bytesRead);
  } finally {
    await file.close();
  }
}

async function readHandleRange(file: Awaited<ReturnType<typeof open>>, offset: number, limit: number, signal?: AbortSignal): Promise<Uint8Array> {
  throwIfAborted(signal);
  const buffer = Buffer.alloc(limit);
  const { bytesRead } = await file.read(buffer, 0, limit, offset);
  throwIfAborted(signal);
  return buffer.subarray(0, bytesRead);
}

async function skipOversizedFrame(file: Awaited<ReturnType<typeof open>>, offset: number, scanned: number, signal?: AbortSignal): Promise<{ nextOffset?: number; scanned: number; exhausted: boolean }> {
  let position = offset + scanned;
  let total = scanned;
  const limit = Math.min(scanned + MAX_OVERSIZED_SCAN_BYTES_PER_POLL, MAX_DROPPED_FRAME_BYTES);
  while (total < limit) {
    const chunk = await readHandleRange(file, position, Math.min(64_000, limit - total), signal);
    if (!chunk.byteLength) return { scanned: total, exhausted: total >= MAX_DROPPED_FRAME_BYTES };
    const newline = chunk.indexOf(10);
    if (newline >= 0) return { nextOffset: position + newline + 1, scanned: total + newline + 1, exhausted: false };
    position += chunk.byteLength;
    total += chunk.byteLength;
  }
  return { scanned: total, exhausted: total >= MAX_DROPPED_FRAME_BYTES };
}

export async function captureParticipant(participant: Participant, state: CollectorState, signal?: AbortSignal, dataDir?: string, trace?: import("./shared").TraceContext): Promise<Activity[]> {
  if ((participant.kind !== "claude" && participant.kind !== "codex") || !participant.sessionId) return [];
  const span = dataDir ? collectorTracer({ dataDirectory: dataDir }).start("transcript.read", { ...trace, participantId: participant.id }) : undefined;
  try { for (const path of await transcriptPaths(participant.kind, participant.sessionId, signal, dataDir, { ...span?.context, participantId: participant.id })) {
    throwIfAborted(signal);
    const file = await open(path, "r");
    try {
      const stats = await file.stat();
      const size = stats.size;
      const header = await readHandleRange(file, 0, 1_024, signal);
      const headerEnd = header.indexOf(10);
      const headerBytes = header.slice(0, headerEnd >= 0 ? headerEnd + 1 : header.byteLength);
      const headerHash = digest(headerBytes);
      const previousFile = state.files[participant.id];
      const candidate = captureStructured(participant.kind, participant.sessionId, {
        text: new TextDecoder().decode(await readHandleRange(file, 0, MAX_TRANSCRIPT_READ_BYTES, signal)),
        offset: 0,
        cursor: { participantId: participant.id, sourceCursor: "0", offset: 0, initialized: false, exactSessionVerified: false },
      }, participant);

      if (!candidate.cursor.exactSessionVerified || !candidate.verificationFingerprint) {
        if (previousFile?.path !== path) continue;
        state.cursors[participant.id] = {
          ...(state.cursors[participant.id] ?? candidate.cursor),
          exactSessionVerified: false,
        };
        const text = "Source transcript no longer matches the exact session.";
        return [{ id: digest(`${participant.id}:rejected:${path}:${stats.dev}:${stats.ino}:${size}`), participantId: participant.id, sourceCursor: state.cursors[participant.id]!.sourceCursor, observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }];
      }

      const sessionFingerprint = digest(JSON.stringify([
        participant.kind,
        participant.sessionReferenceKind,
        participant.sessionReferenceSource,
        participant.sessionId,
        candidate.verificationFingerprint,
      ]));
      const fileCursor: FileCursor = {
        path,
        size,
        headerHash,
        sessionFingerprint,
        device: stats.dev,
        inode: stats.ino,
        createdAtMs: stats.birthtimeMs || stats.ctimeMs,
      };
      const cursor = state.cursors[participant.id] ?? { participantId: participant.id, sourceCursor: "0", offset: 0, initialized: false };

      if (!previousFile) {
        state.files[participant.id] = fileCursor;
        state.cursors[participant.id] = { ...cursor, offset: size, sourceCursor: String(size), initialized: true, exactSessionVerified: true };
        return [];
      }

      const replaced = previousFile.path !== path
        || previousFile.device !== fileCursor.device
        || previousFile.inode !== fileCursor.inode
        || previousFile.createdAtMs !== fileCursor.createdAtMs
        || previousFile.sessionFingerprint !== fileCursor.sessionFingerprint
        || size < previousFile.size;
      if (replaced) {
        state.files[participant.id] = fileCursor;
        state.cursors[participant.id] = { ...cursor, offset: size, sourceCursor: String(size), initialized: true, exactSessionVerified: true, discardOffset: undefined, discardScanned: undefined };
        const text = "Source transcript was replaced or truncated.";
        return [{ id: digest(`${participant.id}:gap:${path}:${stats.dev}:${stats.ino}:${size}`), participantId: participant.id, sourceCursor: "0", observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }];
      }

      const readOffset = cursor.discardOffset ?? cursor.offset;
      const bytes = await readHandleRange(file, readOffset, MAX_TRANSCRIPT_READ_BYTES, signal);
      if (bytes.byteLength === MAX_TRANSCRIPT_READ_BYTES && readOffset + bytes.byteLength < size && bytes.lastIndexOf(10) < 0) {
        const scanned = cursor.discardOffset === undefined ? bytes.byteLength : cursor.discardScanned ?? bytes.byteLength;
        const discard = await skipOversizedFrame(file, cursor.offset, scanned, signal);
        if (discard.nextOffset && discard.nextOffset - cursor.offset <= MAX_SUPPORTED_FRAME_BYTES) {
          const complete = await readHandleRange(file, cursor.offset, discard.nextOffset - cursor.offset, signal);
          const result = captureStructured(participant.kind, participant.sessionId, { text: new TextDecoder().decode(complete), offset: cursor.offset, cursor }, participant);
          state.files[participant.id] = fileCursor;
          state.cursors[participant.id] = { ...result.cursor, discardOffset: undefined, discardScanned: undefined };
          if (!cursor.initialized || !result.cursor.exactSessionVerified) return [];
        return result.activities.filter((activity) => !state.activityIds.has(activity.id));
        }

        state.files[participant.id] = fileCursor;
        if (discard.nextOffset || discard.exhausted) {
          const nextOffset = discard.nextOffset ?? size;
          state.cursors[participant.id] = {
            ...cursor,
            offset: nextOffset,
            sourceCursor: String(nextOffset),
            initialized: true,
            exactSessionVerified: false,
            discardOffset: undefined,
            discardScanned: undefined,
          };
          const text = "An oversized transcript record was skipped and session verification was revoked.";
          return [{ id: digest(`${participant.id}:oversized:${cursor.offset}`), participantId: participant.id, sourceCursor: String(cursor.offset), observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }];
        }

        state.cursors[participant.id] = {
          ...cursor,
          discardOffset: cursor.offset,
          discardScanned: discard.scanned,
        };
        if (!cursor.discardScanned) {
          const text = "An oversized transcript record is being skipped in bounded scans.";
          return [{ id: digest(`${participant.id}:oversized-scanning:${cursor.offset}`), participantId: participant.id, sourceCursor: String(cursor.offset), observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }];
        }
        return [];
      }

      const result = captureStructured(participant.kind, participant.sessionId, { text: new TextDecoder().decode(bytes), offset: cursor.offset, cursor }, participant);
      state.files[participant.id] = fileCursor;
      state.cursors[participant.id] = result.cursor;
      if (!result.cursor.exactSessionVerified) return [];
      if (!cursor.initialized) return [];
      return result.activities.filter((activity) => !state.activityIds.has(activity.id));
    } finally {
      await file.close();
    }
  }
  return [];
  } catch (error) { await span?.end(signal?.aborted ? "cancelled" : "failed"); throw error; } finally { await span?.end("succeeded"); }
}

async function captureTerminalFallback(participant: Participant, state: CollectorState, signal?: AbortSignal, dataDir?: string, trace?: import("./shared").TraceContext): Promise<Activity[]> {
  const cursor = state.cursors[participant.id] ?? { participantId: participant.id, sourceCursor: "", offset: 0, initialized: false };
  const span = dataDir ? collectorTracer({ dataDirectory: dataDir }).start("herdr.terminal.read", { ...trace, participantId: participant.id }) : undefined;
  const snapshot = await runCommand(["herdr", "pane", "read", participant.rawPaneId, "--source", "recent-unwrapped", "--lines", "120"], signal, 5_000, span);
  const result = captureTerminal(snapshot, cursor, participant);
  state.cursors[participant.id] = result.cursor;
  return result.activities;
}

async function processPending(config: CollectorConfig, state: CollectorState, signal?: AbortSignal): Promise<string | undefined> {
  const logger = collectorLogger({ dataDirectory: config.dataDir });
  const tracer = collectorTracer({ dataDirectory: config.dataDir });
  let clip: string | undefined;
  for (const job of state.jobs.filter((item) => !item.done && item.attempts < 2)) {
    if (signal?.aborted) break;
    const active = state.topology?.participants.some((participant) => participant.id === job.activity.participantId && participant.active);
    const replaced = job.activity.revisionOf && state.jobs.some((other) => !other.done && other !== job && other.activity.revisionOf === job.activity.revisionOf);
    if (active === false || replaced) {
      job.done = true;
      job.error = active === false ? "source is no longer active" : "superseded by a newer revision";
      continue;
    }
    job.attempts += 1;
    try {
      const evidenceEventIds = [job.activity.id];
        const summary = await summarize({
          evidenceEventIds,
          text: formatEvidence(job.activity),
          retry: job.attempts - 1,
          }, undefined, undefined, signal, logger, tracer, job.activity.trace);
      if (signal?.aborted) break;
      if (!summary.speak) { job.done = true; job.error = undefined; continue; }
      const key = digest(`${summary.text}\0${Bun.env.ELEVENLABS_VOICE_ID}\0eleven_flash_v2_5`);
        const generated = await synthesize(summary.text, key, { apiKey: Bun.env.ELEVENLABS_API_KEY, voiceId: Bun.env.ELEVENLABS_VOICE_ID, dataDir: config.dataDir, logger, tracer, trace: job.activity.trace, operationId: job.activity.id, jobId: job.activity.id }, fetch, signal);
      if (signal?.aborted) break;
      const temporary = join(config.dataDir, `latest-announcement.${crypto.randomUUID()}.tmp`);
      const publishedKey = generated.path.split("/").at(-1)?.replace(/\.mp3$/, "");
        await writeFile(temporary, JSON.stringify({ key: publishedKey, text: summary.text, kind: summary.kind, createdAt: new Date().toISOString(), trace: job.activity.trace, announcementId: job.activity.id }));
      await rename(temporary, join(config.dataDir, "latest-announcement.json"));
      job.done = true;
      job.error = undefined;
      clip = generated.path;
      await logger.log("info", { operation: "collector.job", message: "Completed announcement job", outcome: "succeeded", participantId: job.activity.participantId, jobId: job.activity.id, metadata: { operationId: job.activity.id, clip: publishedKey } });
    } catch (error) {
      job.error = error instanceof Error ? error.message : "summary or speech failure";
      if (job.attempts >= 2) job.done = true;
      await logger.log("error", { operation: "collector.job", message: "Announcement job failed", outcome: "failed", participantId: job.activity.participantId, jobId: job.activity.id, metadata: { operationId: job.activity.id, error: job.error } });
    }
    await persist(config.dataDir, state);
  }
  return clip;
}

function formatEvidence(activity: Activity): string {
  const revision = activity.revisionOf ? `; revision-of=${activity.revisionOf}; revision=${activity.revisionId}` : "";
  return `[Evidence id=${activity.id}; capture-status=${activity.status}; excerpt=${activity.excerpt}; original-text-bytes=${activity.originalTextBytes}${revision}]\n${activity.text}`;
}

function enqueueActivity(state: CollectorState, activity: Activity, accepted: Activity[]): void {
  if (state.activityIds.has(activity.id)) return;
  state.activityIds.add(activity.id);
  const replaced = activity.revisionOf
    ? state.jobs.findIndex((job) => !job.done && job.activity.revisionOf === activity.revisionOf)
    : -1;
  const job = { activity, attempts: 0, done: false };
  if (replaced >= 0) state.jobs[replaced] = job;
  else state.jobs.push(job);
  accepted.push(activity);
}

export async function runOnce(config: CollectorConfig, state: CollectorState, signal?: AbortSignal): Promise<{ activities: Activity[]; clip?: string }> {
  const logger = collectorLogger({ dataDirectory: config.dataDir });
  const tracer = collectorTracer({ dataDirectory: config.dataDir });
  throwIfAborted(signal);
  let observed: Topology;
  try {
    observed = await readTopology(config, signal);
  } catch (error) {
    if (!state.topology) throw error;
    state.topology.source.stale = true;
    addStatus(state, "source", "error", error instanceof Error ? error.message.slice(0, 240) : "Topology observation failed");
    await persist(config.dataDir, state);
    return { activities: [] };
  }
  const next = cloneState(state);
  next.topology = next.topology ? reconcileTopology(next.topology, observed) : observed;
  next.topology.source.stale = false;
  const participants = [...new Map(next.topology.participants.map((item) => [participantIdentity(item), item])).values()];
  const accepted: Activity[] = [];
  for (const participant of participants) {
    throwIfAborted(signal);
    const staged = cloneState(next);
    const captureSpan = tracer.start(participant.sessionId ? "capture.transcript" : "capture.terminal", { sourceId: config.sourceNamespace, participantId: participant.id });
    try {
      const activities = participant.sessionId
        ? await captureParticipant(participant, staged, signal, config.dataDir, captureSpan.context)
        : await captureTerminalFallback(participant, staged, signal, config.dataDir, captureSpan.context);
      for (const activity of activities) activity.trace = captureSpan.context;
      for (const activity of activities) await logger.log(activity.status === "gap" ? "warn" : "info", { operation: activity.captureMode === "structured" ? "capture.transcript" : "capture.terminal", message: "Captured observation activity", outcome: activity.status, participantId: participant.id, sourceId: config.sourceNamespace, metadata: { operationId: activity.id, sourceCursor: activity.sourceCursor, originalTextBytes: activity.originalTextBytes } });
      const admitted: Activity[] = [];
      for (const activity of activities) {
        if (!canEnqueue(next, [...admitted, activity])) break;
        admitted.push(activity);
      }
      if (admitted.length < activities.length) {
        const withheld = activities[admitted.length];
        if (withheld) {
          const offset = Number(withheld.sourceCursor);
          if (Number.isFinite(offset)) staged.cursors[participant.id] = { ...staged.cursors[participant.id]!, offset, sourceCursor: String(offset) };
        }
        addStatus(next, participant.id, "overflow", "Local retry spool is full. Capture will retry after capacity is reclaimed.");
      }
      if (!admitted.length && activities.length) { await captureSpan.end("retried"); continue; }
      next.cursors[participant.id] = staged.cursors[participant.id]!;
      if (staged.files[participant.id]) next.files[participant.id] = staged.files[participant.id];
      for (const activity of admitted) enqueueActivity(next, activity, accepted);
      if (participant.sessionId) {
        if (admitted.length === activities.length) {
          next.sourceStatuses = next.sourceStatuses.filter((status) => status.participantId !== participant.id);
        }
      } else {
        addStatus(next, participant.id, "limited", "No exact session reference. Read-only terminal fallback is limited.");
      }
      await captureSpan.end("succeeded");
    } catch (error) {
      await captureSpan.end(signal?.aborted ? "cancelled" : "failed");
      addStatus(next, participant.id, participant.sessionId ? "unavailable" : "limited", error instanceof Error ? error.message.slice(0, 240) : "Source observation failed");
      await logger.log("error", { operation: "capture.observe", message: "Capture observation failed", outcome: "failed", participantId: participant.id, sourceId: config.sourceNamespace, metadata: { error: error instanceof Error ? error.message : "unknown" } });
    }
  }
  throwIfAborted(signal);
  await persist(config.dataDir, next);
  Object.assign(state, next);
  const clip = await processPending(config, state, signal);
  if (signal?.aborted) return { activities: accepted, clip };
  state.jobs = state.jobs.map((job) => job.done ? { ...job, activity: { ...job.activity, text: "" } } : job).slice(-MAX_PENDING_JOBS);
  if (state.activityIds.size > MAX_PENDING_JOBS * 4) state.activityIds = new Set([...state.activityIds].slice(-MAX_PENDING_JOBS * 4));
  await persist(config.dataDir, state);
  await logger.flush();
  return { activities: accepted, clip };
}

export async function watch(config: CollectorConfig, signal?: AbortSignal): Promise<void> {
  const state = await restore(config.dataDir);
  while (!signal?.aborted) {
    await runOnce(config, state, signal).catch(() => undefined);
    if (signal?.aborted) break;
    await new Promise<void>((resolve) => {
      const timer = setTimeout(done, config.pollMs ?? 1_000);
      const onAbort = () => done();
      function done(): void {
        clearTimeout(timer);
        signal?.removeEventListener("abort", onAbort);
        resolve();
      }
      signal?.addEventListener("abort", onAbort, { once: true });
    });
  }
}

if (import.meta.main) {
  const controller = new AbortController();
  process.once("SIGINT", () => controller.abort());
  process.once("SIGTERM", () => controller.abort());
  const config = { sourceNamespace: Bun.env.SOURCE_NAMESPACE ?? "local", dataDir: Bun.env.SPEAK_NOW_DATA_DIR ?? "data", pollMs: Number(Bun.env.POLL_MS ?? 1_000), excludePaneIds: (Bun.env.OBSERVATION_EXCLUDE_PANES ?? process.env.HERDR_PANE_ID ?? "").split(",").filter(Boolean) };
  void watch(config, controller.signal).finally(() => collectorLogger({ dataDirectory: config.dataDir }).flush());
}
