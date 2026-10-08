import { createHash } from "node:crypto";
import { mkdir, open, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Result from "effect/Result";
import { captureStructured, captureTerminal, participantIdentity, reconcileTopology, topologyFromHerdr } from "./capture";
import { COLLECTOR_WIRE_LIMITS, collectorUtf8Bytes, encodeCollectorJson, formatEvidence, type Activity, type Cursor, type Participant, type Topology } from "./shared";
import { summarize } from "./summarizer";
import { synthesize } from "./speech";
import { collectorLogger } from "./logging";
import { collectorTracer } from "./tracing";
import { createErrorReporter } from "./errors";

export interface CollectorConfig {
  sourceNamespace: string;
  dataDir: string;
  excludePaneIds: string[];
  pollMs?: number;
  collectorUrl?: string;
  collectorToken?: string;
  sourceEpoch?: string;
  workerId?: string;
}

interface PendingJob { activity: Activity; attempts: number; done: boolean; error?: string; }
interface FileCursor { path: string; size: number; headerHash: string; sessionFingerprint: string; device: number; inode: number; createdAtMs: number; baselineAt?: number; baselineDiscardOffset?: number; baselineDiscardScanned?: number; }
interface ParticipantCapture { activities: Activity[]; silentBaselineVerified: boolean; }
interface SourceStatus { participantId: string; kind: "error" | "overflow" | "unavailable" | "limited"; message: string; observedAt: string; }
interface RemoteBatch { batchId: string; body: Record<string, unknown>; encoded: string; topologyDigest: string; }
interface RemoteState { pendingBatches: RemoteBatch[]; acknowledgedCursors: Record<string, string>; baselineGenerations: Record<string, number>; baselineReadyParticipantIds: string[]; absentParticipantIds: string[]; listeningScope?: RemoteConfig["listeningScope"]; topologySequence: number; sourceEpoch?: string; bootstrapped: boolean; acknowledgedTopologyDigest?: string; lastTopologyPublishedAt?: string; }
interface RemoteConfig { sourceId: string; listeningScope: { workspaceId?: string; tabId?: string } | null; listeningGeneration: number; }
interface RemoteJob { jobId: string; attempt: number; leaseToken: string; leaseDurationMs: number; evidenceEventIds: string[]; evidence: Activity[]; trace?: import("./shared").TraceContext; }
export interface CollectorState {
  topology?: Topology;
  cursors: Record<string, Cursor>;
  files: Record<string, FileCursor>;
  activityIds: Set<string>;
  jobs: PendingJob[];
  sourceStatuses: SourceStatus[];
  remote: RemoteState;
}

const MAX_PENDING_JOBS = 100;
const MAX_RETAINED_EVIDENCE_BYTES = 512_000;
const MAX_TRANSCRIPT_READ_BYTES = 256_000;
const MAX_SUPPORTED_FRAME_BYTES = 2_000_000;
const MAX_OVERSIZED_SCAN_BYTES_PER_POLL = 256_000;
const MAX_DROPPED_FRAME_BYTES = 8_000_000;
const MAX_REMOTE_QUEUE_BYTES = 8_388_608;
const PROCESS_STOP_GRACE_MS = 250;
const MAX_REMOTE_RESPONSE_BYTES = COLLECTOR_WIRE_LIMITS.responseBytes;
const REMOTE_REQUEST_TIMEOUT_MS = 5_000;
const LEASE_RESERVE_MS = 5_000;

const stateFile = (dataDir: string) => join(dataDir, "collector-state.json");
const digest = (value: string | Uint8Array) => createHash("sha256").update(value).digest("hex");

export const initialState = (): CollectorState => ({ cursors: {}, files: {}, activityIds: new Set(), jobs: [], sourceStatuses: [], remote: { pendingBatches: [], acknowledgedCursors: {}, baselineGenerations: {}, baselineReadyParticipantIds: [], absentParticipantIds: [], topologySequence: 0, bootstrapped: false } });

class StateMissing extends Error {}
class StateRecoveryBlocked extends Error {}

export async function restore(dataDir: string): Promise<CollectorState> {
  const loaded = await Effect.runPromiseExit(Effect.tryPromise({
    try: () => readFile(stateFile(dataDir), "utf8"),
    catch: (error) => (typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT") ? new StateMissing() : new StateRecoveryBlocked(),
  }));
  if (Exit.isFailure(loaded)) {
    const error = Cause.findError(loaded.cause);
    if (Result.isSuccess(error) && error.success instanceof StateMissing) return initialState();
    throw new StateRecoveryBlocked();
  }
  const decoded = await Effect.runPromiseExit(Effect.try({
    try: () => JSON.parse(loaded.value) as Omit<CollectorState, "activityIds"> & { activityIds: string[] },
    catch: () => new StateRecoveryBlocked(),
  }));
  if (Exit.isFailure(decoded)) throw new StateRecoveryBlocked();
  const saved = decoded.value;
  if (!Array.isArray(saved.activityIds) || !saved.remote || !Array.isArray(saved.remote.pendingBatches) || !saved.cursors || !saved.files || !Array.isArray(saved.jobs) || !Array.isArray(saved.sourceStatuses)) throw new StateRecoveryBlocked();
  return { ...initialState(), ...saved, activityIds: new Set(saved.activityIds), remote: { ...initialState().remote, ...saved.remote } };
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
      remote: structuredClone(state.remote),
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

function canStageRemoteGroup(accepted: Activity[], activity: Activity): boolean {
  return accepted.length < MAX_PENDING_JOBS
    && accepted.reduce((total, item) => total + Buffer.byteLength(item.text), 0) + Buffer.byteLength(activity.text) <= MAX_RETAINED_EVIDENCE_BYTES;
}

function retainedParticipantIds(state: CollectorState, sourceId: string): string[] {
  if (state.topology?.source.id !== sourceId) return [];
  const pending = state.remote.pendingBatches.flatMap((batch) => {
    const topology = batch.body.topology as Partial<Topology> | undefined;
    return topology?.source?.id === sourceId && Array.isArray(topology.participants)
      ? topology.participants.map((participant) => participant.id)
      : [];
  });
  return [...new Set([
    ...Object.keys(state.cursors),
    ...Object.keys(state.remote.acknowledgedCursors),
    ...state.remote.absentParticipantIds,
    ...state.topology.participants.map((participant) => participant.id),
    ...pending,
  ])];
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
  const span = tracer.start("herdr.snapshot");
  try {
    const command = tracer.start("herdr.snapshot.command", span.context);
    const response = JSON.parse(await runCommand(["herdr", "api", "snapshot"], signal, 5_000, command)) as { result?: { snapshot?: unknown } };
    const topology = topologyFromHerdr(config.sourceNamespace, response.result?.snapshot as Parameters<typeof topologyFromHerdr>[1]);
    const excluded = exclusionPaneIds(config.sourceNamespace, config.excludePaneIds);
    await span.end("succeeded");
    return { ...topology, participants: topology.participants.filter((participant) => !excluded.has(participant.paneId)) };
  } catch (error) { await span.end(signal?.aborted ? "cancelled" : "failed"); throw Object.assign(error instanceof Error ? error : new Error(String(error)), { trace: span.context }); }
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

export async function captureParticipant(participant: Participant, state: CollectorState, signal?: AbortSignal, dataDir?: string, trace?: import("./shared").TraceContext, silentBaseline = false): Promise<ParticipantCapture> {
  if ((participant.kind !== "claude" && participant.kind !== "codex") || !participant.sessionId) return { activities: [], silentBaselineVerified: false };
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
        return { activities: [{ id: digest(`${participant.id}:rejected:${path}:${stats.dev}:${stats.ino}:${size}`), participantId: participant.id, sourceCursor: state.cursors[participant.id]!.sourceCursor, observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }], silentBaselineVerified: false };
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

      if (silentBaseline) {
        const finalByte = size ? await readHandleRange(file, size - 1, 1, signal) : new Uint8Array();
        state.files[participant.id] = {
          ...fileCursor,
          baselineAt: size,
          baselineDiscardOffset: finalByte.byteLength && finalByte[0] !== 10 ? size : undefined,
          baselineDiscardScanned: finalByte.byteLength && finalByte[0] !== 10 ? 0 : undefined,
        };
        state.cursors[participant.id] = {
          ...cursor,
          offset: size,
          sourceCursor: String(size),
          initialized: true,
          exactSessionVerified: true,
          discardOffset: undefined,
          discardScanned: undefined,
        };
        return { activities: [], silentBaselineVerified: true };
      }

      if (!previousFile) {
        state.files[participant.id] = fileCursor;
        state.cursors[participant.id] = { ...cursor, offset: size, sourceCursor: String(size), initialized: true, exactSessionVerified: true };
        return { activities: [], silentBaselineVerified: false };
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
        return { activities: [{ id: digest(`${participant.id}:gap:${path}:${stats.dev}:${stats.ino}:${size}`), participantId: participant.id, sourceCursor: "0", observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }], silentBaselineVerified: false };
      }

      if (previousFile.baselineDiscardOffset !== undefined) {
        const scanned = previousFile.baselineDiscardScanned ?? 0;
        const discard = await skipOversizedFrame(file, previousFile.baselineDiscardOffset, scanned, signal);
        state.files[participant.id] = { ...fileCursor, baselineDiscardOffset: discard.nextOffset === undefined ? previousFile.baselineDiscardOffset : undefined, baselineDiscardScanned: discard.nextOffset === undefined ? discard.scanned : undefined };
        if (discard.nextOffset === undefined) return { activities: [], silentBaselineVerified: false };
        state.cursors[participant.id] = { ...cursor, offset: discard.nextOffset, sourceCursor: String(discard.nextOffset), initialized: true, exactSessionVerified: true, discardOffset: undefined, discardScanned: undefined };
        return { activities: [], silentBaselineVerified: false };
      }

      const activeCursor = state.cursors[participant.id] ?? cursor;
      const readOffset = activeCursor.discardOffset ?? activeCursor.offset;
      const bytes = await readHandleRange(file, readOffset, MAX_TRANSCRIPT_READ_BYTES, signal);
      if (bytes.byteLength === MAX_TRANSCRIPT_READ_BYTES && readOffset + bytes.byteLength < size && bytes.lastIndexOf(10) < 0) {
        const scanned = activeCursor.discardOffset === undefined ? bytes.byteLength : activeCursor.discardScanned ?? bytes.byteLength;
        const discard = await skipOversizedFrame(file, activeCursor.offset, scanned, signal);
        if (discard.nextOffset && discard.nextOffset - activeCursor.offset <= MAX_SUPPORTED_FRAME_BYTES) {
          const complete = await readHandleRange(file, activeCursor.offset, discard.nextOffset - activeCursor.offset, signal);
          const result = captureStructured(participant.kind, participant.sessionId, { text: new TextDecoder().decode(complete), offset: activeCursor.offset, cursor: activeCursor }, participant);
          state.files[participant.id] = fileCursor;
          state.cursors[participant.id] = { ...result.cursor, discardOffset: undefined, discardScanned: undefined };
          if (!activeCursor.initialized || !result.cursor.exactSessionVerified) return { activities: [], silentBaselineVerified: false };
        return { activities: result.activities.filter((activity) => !state.activityIds.has(activity.id)), silentBaselineVerified: false };
        }

        state.files[participant.id] = fileCursor;
        if (discard.nextOffset || discard.exhausted) {
          const nextOffset = discard.nextOffset ?? size;
          state.cursors[participant.id] = {
            ...activeCursor,
            offset: nextOffset,
            sourceCursor: String(nextOffset),
            initialized: true,
            exactSessionVerified: false,
            discardOffset: undefined,
            discardScanned: undefined,
          };
          const text = "An oversized transcript record was skipped and session verification was revoked.";
          return { activities: [{ id: digest(`${participant.id}:oversized:${activeCursor.offset}`), participantId: participant.id, sourceCursor: String(activeCursor.offset), observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }], silentBaselineVerified: false };
        }

        state.cursors[participant.id] = {
          ...activeCursor,
          discardOffset: activeCursor.offset,
          discardScanned: discard.scanned,
        };
        if (!activeCursor.discardScanned) {
          const text = "An oversized transcript record is being skipped in bounded scans.";
          return { activities: [{ id: digest(`${participant.id}:oversized-scanning:${activeCursor.offset}`), participantId: participant.id, sourceCursor: String(activeCursor.offset), observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) }], silentBaselineVerified: false };
        }
        return { activities: [], silentBaselineVerified: false };
      }

      const result = captureStructured(participant.kind, participant.sessionId, { text: new TextDecoder().decode(bytes), offset: activeCursor.offset, cursor: activeCursor }, participant);
      state.files[participant.id] = fileCursor;
      state.cursors[participant.id] = result.cursor;
      if (!result.cursor.exactSessionVerified) return { activities: [], silentBaselineVerified: false };
      if (!activeCursor.initialized) return { activities: [], silentBaselineVerified: false };
      return { activities: result.activities.filter((activity) => !state.activityIds.has(activity.id)), silentBaselineVerified: false };
    } finally {
      await file.close();
    }
  }
  return { activities: [], silentBaselineVerified: false };
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
  const reporter = createErrorReporter({ dataDirectory: config.dataDir });
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
          }, undefined, undefined, signal, logger, tracer, job.activity.trace, reporter);
      if (signal?.aborted) break;
      if (!summary.speak) { job.done = true; job.error = undefined; continue; }
      const key = digest(`${summary.text}\0${Bun.env.ELEVENLABS_VOICE_ID}\0eleven_flash_v2_5`);
        const generated = await synthesize(summary.text, key, { apiKey: Bun.env.ELEVENLABS_API_KEY, voiceId: Bun.env.ELEVENLABS_VOICE_ID, dataDir: config.dataDir, logger, tracer, reporter, trace: job.activity.trace, operationId: job.activity.id, jobId: job.activity.id }, fetch, signal);
      if (signal?.aborted) break;
      const temporary = join(config.dataDir, `latest-announcement.${crypto.randomUUID()}.tmp`);
      const publishedKey = generated.path.split("/").at(-1)?.replace(/\.mp3$/, "");
      try {
        await writeFile(temporary, JSON.stringify({ key: publishedKey, text: summary.text, kind: summary.kind, createdAt: new Date().toISOString(), trace: job.activity.trace, announcementId: job.activity.id }));
        await rename(temporary, join(config.dataDir, "latest-announcement.json"));
      } catch (error) {
        await reporter.report({ service: "collector", operation: "collector.publication", category: "publication", error, trace: job.activity.trace, context: { activityId: job.activity.id, operationId: job.activity.id } });
        throw error;
      }
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

class TransportFailure extends Error {
  constructor(readonly operation: string, readonly code: "cancelled" | "network" | "timeout" | "malformed" | "conflict") { super(`Collector ${operation} ${code}`); }
}

type RemoteBootstrapConfig = CollectorConfig & { collectorUrl: string; collectorToken: string; workerId: string; };
type RemoteCollectorConfig = RemoteBootstrapConfig & { sourceEpoch: string; };
const remoteEnabled = (config: CollectorConfig): config is RemoteBootstrapConfig => Boolean(config.collectorUrl && config.collectorToken && config.workerId);

const canonical = encodeCollectorJson;

async function fetchAndReadBounded(config: RemoteCollectorConfig, url: URL, path: string, method: "GET" | "POST", body: unknown, controller: AbortController): Promise<{ status: number; body?: unknown }> {
  const response = await fetch(url, {
    method,
    signal: controller.signal,
    headers: { Authorization: `Bearer ${config.collectorToken}`, ...(body === undefined ? {} : { "Content-Type": "application/json" }) },
    ...(body === undefined ? {} : { body: typeof body === "string" ? body : JSON.stringify(body) }),
  });
  const reader = response.body?.getReader();
  if (!reader) return { status: response.status };
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  let eof = false;
  try {
    for (;;) {
      const part = await reader.read();
      if (part.done) { eof = true; break; }
      bytes += part.value.byteLength;
      if (bytes > MAX_REMOTE_RESPONSE_BYTES) {
        controller.abort();
        throw new TransportFailure("response body", "malformed");
      }
      chunks.push(part.value);
    }
    const text = new TextDecoder().decode(Buffer.concat(chunks));
    if (!text) return { status: response.status };
    try { return { status: response.status, body: JSON.parse(text) }; }
    catch { throw new TransportFailure(path, "malformed"); }
  } finally {
    if (!eof) controller.abort();
    await Promise.allSettled([Promise.resolve().then(() => reader.cancel())]);
    reader.releaseLock();
  }
}

function remoteRequestEffect(config: RemoteCollectorConfig, path: string, method: "GET" | "POST", body: unknown) {
  const url = new URL(path, config.collectorUrl);
  const request = Effect.callback<{ status: number; body?: unknown }, TransportFailure>((resume) => {
      const controller = new AbortController();
      const completion = fetchAndReadBounded(config, url, path, method, body, controller);
      const classify = (error: unknown) => error instanceof TransportFailure ? error : new TransportFailure(path, controller.signal.aborted ? "cancelled" : "network");
      void completion.then((value) => resume(Effect.succeed(value)), (error) => resume(Effect.fail(classify(error))));
      return Effect.ignore(Effect.andThen(
        Effect.sync(() => controller.abort()),
        Effect.tryPromise({ try: () => completion, catch: classify }),
      ));
    });
  return Effect.timeoutOrElse(Effect.interruptible(request), { duration: REMOTE_REQUEST_TIMEOUT_MS, orElse: () => Effect.fail(new TransportFailure(path, "timeout")) });
}

async function remoteRequest(config: RemoteCollectorConfig, path: string, method: "GET" | "POST", body: unknown, signal?: AbortSignal): Promise<{ status: number; body?: unknown }> {
  return Effect.runPromise(remoteRequestEffect(config, path, method, body), { signal });
}

const persistEffect = (dataDir: string, state: CollectorState) => Effect.callback<void, TransportFailure>((resume) => {
  const completion = persist(dataDir, state);
  const failure = () => new TransportFailure("spool", "network");
  void completion.then(() => resume(Effect.void), () => resume(Effect.fail(failure())));
  return Effect.ignore(Effect.tryPromise({ try: () => completion, catch: failure }));
});
const safeLogEffect = (logger: ReturnType<typeof collectorLogger>, entry: Parameters<ReturnType<typeof collectorLogger>["log"]>[1]) => Effect.ignore(Effect.tryPromise({ try: () => logger.log("warn", entry), catch: () => new TransportFailure("diagnostic", "network") }));

async function getRemoteConfig(config: RemoteCollectorConfig, sourceId: string, signal?: AbortSignal): Promise<RemoteConfig> {
  const response = await remoteRequest(config, `/api/collector/config?sourceId=${encodeURIComponent(sourceId)}`, "GET", undefined, signal);
  if (response.status !== 200 || !response.body || typeof response.body !== "object") throw new TransportFailure("config", "malformed");
  const value = response.body as Partial<RemoteConfig>;
  if (value.sourceId !== sourceId || !Number.isInteger(value.listeningGeneration) || (value.listeningScope !== null && (!value.listeningScope || typeof value.listeningScope !== "object"))) throw new TransportFailure("config", "malformed");
  return value as RemoteConfig;
}

function inListeningScope(topology: Topology, participant: Participant, scope: RemoteConfig["listeningScope"]): boolean {
  if (!scope) return false;
  const pane = topology.panes.find((item) => item.id === participant.paneId);
  if (!pane) return false;
  if (scope.tabId) return pane.tabId === scope.tabId;
  if (scope.workspaceId) return topology.tabs.find((item) => item.id === pane.tabId)?.workspaceId === scope.workspaceId;
  return false;
}

function recordAuthoritativeAbsence(state: CollectorState, previous: Topology | undefined, current: Topology, scope: RemoteConfig["listeningScope"] | undefined): void {
  if (!previous || !scope) return;
  const currentParticipantIds = new Set(current.participants.map((participant) => participant.id));
  const newlyAbsent = previous.participants
    .filter((participant) => participant.sessionId && inListeningScope(previous, participant, scope) && !currentParticipantIds.has(participant.id))
    .map((participant) => participant.id);
  state.remote.absentParticipantIds = [...new Set([...state.remote.absentParticipantIds, ...newlyAbsent])].sort();
}

function stageBootstrap(config: RemoteCollectorConfig, state: CollectorState, topology: Topology): RemoteBatch {
  const body: Record<string, unknown> = {
    sourceId: topology.source.id,
    sourceEpoch: config.sourceEpoch,
    batchId: crypto.randomUUID(),
    listeningGeneration: 0,
    topologySequence: state.remote.topologySequence + 1,
    topology,
    baselineReady: [],
    cursors: [],
    activities: [],
  };
  return { batchId: String(body.batchId), body, encoded: canonical(body), topologyDigest: digest(canonical(topology)) };
}

type BatchStage = { kind: "idle" } | { kind: "blocked" } | { kind: "planned"; batches: RemoteBatch[] };

function stageBatch(config: RemoteCollectorConfig, state: CollectorState, topology: Topology, remote: RemoteConfig, activities: Activity[]): BatchStage {
  if (state.remote.pendingBatches.length) return { kind: "idle" };
  const selected = topology.participants.filter((participant) => participant.sessionId && inListeningScope(topology, participant, remote.listeningScope));
  const baselineReady = selected.filter((participant) => state.remote.baselineReadyParticipantIds.includes(participant.id)).map((participant) => participant.id).sort();
  const silent = new Set(baselineReady);
  const publishable = activities.filter((activity) => !silent.has(activity.participantId));
  const topologyDigest = digest(canonical(topology));
  const heartbeatDue = !state.remote.lastTopologyPublishedAt || Date.now() - Date.parse(state.remote.lastTopologyPublishedAt) >= 30_000;
  const currentParticipantIds = new Set(topology.participants.map((participant) => participant.id));
  const cursors = Object.entries(state.cursors)
    .filter(([participantId, cursor]) => currentParticipantIds.has(participantId as Participant["id"])
      && (baselineReady.includes(participantId as Participant["id"]) || state.remote.acknowledgedCursors[participantId] !== cursor.sourceCursor))
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([participantId, cursor]) => ({ participantId, previous: state.remote.acknowledgedCursors[participantId] ?? null, next: cursor.sourceCursor }));
  const cursorChanged = cursors.length > 0;
  if (!publishable.length && !baselineReady.length && !cursorChanged && topologyDigest === state.remote.acknowledgedTopologyDigest && !heartbeatDue) return { kind: "idle" };
  const units = [
    ...publishable.map((activity) => ({ activities: [activity], cursors: [] as Array<{ participantId: string; previous: string | null; next: string }>, baselineReady: [] as string[] })),
    ...cursors.map((cursor) => ({ activities: [] as Activity[], cursors: [cursor], baselineReady: baselineReady.includes(cursor.participantId as Participant["id"]) ? [cursor.participantId] : [] })),
  ];
  const make = (items: typeof units, sequence: number): RemoteBatch => {
    const body: Record<string, unknown> = { sourceId: topology.source.id, sourceEpoch: config.sourceEpoch, batchId: crypto.randomUUID(), listeningGeneration: remote.listeningGeneration, topologySequence: sequence, topology, baselineReady: items.flatMap((item) => item.baselineReady), cursors: items.flatMap((item) => item.cursors), activities: items.flatMap((item) => item.activities) };
    return { batchId: String(body.batchId), body, encoded: canonical(body), topologyDigest };
  };
  if (!units.length) {
    const batch = make([], state.remote.topologySequence + 1);
    return collectorUtf8Bytes(batch.encoded) <= COLLECTOR_WIRE_LIMITS.requestBytes ? { kind: "planned", batches: [batch] } : { kind: "blocked" };
  }
  const batches: RemoteBatch[] = [];
  let packed: typeof units = [];
  for (const unit of units) {
    const candidate = make([...packed, unit], state.remote.topologySequence + batches.length + 1);
    if (collectorUtf8Bytes(candidate.encoded) <= COLLECTOR_WIRE_LIMITS.requestBytes) { packed.push(unit); continue; }
    if (!packed.length) return { kind: "blocked" };
    batches.push(make(packed, state.remote.topologySequence + batches.length + 1));
    packed = [unit];
    if (collectorUtf8Bytes(make(packed, state.remote.topologySequence + batches.length + 1).encoded) > COLLECTOR_WIRE_LIMITS.requestBytes) return { kind: "blocked" };
  }
  batches.push(make(packed, state.remote.topologySequence + batches.length + 1));
  return { kind: "planned", batches };
}

function acknowledgedReceipt(batch: RemoteBatch, receipt: unknown): boolean {
  if (!receipt || typeof receipt !== "object") return false;
  const value = receipt as { batchId?: unknown; acceptedEventIds?: unknown; cursors?: unknown; topologySequence?: unknown };
  const activities = batch.body.activities as Activity[];
  const expectedIds = activities.map((activity) => activity.id).sort();
  const actualIds = Array.isArray(value.acceptedEventIds) ? [...value.acceptedEventIds].filter((id): id is string => typeof id === "string").sort() : [];
  const submitted = batch.body.cursors as Array<{ participantId: string; next: string }>;
  const received = Array.isArray(value.cursors) ? value.cursors : [];
  const exactCursors = received.length === submitted.length && received.every((cursor) => {
    if (!cursor || typeof cursor !== "object") return false;
    const item = cursor as { participantId?: unknown; cursor?: unknown };
    return typeof item.participantId === "string" && typeof item.cursor === "string" && submitted.some((expected) => expected.participantId === item.participantId && expected.next === item.cursor);
  }) && new Set(received.map((cursor) => (cursor as { participantId: string }).participantId)).size === received.length;
  return value.batchId === batch.batchId && value.topologySequence === batch.body.topologySequence && canonical(actualIds) === canonical(expectedIds) && exactCursors;
}

async function publishPending(config: RemoteCollectorConfig, state: CollectorState, signal?: AbortSignal): Promise<boolean> {
  const batch = state.remote.pendingBatches[0];
  if (!batch) return false;
  if (collectorUtf8Bytes(batch.encoded) > COLLECTOR_WIRE_LIMITS.requestBytes) {
    addStatus(state, "source", "overflow", "Collector retry spool head exceeds the supported wire limit and is recovery-blocked.");
    return false;
  }
  const response = await remoteRequest(config, "/api/collector/batches", "POST", batch.encoded, signal);
  if (response.status !== 200 && response.status !== 201) {
    addStatus(state, "source", "unavailable", response.status === 409 ? "Collector batch conflict is recoverable and remains queued." : "Collector batch acknowledgement is unavailable.");
    return false;
  }
  if (!acknowledgedReceipt(batch, response.body)) {
    addStatus(state, "source", "unavailable", "Collector batch receipt is invalid and remains queued.");
    return false;
  }
  const next = cloneState(state);
  for (const cursor of batch.body.cursors as Array<{ participantId: string; next: string }>) next.remote.acknowledgedCursors[cursor.participantId] = cursor.next;
  for (const participantId of batch.body.baselineReady as string[]) next.remote.baselineGenerations[participantId] = Number(batch.body.listeningGeneration);
  next.remote.baselineReadyParticipantIds = next.remote.baselineReadyParticipantIds.filter((participantId) => !(batch.body.baselineReady as string[]).includes(participantId));
  next.remote.absentParticipantIds = next.remote.absentParticipantIds.filter((participantId) => !(batch.body.baselineReady as string[]).includes(participantId));
  next.remote.topologySequence = Math.max(next.remote.topologySequence, Number(batch.body.topologySequence));
  next.remote.bootstrapped ||= Number(batch.body.listeningGeneration) === 0;
  next.remote.acknowledgedTopologyDigest = batch.topologyDigest;
  next.remote.lastTopologyPublishedAt = new Date().toISOString();
  next.remote.pendingBatches.shift();
  await Effect.runPromise(persistEffect(config.dataDir, next), { signal });
  Object.assign(state, next);
  return true;
}

async function processRemoteJob(config: RemoteCollectorConfig, remote: RemoteConfig, state: CollectorState, signal?: AbortSignal): Promise<void> {
  const logger = collectorLogger({ dataDirectory: config.dataDir });
  const tracer = collectorTracer({ dataDirectory: config.dataDir });
  const reporter = createErrorReporter({ dataDirectory: config.dataDir });
  const program = Effect.gen(function*() {
    const claimStartedAt = performance.now();
    const claimed = yield* remoteRequestEffect(config, "/api/collector/jobs/claim", "POST", { workerId: config.workerId });
    if (claimed.status === 204) return;
    if (claimed.status !== 200 || !claimed.body || typeof claimed.body !== "object") return yield* Effect.fail(new TransportFailure("job claim", "malformed"));
    const job = claimed.body as Partial<RemoteJob>;
    const leaseDurationMs = job.leaseDurationMs;
    if (typeof job.jobId !== "string" || typeof job.attempt !== "number" || typeof job.leaseToken !== "string" || typeof leaseDurationMs !== "number" || !Number.isFinite(leaseDurationMs) || leaseDurationMs <= 0 || !Array.isArray(job.evidenceEventIds) || job.evidenceEventIds.some((id) => typeof id !== "string") || !Array.isArray(job.evidence) || job.evidence.some((activity) => !activity || typeof activity !== "object" || typeof (activity as Activity).id !== "string" || typeof (activity as Activity).text !== "string")) return yield* Effect.fail(new TransportFailure("job claim", "malformed"));
    const evidenceEventIds = job.evidenceEventIds as string[];
    const evidence = job.evidence as Activity[];
    if (evidenceEventIds.some((id) => !evidence.some((activity) => activity.id === id))) return yield* Effect.fail(new TransportFailure("job claim", "malformed"));
    const configuredTimeoutMs = Number(Bun.env.SUMMARY_TIMEOUT_MS ?? 30_000);
    const remainingLeaseMs = leaseDurationMs - (performance.now() - claimStartedAt) - LEASE_RESERVE_MS;
    if (!Number.isFinite(configuredTimeoutMs) || configuredTimeoutMs <= 0 || remainingLeaseMs <= 0) return yield* Effect.fail(new TransportFailure("summary job", "timeout"));
    const summaryTimeoutMs = Math.min(configuredTimeoutMs, remainingLeaseMs);
    const summary = yield* Effect.timeoutOrElse(Effect.interruptible(Effect.callback<import("./summarizer").Summary, TransportFailure>((resume) => {
        const controller = new AbortController();
        const completion = summarize({ text: evidence.map(formatEvidence).join("\n\n"), evidenceEventIds, retry: Math.max(0, job.attempt! - 1) }, undefined, undefined, controller.signal, logger, tracer, job.trace, reporter);
        const classify = (error: unknown) => error instanceof TransportFailure ? error : new TransportFailure("summary job", controller.signal.aborted ? "cancelled" : "network");
        void completion.then((value) => resume(Effect.succeed(value)), (error) => resume(Effect.fail(classify(error))));
        return Effect.ignore(Effect.andThen(
          Effect.sync(() => controller.abort()),
          Effect.tryPromise({ try: () => completion, catch: classify }),
        ));
      })), { duration: summaryTimeoutMs, orElse: () => Effect.fail(new TransportFailure("summary job", "timeout")) });
    const result = { leaseToken: job.leaseToken, resultKey: digest(`${job.jobId}\0${canonical(summary)}`), result: summary };
    const completed = yield* remoteRequestEffect(config, `/api/collector/jobs/${encodeURIComponent(job.jobId)}/result`, "POST", result);
    if (completed.status === 200 || completed.status === 201) return;
    return yield* Effect.fail(new TransportFailure("job result", completed.status === 409 ? "conflict" : "network"));
  });
  const exit = await Effect.runPromiseExit(program, { signal });
  if (Exit.isFailure(exit) && !signal?.aborted) {
    const error = Cause.findError(exit.cause);
    const code = Result.isSuccess(error) && error.success instanceof TransportFailure ? error.success.code : "network";
    await Effect.runPromise(safeLogEffect(logger, { operation: "collector.job", message: "Leased summary job remains recoverable", outcome: "retried", metadata: { status: code } }));
  }
}

export async function runOnce(config: CollectorConfig, state: CollectorState, signal?: AbortSignal): Promise<{ activities: Activity[]; clip?: string }> {
  const logger = collectorLogger({ dataDirectory: config.dataDir });
  const tracer = collectorTracer({ dataDirectory: config.dataDir });
  const reporter = createErrorReporter({ dataDirectory: config.dataDir });
  throwIfAborted(signal);
  let observed: Topology;
  try {
    observed = await readTopology(config, signal);
  } catch (error) {
      if (!state.topology) { if (!signal?.aborted) await reporter.report({ service: "collector", operation: "capture.topology", category: "capture", error, trace: (error as Error & { trace?: import("./shared").TraceContext }).trace }); throw error; }
    state.topology.source.stale = true;
    addStatus(state, "source", "error", error instanceof Error ? error.message.slice(0, 240) : "Topology observation failed");
    await persist(config.dataDir, state);
    return { activities: [] };
  }
  const previousTopology = state.topology;
  let next = cloneState(state);
  next.topology = next.topology ? reconcileTopology(next.topology, observed, retainedParticipantIds(state, observed.source.id)) : observed;
  next.topology.source.stale = false;
  recordAuthoritativeAbsence(next, previousTopology, next.topology, next.remote.listeningScope);
  let transport: RemoteCollectorConfig | undefined;
  let remote: RemoteConfig | undefined;
  if (remoteEnabled(config)) {
    if (collectorUtf8Bytes(canonical(next.topology)) > COLLECTOR_WIRE_LIMITS.topologyBytes) {
      addStatus(next, "source", "overflow", "Observed topology exceeds the supported collector wire bound.");
      Object.assign(state, next);
      await Effect.runPromise(persistEffect(config.dataDir, state), { signal });
      return { activities: [] };
    }
    if (config.sourceEpoch && next.remote.sourceEpoch && config.sourceEpoch !== next.remote.sourceEpoch) {
      addStatus(next, "source", "unavailable", "Configured collector epoch conflicts with persisted source identity.");
      Object.assign(state, next);
      await Effect.runPromise(persistEffect(config.dataDir, state), { signal });
      return { activities: [] };
    }
    transport = { ...config, sourceEpoch: config.sourceEpoch ?? next.remote.sourceEpoch ?? crypto.randomUUID() };
    next.remote.sourceEpoch = transport.sourceEpoch;
    await Effect.runPromise(persistEffect(config.dataDir, next), { signal });
    if (!next.remote.bootstrapped && !next.remote.pendingBatches.length) {
      next.remote.pendingBatches.push(stageBootstrap(transport, next, next.topology));
      await Effect.runPromise(persistEffect(config.dataDir, next), { signal });
    }
    if (next.remote.pendingBatches.length) {
      Object.assign(state, next);
      if (!await publishPending(transport, state, signal)) {
        await Effect.runPromise(persistEffect(config.dataDir, state), { signal });
        await logger.flush();
        return { activities: [] };
      }
      if (state.remote.pendingBatches.length) {
        await logger.flush();
        return { activities: [] };
      }
      next = cloneState(state);
    }
    if (!next.remote.bootstrapped) {
      await logger.flush();
      return { activities: [] };
    }
    const remoteExit = await Effect.runPromiseExit(Effect.tryPromise({ try: () => getRemoteConfig(transport!, next.topology!.source.id, signal), catch: (error) => error instanceof TransportFailure ? error : new TransportFailure("config", signal?.aborted ? "cancelled" : "network") }), { signal });
    if (Exit.isFailure(remoteExit)) {
      if (!signal?.aborted) addStatus(next, "source", "unavailable", "Collector transport configuration is unavailable.");
      Object.assign(state, next);
      await Effect.runPromise(persistEffect(config.dataDir, state), { signal });
      return { activities: [] };
    }
    remote = remoteExit.value;
    next.remote.listeningScope = remote.listeningScope;
    recordAuthoritativeAbsence(next, previousTopology, next.topology!, remote.listeningScope);
  }
  const captureCheckpoint = cloneState(next);
  const participants = [...new Map(next.topology!.participants.map((item) => [participantIdentity(item), item])).values()];
  const accepted: Activity[] = [];
  for (const participant of participants) {
    throwIfAborted(signal);
    const staged = cloneState(next);
    const baselineRequired = Boolean(remote && participant.sessionId && inListeningScope(next.topology!, participant, remote.listeningScope)
      && (next.remote.absentParticipantIds.includes(participant.id) || next.remote.baselineGenerations[participant.id] !== remote.listeningGeneration));
    const captureSpan = tracer.start(participant.sessionId ? "capture.transcript" : "capture.terminal", { sourceId: next.topology!.source.id, participantId: participant.id });
    try {
      const transcriptCapture = participant.sessionId
        ? await captureParticipant(participant, staged, signal, config.dataDir, captureSpan.context, baselineRequired)
        : undefined;
      const activities = transcriptCapture?.activities
        ?? await captureTerminalFallback(participant, staged, signal, config.dataDir, captureSpan.context);
        for (const activity of activities) activity.trace = captureSpan.context;
        for (const activity of activities.filter((activity) => activity.status === "gap")) await reporter.report({ service: "collector", operation: "capture.observe", category: "malformed_output", error: new Error("Capture produced an unrecoverable gap"), trace: captureSpan.context, context: { activityId: activity.id, captureMode: activity.captureMode, originalTextBytes: activity.originalTextBytes } });
      for (const activity of activities) await logger.log(activity.status === "gap" ? "warn" : "info", { operation: activity.captureMode === "structured" ? "capture.transcript" : "capture.terminal", message: "Captured observation activity", outcome: activity.status, participantId: participant.id, sourceId: next.topology!.source.id, metadata: { operationId: activity.id, sourceCursor: activity.sourceCursor, originalTextBytes: activity.originalTextBytes, sourceNamespace: config.sourceNamespace } });
      if (baselineRequired) {
        if (transcriptCapture?.silentBaselineVerified) {
          next.cursors[participant.id] = staged.cursors[participant.id]!;
          next.files[participant.id] = staged.files[participant.id]!;
          next.remote.baselineReadyParticipantIds = [...new Set([...next.remote.baselineReadyParticipantIds, participant.id])].sort();
          next.sourceStatuses = next.sourceStatuses.filter((status) => status.participantId !== participant.id);
        } else {
          addStatus(next, participant.id, "unavailable", "Exact source transcript could not verify a silent return baseline.");
        }
        await captureSpan.end("succeeded");
        continue;
      }
      const admitted: Activity[] = [];
      for (const activity of activities) {
        const remoteGroup = [...accepted, ...admitted.filter((item) => !next.activityIds.has(item.id))];
        if (remote ? (!next.activityIds.has(activity.id) && !canStageRemoteGroup(remoteGroup, activity)) : !canEnqueue(next, [...admitted, activity])) break;
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
      for (const activity of admitted) {
        if (remote) {
          if (!next.activityIds.has(activity.id)) {
            next.activityIds.add(activity.id);
            accepted.push(activity);
          }
        } else enqueueActivity(next, activity, accepted);
      }
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
        if (signal?.aborted) break;
        addStatus(next, participant.id, participant.sessionId ? "unavailable" : "limited", error instanceof Error ? error.message.slice(0, 240) : "Source observation failed");
        await logger.log("error", { operation: "capture.observe", message: "Capture observation failed", outcome: "failed", participantId: participant.id, sourceId: next.topology!.source.id, metadata: { error: error instanceof Error ? error.message : "unknown", sourceNamespace: config.sourceNamespace } });
        await reporter.report({ service: "collector", operation: "capture.observe", category: "capture", error, trace: captureSpan.context, context: { participantId: participant.id } });
    }
  }
  throwIfAborted(signal);
  if (remote && transport && next.topology) {
    const stage = stageBatch(transport, next, next.topology, remote, accepted);
    const plannedBytes = stage.kind === "planned" ? stage.batches.reduce((total, batch) => total + collectorUtf8Bytes(batch.encoded), 0) : 0;
    const queuedBytes = next.remote.pendingBatches.reduce((total, batch) => total + collectorUtf8Bytes(batch.encoded), 0) + plannedBytes;
    if (stage.kind === "blocked" || queuedBytes > MAX_REMOTE_QUEUE_BYTES) {
      const blocked = cloneState(captureCheckpoint);
      blocked.sourceStatuses = next.sourceStatuses;
      addStatus(blocked, "source", "overflow", stage.kind === "blocked"
        ? "Observed capture cannot fit in a supported immutable collector envelope."
        : "Collector retry spool would exceed its supported queue bound.");
      await Effect.runPromise(persistEffect(config.dataDir, blocked), { signal });
      Object.assign(state, blocked);
      await logger.flush();
      return { activities: [] };
    }
    if (stage.kind === "planned") next.remote.pendingBatches.push(...stage.batches);
  }
  await persist(config.dataDir, next);
  Object.assign(state, next);
  let clip: string | undefined;
  if (remote && transport && state.topology) {
    if (state.remote.pendingBatches.length) await publishPending(transport, state, signal);
    if (!state.remote.pendingBatches.length) await processRemoteJob(transport, remote, state, signal);
  } else {
    clip = await processPending(config, state, signal);
  }
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
  const config = { sourceNamespace: Bun.env.SOURCE_NAMESPACE ?? "local", sourceEpoch: Bun.env.SOURCE_EPOCH, dataDir: Bun.env.SPEAK_NOW_DATA_DIR ?? "data", pollMs: Number(Bun.env.POLL_MS ?? 1_000), collectorUrl: Bun.env.SPEAK_NOW_COLLECTOR_URL, collectorToken: Bun.env.SPEAK_NOW_COLLECTOR_TOKEN, workerId: Bun.env.SUMMARY_WORKER_ID ?? "host-worker", excludePaneIds: (Bun.env.OBSERVATION_EXCLUDE_PANES ?? process.env.HERDR_PANE_ID ?? "").split(",").filter(Boolean) };
  void watch(config, controller.signal).finally(() => collectorLogger({ dataDirectory: config.dataDir }).flush());
}
