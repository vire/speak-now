import { appLogger } from "./logging";
import { appTracer } from "./tracing";
import { createErrorReporter } from "./errors";
import { createServer } from "node:http";
import { timingSafeEqual } from "node:crypto";
import { Cause, Deferred, Effect, Exit, FiberSet } from "effect";
import { leaseDurationMs, maxAttempts, openStorage, type CatchUpRequest, type HistoryQuery, type IngestBatch, type JobCompletion, type ListeningSelection, type PlaybackSettingsPatch, type StateQuery, type Storage } from "./storage";
import type { ParticipantId, SourceId } from "./shared";
import { validateSummary } from "./summarizer";
import { COLLECTOR_WIRE_LIMITS, collectorUtf8Bytes, encodeCollectorJson } from "./shared";
import { synthesize } from "./speech";

const port = Number(Bun.env.PORT ?? 3000);
const dataDir = Bun.env.SPEAK_NOW_DATA_DIR ?? "data";
const logger = appLogger({ dataDirectory: dataDir });
const tracer = appTracer({ dataDirectory: dataDir });
const errors = createErrorReporter({ dataDirectory: dataDir });
const clientErrorLimit = 8_192;
const collectorBodyLimit = COLLECTOR_WIRE_LIMITS.requestBytes;
const collectorToken = Bun.env.SPEAK_NOW_COLLECTOR_TOKEN;
const traceContext = (value: string | string[] | undefined) => {
  const match = (Array.isArray(value) ? value[0] : value ?? "").match(/^00-([a-f0-9]{32})-([a-f0-9]{16})-[a-f0-9]{2}$/i);
  return match ? { traceId: match[1], spanId: match[2] } : undefined;
};

const json = (response: import("node:http").ServerResponse, status: number, value: unknown) => { response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }); response.end(JSON.stringify(value)); };
const respond = (response: import("node:http").ServerResponse, status: number, value: unknown) => Effect.sync(() => json(response, status, value));

type CollectorBody = { kind: "json"; value: unknown } | { kind: "invalid" } | { kind: "too_large" };

const collectorResponse = (response: import("node:http").ServerResponse, status: number, code: string) => json(response, status, { code });
const collectorRespond = (response: import("node:http").ServerResponse, status: number, code: string) => respond(response, status, { code });
const collectorJson = (response: import("node:http").ServerResponse, status: number, value: unknown) => Effect.sync(() => {
  const encoded = encodeCollectorJson(value);
  if (collectorUtf8Bytes(encoded) > COLLECTOR_WIRE_LIMITS.responseBytes) return collectorResponse(response, 500, "response_too_large");
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  response.end(encoded);
});

const matchesCollectorToken = (authorization: string | undefined) => {
  if (!collectorToken) return false;
  const expected = Buffer.from(`Bearer ${collectorToken}`);
  const actual = Buffer.from(authorization ?? "");
  const width = Math.max(expected.length, actual.length);
  const left = Buffer.alloc(width);
  const right = Buffer.alloc(width);
  expected.copy(left);
  actual.copy(right);
  return timingSafeEqual(left, right) && expected.length === actual.length;
};

const readJson = (request: import("node:http").IncomingMessage, limit: number = collectorBodyLimit) => {
  return Effect.callback<CollectorBody>((resume, signal) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    let settled = false;
    const cleanup = () => {
      request.off("data", onData);
      request.off("end", onEnd);
      request.off("error", onError);
      request.off("aborted", onAborted);
      signal.removeEventListener("abort", onAborted);
    };
    const finish = (result: CollectorBody, drain = false) => {
      if (settled) return;
      settled = true;
      if (drain) {
        signal.removeEventListener("abort", onAborted);
        resume(Effect.succeed(result));
        return;
      }
      cleanup();
      resume(Effect.succeed(result));
    };
    const onData = (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > limit) {
        request.resume();
        finish({ kind: "too_large" }, true);
        return;
      }
      chunks.push(Buffer.from(chunk));
    };
    const onEnd = () => {
      if (settled) { cleanup(); return; }
      try {
        finish({ kind: "json", value: JSON.parse(Buffer.concat(chunks).toString("utf8")) });
      } catch {
        finish({ kind: "invalid" });
      }
    };
    const onError = () => finish({ kind: "invalid" });
    const onAborted = () => finish({ kind: "invalid" });
    request.on("data", onData);
    request.once("end", onEnd);
    request.once("error", onError);
    request.once("aborted", onAborted);
    signal.addEventListener("abort", onAborted, { once: true });
    return Effect.sync(cleanup);
  });
};

const readCollectorJson = (request: import("node:http").IncomingMessage) => request.headers["content-type"] === "application/json" ? readJson(request) : Effect.succeed<CollectorBody>({ kind: "invalid" });

const record = (value: unknown): Record<string, unknown> | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
const text = (value: unknown, limit = 512) => typeof value === "string" && value.length > 0 && value.length <= limit;
const bounded = (value: unknown, limit = 512) => typeof value === "string" && value.length <= limit;
const collectorText = (value: unknown, limit: number, required = true) => typeof value === "string" && (!required || value.length > 0) && collectorUtf8Bytes(value) <= limit;
const exactKeys = (value: Record<string, unknown>, required: readonly string[], optional: readonly string[] = []) => required.every((key) => key in value) && Object.keys(value).every((key) => required.includes(key) || optional.includes(key));
const nonnegativeInteger = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const timestamp = (value: unknown): value is string => typeof value === "string" && text(value, 64) && Number.isFinite(Date.parse(value));
const trace = (value: unknown) => {
  const entry = record(value);
  if (!entry || !exactKeys(entry, ["traceId", "spanId"], ["parentSpanId", "sourceId", "participantId", "jobId", "announcementId", "clipId"])) return false;
  return typeof entry.traceId === "string" && /^[a-f0-9]{32}$/i.test(entry.traceId) && typeof entry.spanId === "string" && /^[a-f0-9]{16}$/i.test(entry.spanId)
    && (entry.parentSpanId === undefined || typeof entry.parentSpanId === "string" && /^[a-f0-9]{16}$/i.test(entry.parentSpanId))
    && [entry.sourceId, entry.participantId, entry.jobId, entry.announcementId, entry.clipId].every((id) => id === undefined || text(id));
};
const hasForbiddenField = (value: unknown): boolean => {
  if (Array.isArray(value)) return value.some(hasForbiddenField);
  const entry = record(value); if (!entry) return false;
  return Object.entries(entry).some(([key, child]) => ["command", "path", "filepath", "argv", "shell"].includes(key.toLowerCase()) || hasForbiddenField(child));
};
const isCollectorBatchShape = (value: unknown): value is IngestBatch => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const body = value as Record<string, unknown>;
  if (hasForbiddenField(body)) return false;
  if (!exactKeys(body, ["sourceId", "sourceEpoch", "batchId", "listeningGeneration", "topologySequence", "topology", "baselineReady", "cursors", "activities"]) || !text(body.sourceId) || !text(body.sourceEpoch) || !text(body.batchId) || !nonnegativeInteger(body.listeningGeneration) || !nonnegativeInteger(body.topologySequence) || body.topologySequence === 0
    || !Array.isArray(body.baselineReady) || !Array.isArray(body.cursors) || !Array.isArray(body.activities) || body.baselineReady.length > COLLECTOR_WIRE_LIMITS.maxArrayItems || body.cursors.length > COLLECTOR_WIRE_LIMITS.maxArrayItems || body.activities.length > COLLECTOR_WIRE_LIMITS.maxArrayItems) return false;
  const topology = record(body.topology);
  if (!topology || !record(topology.source) || !Array.isArray(topology.workspaces) || !Array.isArray(topology.tabs) || !Array.isArray(topology.panes) || !Array.isArray(topology.participants) || [topology.workspaces, topology.tabs, topology.panes, topology.participants].some((items) => items.length > COLLECTOR_WIRE_LIMITS.maxArrayItems)) return false;
  const source = topology.source as Record<string, unknown>;
  if (!exactKeys(topology, ["source", "workspaces", "tabs", "panes", "participants"]) || !exactKeys(source, ["id", "namespace", "stale", "observedAt"]) || source.id !== body.sourceId || !text(source.namespace, 128) || typeof source.stale !== "boolean" || !timestamp(source.observedAt)) return false;
  const ids = (items: unknown[]): string[] | undefined => {
    const values: string[] = [];
    for (const item of items) { const entry = record(item); const id = entry?.id; if (!entry || !text(id)) return undefined; values.push(id as string); }
    return values;
  };
  const workspaceIds = ids(topology.workspaces); const tabIds = ids(topology.tabs); const paneIds = ids(topology.panes); const participantIds = ids(topology.participants);
  const prefix = `${source.namespace}:`;
  if (!workspaceIds || !tabIds || !paneIds || !participantIds || ![source.id, ...workspaceIds, ...tabIds, ...paneIds, ...participantIds].every((id) => typeof id === "string" && id.startsWith(prefix)) || ![workspaceIds, tabIds, paneIds, participantIds].every((items) => new Set(items).size === items.length)) return false;
  if (topology.workspaces.some((item) => { const workspace = record(item); return !workspace || !exactKeys(workspace, ["id", "sourceId", "label", "order", "live"]) || workspace.sourceId !== body.sourceId || !bounded(workspace.label) || !nonnegativeInteger(workspace.order) || typeof workspace.live !== "boolean"; })
    || topology.tabs.some((item) => { const tab = record(item); return !tab || !exactKeys(tab, ["id", "workspaceId", "label", "order"]) || !workspaceIds.includes(tab.workspaceId as string) || !bounded(tab.label) || !nonnegativeInteger(tab.order); })
    || topology.panes.some((item) => { const pane = record(item); return !pane || !exactKeys(pane, ["id", "tabId", "terminalId"], ["cwd", "label"]) || !tabIds.includes(pane.tabId as string) || !text(pane.terminalId) || (pane.cwd !== undefined && !bounded(pane.cwd)) || (pane.label !== undefined && !bounded(pane.label)); })
    || topology.participants.some((item) => { const participant = record(item); return !participant || !exactKeys(participant, ["id", "sourceId", "paneId", "rawPaneId", "terminalId", "kind", "generation", "active"], ["sessionId", "sessionReferenceKind", "sessionReferenceSource"]) || participant.sourceId !== body.sourceId || !paneIds.includes(participant.paneId as string) || !text(participant.rawPaneId) || !text(participant.terminalId) || typeof participant.kind !== "string" || !["claude", "codex", "terminal", "unsupported"].includes(participant.kind) || !nonnegativeInteger(participant.generation) || typeof participant.active !== "boolean" || [participant.sessionId, participant.sessionReferenceKind, participant.sessionReferenceSource].some((field) => field !== undefined && !text(field)); })) return false;
  const cursorIds = body.cursors.map((item) => record(item)?.participantId).filter((id): id is string => typeof id === "string");
  if (!body.baselineReady.every((id) => typeof id === "string" && text(id) && participantIds.includes(id) && cursorIds.includes(id)) || new Set(body.baselineReady).size !== body.baselineReady.length || new Set(cursorIds).size !== body.cursors.length || !body.cursors.every((item) => { const cursor = record(item); return cursor && exactKeys(cursor, ["participantId", "previous", "next"]) && typeof cursor.participantId === "string" && text(cursor.participantId) && (cursor.previous === null || collectorText(cursor.previous, COLLECTOR_WIRE_LIMITS.cursorTokenBytes, false)) && collectorText(cursor.next, COLLECTOR_WIRE_LIMITS.cursorTokenBytes, false); }) || !body.activities.every((item) => { const activity = record(item); const emptyGap = activity?.text === "" && (activity.kind === "lifecycle" || activity.status === "gap"); return activity && exactKeys(activity, ["id", "participantId", "sourceCursor", "observedAt", "kind", "text", "captureMode", "status", "truncated", "excerpt", "originalTextBytes"], ["stableMessageId", "revisionId", "revisionOf", "trace"]) && typeof activity.participantId === "string" && text(activity.id) && text(activity.participantId) && participantIds.includes(activity.participantId) && text(activity.sourceCursor) && timestamp(activity.observedAt) && typeof activity.kind === "string" && ["assistant", "tool", "lifecycle", "unknown"].includes(activity.kind) && typeof activity.captureMode === "string" && ["structured", "terminal"].includes(activity.captureMode) && typeof activity.status === "string" && ["complete", "partial", "gap", "limited"].includes(activity.status) && typeof activity.truncated === "boolean" && typeof activity.excerpt === "string" && ["full", "tail"].includes(activity.excerpt) && nonnegativeInteger(activity.originalTextBytes) && [activity.stableMessageId, activity.revisionId, activity.revisionOf].every((id) => id === undefined || text(id)) && (activity.trace === undefined || trace(activity.trace)) && (collectorText(activity.text, COLLECTOR_WIRE_LIMITS.activityTextBytes) || (emptyGap && collectorText(activity.text, COLLECTOR_WIRE_LIMITS.activityTextBytes, false))); })) return false;
  return true;
};

const collectorIdentity = (value: unknown): { body: Record<string, unknown>; sourceId: string; sourceEpoch: string; batchId: string } | undefined => {
  const body = record(value);
  const sourceId = body?.sourceId;
  const sourceEpoch = body?.sourceEpoch;
  const batchId = body?.batchId;
  return body && text(sourceId) && text(sourceEpoch) && text(batchId)
    ? { body, sourceId: sourceId as string, sourceEpoch: sourceEpoch as string, batchId: batchId as string }
    : undefined;
};

const isListeningSelection = (value: unknown) => {
  if (value === null) return true;
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const selection = value as Record<string, unknown>;
  const workspace = Object.hasOwn(selection, "workspaceId");
  const tab = Object.hasOwn(selection, "tabId");
  return exactKeys(selection, ["sourceId"], ["workspaceId", "tabId"]) && text(selection.sourceId) && workspace !== tab && (!workspace || text(selection.workspaceId)) && (!tab || text(selection.tabId));
};

const isCatchUpRequest = (value: unknown): value is CatchUpRequest => {
  const request = record(value);
  return !!request && exactKeys(request, ["sourceId", "generation", "requestId"]) && text(request.sourceId) && nonnegativeInteger(request.generation) && text(request.requestId);
};

const validatedCompletion = (value: unknown): JobCompletion | undefined => {
  const completion = record(value);
  const result = completion && record(completion.result);
  if (!completion || !result || !exactKeys(completion, ["leaseToken", "resultKey", "result"]) || !text(completion.leaseToken) || !text(completion.resultKey) || !Array.isArray(result.evidenceEventIds) || !result.evidenceEventIds.every((id) => text(id))) return undefined;
  try { return { leaseToken: completion.leaseToken as string, resultKey: completion.resultKey as string, result: validateSummary(result, result.evidenceEventIds as string[]) }; } catch { return undefined; }
};

const sameOrigin = (request: import("node:http").IncomingMessage) => request.headers.origin === undefined || request.headers.origin === `http://${request.headers.host}`;

const isEventSequence = (value: string | undefined) => value === undefined || (/^(0|[1-9][0-9]*)$/.test(value) && Number.isSafeInteger(Number(value)));

const isWorkerClaim = (value: unknown) => {
  const body = record(value);
  return !!body && exactKeys(body, ["workerId"]) && !hasForbiddenField(body) && text(body.workerId, 128);
};

const historyTimestamp = (value: string | undefined) => {
  if (!value || !text(value, 64)) return undefined;
  const match = value.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|[+-]\d{2}:\d{2})$/);
  if (!match) return undefined;
  const [year, month, day, hour, minute, second] = match.slice(1, 7).map(Number);
  const offset = match[8] === "Z" ? undefined : match[8]?.slice(1).split(":").map(Number);
  if (!Number.isSafeInteger(year) || month < 1 || month > 12 || day < 1 || day > new Date(Date.UTC(year, month, 0)).getUTCDate() || hour > 23 || minute > 59 || second > 59 || offset && (offset[0]! > 23 || offset[1]! > 59)) return undefined;
  const instant = Date.parse(value);
  return Number.isFinite(instant) ? new Date(instant).toISOString() : undefined;
};

const validHistoryQuery = (url: URL) => {
  const names = ["sourceId", "workspaceId", "tabId", "participantId", "from", "to", "limit", "cursor", "order", "playbackStatus"];
  if (names.some((name) => url.searchParams.getAll(name).length > 1)) return undefined;
  const one = (name: string) => {
    const values = url.searchParams.getAll(name);
    return values[0];
  };
  const sourceId = one("sourceId");
  const workspaceId = one("workspaceId");
  const tabId = one("tabId");
  const participantId = one("participantId");
  const from = one("from");
  const to = one("to");
  const limit = one("limit");
  const cursor = one("cursor");
  const order = one("order");
  const playbackStatus = one("playbackStatus");
  const canonicalFrom = from === undefined ? undefined : historyTimestamp(from);
  const canonicalTo = to === undefined ? undefined : historyTimestamp(to);
  if ([sourceId, workspaceId, tabId, participantId].some((value) => value !== undefined && !text(value))) return undefined;
  if ((workspaceId !== undefined || tabId !== undefined) && !sourceId || workspaceId !== undefined && tabId !== undefined) return undefined;
  if (from !== undefined && !canonicalFrom || to !== undefined && !canonicalTo || canonicalFrom !== undefined && canonicalTo !== undefined && canonicalFrom > canonicalTo) return undefined;
  if (limit !== undefined && !/^(?:[1-9][0-9]?|100)$/.test(limit) || cursor !== undefined && !(/^(0|[1-9][0-9]*)$/.test(cursor) && Number.isSafeInteger(Number(cursor))) || order !== undefined && order !== "asc" && order !== "desc" || playbackStatus !== undefined && !["unattempted", "prepared", "started", "heard", "stopped", "skipped", "blocked", "failed"].includes(playbackStatus)) return undefined;
  return { ...(sourceId ? { sourceId: sourceId as HistoryQuery["sourceId"] } : {}), ...(workspaceId ? { workspaceId: workspaceId as HistoryQuery["workspaceId"] } : {}), ...(tabId ? { tabId: tabId as HistoryQuery["tabId"] } : {}), ...(participantId ? { participantId: participantId as HistoryQuery["participantId"] } : {}), ...(canonicalFrom ? { from: canonicalFrom } : {}), ...(canonicalTo ? { to: canonicalTo } : {}), ...(limit ? { limit: Number(limit) } : {}), ...(cursor ? { cursor } : {}), ...(order ? { order } : {}), ...(playbackStatus ? { playbackStatus: playbackStatus as HistoryQuery["playbackStatus"] } : {}) } satisfies HistoryQuery;
};

const playbackSettingsQuery = (url: URL) => {
  const values = url.searchParams.getAll("sourceId");
  return values.length === 1 && text(values[0]) ? values[0] as SourceId : undefined;
};

const playbackSettingsPatch = (value: unknown): PlaybackSettingsPatch | undefined => {
  const body = record(value);
  if (!body || !exactKeys(body, [], ["master", "participant"]) || !Object.keys(body).length) return undefined;
  const master = body.master === undefined ? undefined : record(body.master);
  const participant = body.participant === undefined ? undefined : record(body.participant);
  if (master && (!exactKeys(master, [], ["muted", "volume", "speed"]) || !Object.keys(master).length || master.muted !== undefined && typeof master.muted !== "boolean" || master.volume !== undefined && (typeof master.volume !== "number" || !Number.isFinite(master.volume) || master.volume < 0 || master.volume > 1) || master.speed !== undefined && (typeof master.speed !== "number" || !Number.isFinite(master.speed) || master.speed < 0.5 || master.speed > 2))) return undefined;
  if (participant && (!exactKeys(participant, ["sourceId", "participantId"], ["muted", "volume"]) || !text(participant.sourceId) || !text(participant.participantId) || participant.muted === undefined && participant.volume === undefined || participant.muted !== undefined && typeof participant.muted !== "boolean" || participant.volume !== undefined && (typeof participant.volume !== "number" || !Number.isFinite(participant.volume) || participant.volume < 0 || participant.volume > 1))) return undefined;
  return { ...(master ? { master: master as PlaybackSettingsPatch["master"] } : {}), ...(participant ? { participant: { sourceId: participant.sourceId as SourceId, participantId: participant.participantId as ParticipantId, ...(participant.muted === undefined ? {} : { muted: participant.muted as boolean }), ...(participant.volume === undefined ? {} : { volume: participant.volume as number }) } } : {}) };
};

const playbackCandidatesQuery = (url: URL) => {
  const names = ["limit", "cursor", "order"];
  if (names.some((name) => url.searchParams.getAll(name).length > 1)) return undefined;
  const limit = url.searchParams.get("limit") ?? undefined;
  const cursor = url.searchParams.get("cursor") ?? undefined;
  const order = url.searchParams.get("order") ?? undefined;
  if (limit !== undefined && !/^(?:[1-9][0-9]?|100)$/.test(limit) || cursor !== undefined && !(/^(0|[1-9][0-9]*)$/.test(cursor) && Number.isSafeInteger(Number(cursor))) || order !== undefined && order !== "asc" && order !== "desc") return undefined;
  return { ...(limit ? { limit: Number(limit) } : {}), ...(cursor ? { cursor } : {}), ...(order ? { order: order as "asc" | "desc" } : {}) };
};

const playbackPrepare = (value: unknown) => {
  const body = record(value);
  return body && exactKeys(body, ["sourceId", "participantId", "scopeGeneration"]) && text(body.sourceId) && text(body.participantId) && nonnegativeInteger(body.scopeGeneration)
    ? { sourceId: body.sourceId as SourceId, participantId: body.participantId as ParticipantId, scopeGeneration: body.scopeGeneration }
    : undefined;
};

const playbackAttempt = (value: unknown) => {
  const body = record(value);
  return body && exactKeys(body, ["itemId", "sourceId", "participantId", "scopeGeneration", "intent"]) && text(body.itemId, 1_024) && text(body.sourceId) && text(body.participantId) && nonnegativeInteger(body.scopeGeneration) && (body.intent === "automatic" || body.intent === "replay")
    ? { itemId: body.itemId as string, sourceId: body.sourceId as SourceId, participantId: body.participantId as ParticipantId, scopeGeneration: body.scopeGeneration, intent: body.intent as "automatic" | "replay" }
    : undefined;
};

const playbackAttemptStatus = (value: unknown) => {
  const body = record(value);
  return body && exactKeys(body, ["state", "authorizationGeneration"]) && typeof body.state === "string" && ["started", "heard", "stopped", "skipped", "blocked", "failed"].includes(body.state) && nonnegativeInteger(body.authorizationGeneration)
    ? { state: body.state as "started" | "heard" | "stopped" | "skipped" | "blocked" | "failed", authorizationGeneration: body.authorizationGeneration }
    : undefined;
};

const stateQuery = (url: URL): StateQuery | undefined => {
  const sourceIds = url.searchParams.getAll("sourceId");
  if (sourceIds.length > 1 || sourceIds[0] !== undefined && !text(sourceIds[0])) return undefined;
  return sourceIds[0] ? { sourceId: sourceIds[0] as StateQuery["sourceId"] } : {};
};

type StorageResult<A> = { ok: true; value: A } | { ok: false; cancelled?: boolean };

interface StorageDiagnostic { operation: string; trace?: ReturnType<typeof traceContext>; sourceId?: string; participantId?: string; jobId?: string; operationId?: string; }

const ignoredPromise = (run: () => Promise<unknown>) => Effect.uninterruptible(Effect.ignore(Effect.tryPromise({ try: run, catch: () => undefined })));

const runStorage = <A>(effect: Effect.Effect<A, unknown>, diagnostic: StorageDiagnostic): Effect.Effect<StorageResult<A>> => {
  const trace = { ...diagnostic.trace, ...(diagnostic.sourceId ? { sourceId: diagnostic.sourceId } : {}), ...(diagnostic.participantId ? { participantId: diagnostic.participantId } : {}), ...(diagnostic.jobId ? { jobId: diagnostic.jobId } : {}) };
  const span = tracer.start(diagnostic.operation, trace);
  return effect.pipe(Effect.matchCauseEffect({
    onSuccess: (value) => ignoredPromise(() => span.end("succeeded")).pipe(Effect.as({ ok: true as const, value })),
    onFailure: (cause) => Cause.hasInterruptsOnly(cause)
      ? ignoredPromise(() => span.end("cancelled")).pipe(Effect.as({ ok: false as const, cancelled: true }))
      : Effect.uninterruptible(Effect.gen(function*() {
        yield* ignoredPromise(() => span.end("failed"));
        yield* ignoredPromise(() => logger.log("error", { operation: diagnostic.operation, message: "Durable storage request failed", outcome: "failed", sourceId: diagnostic.sourceId, participantId: diagnostic.participantId, jobId: diagnostic.jobId, traceId: span.context.traceId, spanId: span.context.spanId, metadata: diagnostic.operationId ? { operationId: diagnostic.operationId } : undefined }));
        yield* ignoredPromise(() => errors.report({ service: "app", operation: diagnostic.operation, category: "storage", error: new Error("Durable storage request failed"), trace: span.context, context: diagnostic.operationId ? { operationId: diagnostic.operationId } : undefined }));
        return { ok: false as const };
      })),
  }));
};

const storageOrError = <A>(response: import("node:http").ServerResponse, effect: Effect.Effect<A, unknown>, diagnostic: StorageDiagnostic): Effect.Effect<A | undefined> => Effect.gen(function*() {
  const result = yield* runStorage(effect, diagnostic);
  if (!result.ok) { yield* collectorRespond(response, 500, "storage_failed"); return undefined; }
  return result.value;
});

const handleCollector = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, path: string, url: URL): Effect.Effect<void> => Effect.gen(function*() {
  if (!collectorToken) return yield* collectorRespond(response, 503, "collector_unavailable");
  if (!matchesCollectorToken(request.headers.authorization)) return yield* collectorRespond(response, 401, "unauthorized");
  if (path === "/api/collector/batches" && request.method === "POST") {
    const body = yield* readCollectorJson(request);
    if (body.kind === "too_large") return yield* collectorRespond(response, 413, "request_too_large");
    const identity = body.kind === "json" ? collectorIdentity(body.value) : undefined;
    if (!identity) return yield* collectorRespond(response, 400, "invalid_request");
    const prior = yield* storageOrError(response, storage.lookupBatchReceipt({ sourceId: identity.sourceId as IngestBatch["sourceId"], sourceEpoch: identity.sourceEpoch, batchId: identity.batchId, payload: identity.body }), { operation: "server.collector.receipt_lookup", trace: traceContext(request.headers.traceparent), sourceId: identity.sourceId, operationId: identity.batchId });
    if (!prior) return;
    if (prior.kind === "conflict") return yield* collectorRespond(response, 409, prior.code);
    if (prior.kind === "duplicate") return yield* collectorJson(response, 200, prior.receipt);
    if (!isCollectorBatchShape(identity.body)) return yield* collectorRespond(response, 400, "invalid_request");
    if (collectorUtf8Bytes(encodeCollectorJson(identity.body.topology)) > COLLECTOR_WIRE_LIMITS.topologyBytes) return yield* collectorRespond(response, 413, "request_too_large");
    const batch = identity.body;
    const outcome = yield* storageOrError(response, storage.ingestBatch(batch), { operation: "server.collector.ingest", trace: traceContext(request.headers.traceparent), sourceId: batch.sourceId, operationId: batch.batchId });
    if (!outcome) return;
    if (outcome.kind === "conflict") return yield* collectorRespond(response, 409, outcome.code);
    return yield* collectorJson(response, outcome.kind === "accepted" ? 201 : 200, outcome.receipt);
  }
  if (path === "/api/collector/jobs/claim" && request.method === "POST") {
    const body = yield* readCollectorJson(request);
    if (body.kind === "too_large") return yield* collectorRespond(response, 413, "request_too_large");
    if (body.kind !== "json" || !isWorkerClaim(body.value)) return yield* collectorRespond(response, 400, "invalid_request");
    const outcome = yield* storageOrError(response, storage.claimJob((body.value as { workerId: string }).workerId), { operation: "server.collector.claim" });
    if (!outcome) return;
    return outcome.kind === "empty" ? yield* Effect.sync(() => response.writeHead(204).end()) : yield* collectorJson(response, 200, { ...outcome.job, jobId: outcome.job.id });
  }
  const resultMatch = path.match(/^\/api\/collector\/jobs\/([A-Za-z0-9:_-]{1,256})\/result$/);
  if (resultMatch && request.method === "POST") {
    const body = yield* readCollectorJson(request);
    const completion = body.kind === "json" ? validatedCompletion(body.value) : undefined;
    if (body.kind === "too_large") return yield* collectorRespond(response, 413, "request_too_large");
    if (!completion) return yield* collectorRespond(response, 400, "invalid_request");
    const outcome = yield* storageOrError(response, storage.completeJob(resultMatch[1], completion), { operation: "server.collector.complete", jobId: resultMatch[1], operationId: resultMatch[1] });
    if (!outcome) return;
    if (outcome.kind === "missing") return yield* collectorRespond(response, 404, "not_found");
    if (outcome.kind === "conflict") return yield* collectorRespond(response, 409, outcome.code);
    return yield* collectorJson(response, outcome.kind === "accepted" ? 201 : 200, outcome.receipt);
  }
  if (path === "/api/collector/config" && request.method === "GET") {
    const sourceId = url.searchParams.get("sourceId"); if (!text(sourceId)) return yield* collectorRespond(response, 400, "invalid_request");
    const state = yield* storageOrError(response, storage.getState(), { operation: "server.collector.config", sourceId: sourceId! }); if (!state) return;
    const source = state.sources.find((item) => item.sourceId === sourceId); if (!source) return yield* collectorRespond(response, 404, "not_found");
    return yield* collectorJson(response, 200, { sourceId, listeningScope: state.scope?.sourceId === sourceId ? state.scope : null, listeningGeneration: source.listeningGeneration, freshness: { observedAt: source.observedAt, stale: source.stale, topologySequence: source.topologySequence }, worker: { leaseDurationMs, maxAttempts } });
  }
  return yield* collectorRespond(response, 404, "not_found");
});

const handleListening = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  if (!sameOrigin(request)) return yield* collectorRespond(response, 403, "origin_forbidden");
  const body = yield* readCollectorJson(request);
  if (body.kind === "too_large") return yield* collectorRespond(response, 413, "request_too_large");
  if (body.kind !== "json" || !isListeningSelection(body.value)) return yield* collectorRespond(response, 400, "invalid_request");
  const outcome = yield* storageOrError(response, storage.setListeningScope(body.value as ListeningSelection), { operation: "server.listening", sourceId: (body.value as { sourceId?: string } | null)?.sourceId });
  if (!outcome) return;
  return outcome.kind === "unknown" ? yield* collectorRespond(response, 404, outcome.code) : yield* respond(response, 200, { scope: outcome.scope, generation: outcome.generation });
});

const handleCatchUp = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  if (!sameOrigin(request)) return yield* collectorRespond(response, 403, "origin_forbidden");
  const body = yield* readCollectorJson(request);
  if (body.kind === "too_large") return yield* collectorRespond(response, 413, "request_too_large");
  if (body.kind !== "json" || !isCatchUpRequest(body.value)) return yield* collectorRespond(response, 400, "invalid_request");
  const outcome = yield* storageOrError(response, storage.requestCatchUp(body.value), { operation: "server.catch_up", sourceId: body.value.sourceId });
  if (!outcome) return;
  if (outcome.kind !== "accepted" && outcome.kind !== "duplicate") return yield* collectorRespond(response, 409, outcome.kind);
  return yield* respond(response, outcome.kind === "accepted" && outcome.recap.status === "pending" ? 202 : 200, outcome.recap);
});

const handleEvents = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  const lastEventId = Array.isArray(request.headers["last-event-id"]) ? request.headers["last-event-id"][0] : request.headers["last-event-id"];
  if (!isEventSequence(lastEventId)) return yield* collectorRespond(response, 400, "invalid_request");
  let cursor = Number(lastEventId ?? "0");
  const initial = yield* runStorage(storage.getEventsSince(cursor), { operation: "server.events" });
  if (!initial.ok) return yield* collectorRespond(response, 500, "storage_failed");
  const first = initial.value;
  if (first.kind === "expired") return yield* collectorRespond(response, 410, "events_expired");
  if (cursor > first.latestSequence) return yield* collectorRespond(response, 400, "invalid_request");
  yield* Effect.sync(() => response.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-store", Connection: "keep-alive" }));
  const writable = () => !response.writableEnded && !response.destroyed;
  const storageFailure = () => { if (writable()) { response.write("event: error\ndata: {\"code\":\"storage_failed\"}\n\n"); response.end(); } };
  let page = first;
  while (writable()) {
    for (const event of page.events) {
      if (!writable()) return;
      yield* Effect.sync(() => response.write(`id: ${event.sequence}\nevent: ${event.kind}\ndata: ${JSON.stringify(event.value)}\n\n`));
      cursor = event.sequence;
    }
    if (cursor < page.latestSequence) {
      if (!writable()) return;
      const next = yield* runStorage(storage.getEventsSince(cursor), { operation: "server.events" });
      if (!next.ok) { if (!next.cancelled) yield* Effect.sync(storageFailure); return; }
      if (next.value.kind === "expired") return;
      page = next.value; continue;
    }
    yield* Effect.sleep(100);
    if (!writable()) return;
    const next = yield* runStorage(storage.getEventsSince(cursor), { operation: "server.events" });
    if (!next.ok) { if (!next.cancelled) yield* Effect.sync(storageFailure); return; }
    if (next.value.kind === "expired") return;
    page = next.value;
  }
});

const handlePlaybackCandidates = (storage: Storage, url: URL, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  const query = playbackCandidatesQuery(url);
  if (!query) return yield* collectorRespond(response, 400, "invalid_request");
  const page = yield* storageOrError(response, storage.getPlaybackCandidates(query), { operation: "server.playback.candidates" });
  if (!page) return;
  const candidates = page.candidates.map((candidate) => ({ itemId: candidate.itemId, kind: candidate.kind, jobId: candidate.jobId, ...(candidate.announcementId ? { announcementId: candidate.announcementId } : {}), sourceId: candidate.sourceId, ...(candidate.workspaceId ? { workspaceId: candidate.workspaceId } : {}), ...(candidate.tabId ? { tabId: candidate.tabId } : {}), participantId: candidate.participantId, originGeneration: candidate.originGeneration, media: candidate.media, playback: candidate.playback, createdAt: candidate.createdAt }));
  yield* respond(response, 200, { scope: page.scope, candidates, ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}) });
});

const handlePlaybackSettings = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, url: URL): Effect.Effect<void> => Effect.gen(function*() {
  if (request.method === "GET") {
    const sourceId = playbackSettingsQuery(url);
    if (!sourceId) return yield* collectorRespond(response, 400, "invalid_request");
    const settings = yield* storageOrError(response, storage.getPlaybackSettings(sourceId), { operation: "server.playback.settings", sourceId });
    if (!settings) return yield* collectorRespond(response, 404, "not_found");
    return yield* respond(response, 200, settings);
  }
  if (request.method !== "PUT" || !sameOrigin(request)) return yield* collectorRespond(response, request.method === "PUT" ? 403 : 405, request.method === "PUT" ? "origin_forbidden" : "method_not_allowed");
  const body = yield* readCollectorJson(request);
  const patch = body.kind === "json" ? playbackSettingsPatch(body.value) : undefined;
  if (!patch || body.kind === "too_large") return yield* collectorRespond(response, body.kind === "too_large" ? 413 : 400, body.kind === "too_large" ? "request_too_large" : "invalid_request");
  const settings = yield* storageOrError(response, storage.updatePlaybackSettings(patch), { operation: "server.playback.settings.update", sourceId: patch.participant?.sourceId });
  if (!settings) return yield* collectorRespond(response, 404, "not_found");
  yield* respond(response, 200, settings);
});

const handlePlaybackPrepare = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, itemId: string): Effect.Effect<void> => Effect.gen(function*() {
  if (!sameOrigin(request)) return yield* collectorRespond(response, 403, "origin_forbidden");
  const body = yield* readCollectorJson(request);
  const prepared = body.kind === "json" ? playbackPrepare(body.value) : undefined;
  if (!prepared || body.kind === "too_large") return yield* collectorRespond(response, body.kind === "too_large" ? 413 : 400, body.kind === "too_large" ? "request_too_large" : "invalid_request");
  const outcome = yield* storageOrError(response, storage.preparePlaybackMedia({ ...prepared, itemId }), { operation: "server.playback.prepare", sourceId: prepared.sourceId, participantId: prepared.participantId });
  if (!outcome) return;
  if (outcome.kind === "unknown") return yield* respond(response, 404, { code: "not_found", itemId });
  if (outcome.kind === "stale") return yield* collectorRespond(response, 409, "stale_generation");
  if (outcome.kind === "pending") return yield* respond(response, 202, { media: { state: "preparing" } });
  if (outcome.kind === "ready") return yield* respond(response, 200, { media: outcome.candidate.media });
  if (outcome.kind !== "prepare") return yield* collectorRespond(response, 404, "not_found");
  return yield* Effect.tryPromise({
    try: (signal) => synthesize(outcome.text, outcome.jobId, { apiKey: Bun.env.ELEVENLABS_API_KEY, voiceId: Bun.env.ELEVENLABS_VOICE_ID, dataDir, logger, tracer, reporter: errors, jobId: outcome.jobId }, fetch, signal),
    catch: (cause) => cause,
  }).pipe(Effect.onInterrupt(() => Effect.uninterruptible(storage.abandonPlaybackMedia(itemId, outcome.token).pipe(Effect.ignore))), Effect.matchCauseEffect({
    onSuccess: (clip) => Effect.gen(function*() {
      const mediaId = clip.path.split("/").at(-1)?.replace(/\.mp3$/, "");
      if (!mediaId || !/^[a-f0-9]{64}$/.test(mediaId)) return yield* collectorRespond(response, 500, "media_failed");
      const completed = yield* storageOrError(response, storage.completePlaybackMedia(itemId, outcome.token, mediaId), { operation: "server.playback.prepare.complete", sourceId: prepared.sourceId, participantId: prepared.participantId, jobId: outcome.jobId });
      if (!completed) return;
      return completed.kind === "ready" ? yield* respond(response, 200, { media: completed.candidate.media }) : yield* collectorRespond(response, 409, "stale_generation");
    }),
    onFailure: (cause) => Cause.hasInterruptsOnly(cause) ? storageOrError(response, storage.abandonPlaybackMedia(itemId, outcome.token), { operation: "server.playback.prepare.cancelled", sourceId: prepared.sourceId, participantId: prepared.participantId, jobId: outcome.jobId }).pipe(Effect.asVoid) : Effect.gen(function*() {
      const completed = yield* storageOrError(response, storage.completePlaybackMedia(itemId, outcome.token, undefined, "provider_failed"), { operation: "server.playback.prepare.failed", sourceId: prepared.sourceId, participantId: prepared.participantId, jobId: outcome.jobId });
      if (completed?.kind === "failed") yield* collectorRespond(response, 503, "media_unavailable");
    }),
  }));
});

const handlePlaybackAttempt = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, attemptId: string): Effect.Effect<void> => Effect.gen(function*() {
  if (!sameOrigin(request)) return yield* collectorRespond(response, 403, "origin_forbidden");
  const body = yield* readCollectorJson(request);
  const attempt = body.kind === "json" ? playbackAttempt(body.value) : undefined;
  if (!attempt || body.kind === "too_large") return yield* collectorRespond(response, body.kind === "too_large" ? 413 : 400, body.kind === "too_large" ? "request_too_large" : "invalid_request");
  const outcome = yield* storageOrError(response, storage.createPlaybackAttempt({ ...attempt, attemptId }), { operation: "server.playback.attempt", sourceId: attempt.sourceId, participantId: attempt.participantId });
  if (!outcome) return;
  if (outcome.kind === "unknown") return yield* collectorRespond(response, 404, "not_found");
  if (outcome.kind === "conflict") return yield* collectorRespond(response, 409, "attempt_conflict");
  if (outcome.kind === "stale") return yield* collectorRespond(response, 409, "stale_generation");
  if (outcome.kind !== "created" && outcome.kind !== "duplicate") return;
  yield* respond(response, outcome.kind === "created" ? 201 : 200, { attemptId: outcome.attempt.attemptId, originGeneration: outcome.attempt.originGeneration, authorizationGeneration: outcome.attempt.authorizationGeneration, state: outcome.attempt.state, mediaUrl: `/api/playback/attempts/${encodeURIComponent(outcome.attempt.attemptId)}/media` });
});

const handlePlaybackAttemptStatus = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, attemptId: string): Effect.Effect<void> => Effect.gen(function*() {
  if (!sameOrigin(request)) return yield* collectorRespond(response, 403, "origin_forbidden");
  const body = yield* readCollectorJson(request);
  const update = body.kind === "json" ? playbackAttemptStatus(body.value) : undefined;
  if (!update || body.kind === "too_large") return yield* collectorRespond(response, body.kind === "too_large" ? 413 : 400, body.kind === "too_large" ? "request_too_large" : "invalid_request");
  const outcome = yield* storageOrError(response, storage.updatePlaybackAttempt(attemptId, update.state, update.authorizationGeneration), { operation: "server.playback.attempt.status" });
  if (!outcome) return;
  if (outcome.kind === "unknown") return yield* collectorRespond(response, 404, "not_found");
  if (outcome.kind === "stale") return yield* collectorRespond(response, 409, "stale_generation");
  if (outcome.kind === "invalid_transition") return yield* collectorRespond(response, 409, "invalid_transition");
  yield* respond(response, 200, { attemptId, authorizationGeneration: update.authorizationGeneration, state: update.state });
});

const handlePlaybackAttemptMedia = (storage: Storage, response: import("node:http").ServerResponse, attemptId: string): Effect.Effect<void> => Effect.gen(function*() {
  const media = yield* storageOrError(response, storage.getPlaybackAttemptMedia(attemptId), { operation: "server.playback.media" });
  if (!media) return yield* collectorRespond(response, 404, "not_found");
  const file = Bun.file(`${dataDir}/audio/${media.mediaId}.mp3`);
  const exists = yield* Effect.tryPromise({ try: () => file.exists(), catch: (cause) => cause }).pipe(Effect.match({ onFailure: () => false, onSuccess: (value) => value }));
  if (!exists) return yield* collectorRespond(response, 404, "not_found");
  const bytes = yield* Effect.tryPromise({ try: () => file.arrayBuffer(), catch: (cause) => cause }).pipe(Effect.match({ onFailure: () => undefined, onSuccess: (value) => value }));
  if (!bytes) return yield* collectorRespond(response, 404, "not_found");
  yield* Effect.sync(() => { response.writeHead(200, { "Content-Type": "audio/mpeg", "Cache-Control": "no-store" }); response.end(bytes); });
});

const handleHistory = (storage: Storage, url: URL, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  const query = validHistoryQuery(url);
  if (!query) return yield* collectorRespond(response, 400, "invalid_request");
  const outcome = yield* storageOrError(response, storage.getHistory(query), { operation: "server.history", sourceId: query.sourceId, participantId: query.participantId });
  if (outcome) yield* respond(response, 200, outcome);
});

const handleState = (storage: Storage, url: URL, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  const query = stateQuery(url);
  if (!query) return yield* collectorRespond(response, 400, "invalid_request");
  const state = yield* storageOrError(response, storage.getState(query), { operation: "server.state", sourceId: query.sourceId });
  if (!state) return;
  if (query.sourceId && !state.sources.some((source) => source.sourceId === query.sourceId)) return yield* collectorRespond(response, 404, "not_found");
  yield* respond(response, 200, state);
});

const handleClientErrors = (request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse): Effect.Effect<void> => Effect.gen(function*() {
  const origin = `http://${request.headers.host}`;
  if (request.headers.origin !== origin) return yield* Effect.sync(() => { response.writeHead(403); response.end("Origin forbidden"); });
  if (request.headers["content-type"]?.split(";", 1)[0].trim().toLowerCase() !== "application/json") return yield* Effect.sync(() => { response.writeHead(415); response.end("JSON required"); });
  if (Number(request.headers["content-length"]) > clientErrorLimit) return yield* Effect.sync(() => { response.writeHead(413); response.end("Request too large"); });
  const body = yield* readJson(request, clientErrorLimit);
  if (body.kind === "too_large") return yield* Effect.sync(() => { response.writeHead(413, { Connection: "close" }); response.end("Request too large"); });
  const value = body.kind === "json" && record(body.value);
  if (!value) return yield* Effect.sync(() => { response.writeHead(400); response.end("Invalid error report"); });
  const string = (key: string, limit: number, required = false) => typeof value[key] === "string" && value[key].length > 0 && value[key].length <= limit ? value[key] : required ? undefined : value[key] === undefined ? undefined : null;
  const operation = string("operation", 120, true); const category = string("category", 80, true); const message = string("message", 1_024, true); const stack = string("stack", 2_048); const sourceId = string("sourceId", 128); const participantId = string("participantId", 128); const jobId = string("jobId", 128); const traceId = string("traceId", 32); const spanId = string("spanId", 16);
  if (!operation || !category || !message || [stack, sourceId, participantId, jobId, traceId, spanId].includes(null) || Boolean(traceId) !== Boolean(spanId) || (traceId && !/^[a-f0-9]{32}$/i.test(traceId)) || (spanId && !/^[a-f0-9]{16}$/i.test(spanId)) || (value.context !== undefined && (!value.context || typeof value.context !== "object" || Array.isArray(value.context)))) return yield* Effect.sync(() => { response.writeHead(400); response.end("Invalid error report"); });
  const reportId = yield* Effect.uninterruptible(Effect.tryPromise({
    try: () => errors.report({ service: "app", operation, category, error: new Error(message), stack: stack ?? undefined, trace: { ...(traceId && spanId ? { traceId, spanId } : {}), ...(sourceId ? { sourceId } : {}), ...(participantId ? { participantId } : {}), ...(jobId ? { jobId } : {}) }, context: value.context as Record<string, unknown> | undefined }),
    catch: (cause) => cause,
  })).pipe(Effect.matchEffect({ onFailure: () => Effect.succeed(undefined), onSuccess: (value) => Effect.succeed(value) }));
  if (!reportId) return yield* Effect.sync(() => { response.writeHead(500); response.end("Unavailable"); });
  yield* respond(response, 202, { reportId });
});

const apiRequest = (storage: Storage, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, path: string, url: URL): Effect.Effect<void> | undefined => {
  if (path === "/api/health") return respond(response, 200, { status: "ok" });
  if (path.startsWith("/api/collector/")) return handleCollector(storage, request, response, path, url);
  if (path === "/api/listening" && request.method === "PUT") return handleListening(storage, request, response);
  if (path === "/api/catch-up" && request.method === "POST") return handleCatchUp(storage, request, response);
  if (path === "/api/state" && request.method === "GET") return handleState(storage, url, response);
  if (path === "/api/events" && request.method === "GET") return handleEvents(storage, request, response);
  if (path === "/api/playback/candidates" && request.method === "GET") return handlePlaybackCandidates(storage, url, response);
  if (path === "/api/playback/settings" && (request.method === "GET" || request.method === "PUT")) return handlePlaybackSettings(storage, request, response, url);
  const prepareMatch = path.match(/^\/api\/playback\/items\/([^/]{1,1024})\/prepare$/);
  if (prepareMatch && request.method === "POST") {
    try { return handlePlaybackPrepare(storage, request, response, decodeURIComponent(prepareMatch[1]!)); } catch { return collectorRespond(response, 400, "invalid_request"); }
  }
  const attemptStatusMatch = path.match(/^\/api\/playback\/attempts\/([^/]{1,128})\/status$/);
  if (attemptStatusMatch && request.method === "PUT") return handlePlaybackAttemptStatus(storage, request, response, attemptStatusMatch[1]!);
  const attemptMediaMatch = path.match(/^\/api\/playback\/attempts\/([^/]{1,128})\/media$/);
  if (attemptMediaMatch && request.method === "GET") return handlePlaybackAttemptMedia(storage, response, attemptMediaMatch[1]!);
  const attemptMatch = path.match(/^\/api\/playback\/attempts\/([^/]{1,128})$/);
  if (attemptMatch && request.method === "PUT") return handlePlaybackAttempt(storage, request, response, attemptMatch[1]!);
  if (path === "/api/history" && request.method === "GET") return handleHistory(storage, url, response);
  if (path === "/api/client-errors" && request.method === "POST") return handleClientErrors(request, response);
  return undefined;
};

const requestProgram = (route: Effect.Effect<void>, request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse) => Effect.gen(function*() {
  const disconnected = yield* Deferred.make<void>();
  const disconnect = () => { Deferred.doneUnsafe(disconnected, Effect.void); };
  return yield* Effect.acquireUseRelease(
    Effect.sync(() => {
      request.once("aborted", disconnect);
      request.socket.once("close", disconnect);
      response.once("close", disconnect);
      if (request.destroyed || response.destroyed) disconnect();
    }),
    () => Effect.raceFirst(route, Deferred.await(disconnected).pipe(Effect.andThen(Effect.interrupt))),
    (_, exit) => Effect.sync(() => {
      request.off("aborted", disconnect);
      request.socket.off("close", disconnect);
      response.off("close", disconnect);
      if (Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) && !response.writableEnded) response.destroy();
    }),
  );
});

const handleLegacy = async (request: import("node:http").IncomingMessage, response: import("node:http").ServerResponse, path: string, url: URL) => {
  if (path === "/api/hello") return json(response, 200, { message: "Hello from Speak Now!" });
  if (path === "/api/config") return json(response, 200, {
      liveSpeechConfigured: Boolean(Bun.env.ELEVENLABS_API_KEY && Bun.env.ELEVENLABS_VOICE_ID),
      diagnostics: logger.getStatus(),
      traces: tracer.getStatus(),
      errors: errors.getStatus(),
    });
  if (path === "/assets/client.js" || path === "/") {
    const file = Bun.file(path === "/" ? "public/index.html" : "public/assets/client.js");
    response.writeHead(200, { "Content-Type": path === "/" ? "text/html; charset=utf-8" : "text/javascript; charset=utf-8" });
    return response.end(await file.arrayBuffer());
  }
  if (path === "/assets/client.css") {
    const file = Bun.file("public/assets/client.css");
    if (!(await file.exists())) { response.writeHead(404); return response.end("Not found"); }
    response.writeHead(200, { "Content-Type": "text/css; charset=utf-8" });
    return response.end(await file.arrayBuffer());
  }
  if (path === "/api/prototype/latest") {
    const span = tracer.start("media.read.latest", traceContext(request.headers.traceparent ?? url.searchParams.get("traceparent") ?? undefined));
    try { const file = Bun.file(`${dataDir}/latest-announcement.json`); if (!(await file.exists())) { await span.end("missing"); response.writeHead(404); return response.end("No announcement"); } const announcement = await file.json() as { announcementId?: string }; await span.end("succeeded", { announcementId: announcement.announcementId }); return json(response, 200, announcement); } catch { await span.end("failed"); response.writeHead(500); return response.end("No announcement"); }
  }
  const audio = path.match(/^\/api\/prototype\/audio\/([a-f0-9]{64})$/);
  if (audio) {
    const span = tracer.start("media.read", { ...traceContext(request.headers.traceparent ?? url.searchParams.get("traceparent") ?? undefined), clipId: audio[1] });
    try { const file = Bun.file(`${dataDir}/audio/${audio[1]}.mp3`); const exists = await file.exists(); if (!exists) { await span.end("missing"); void logger.log("warn", { operation: "server.media", message: "Audio clip unavailable", outcome: "missing", metadata: { clip: audio[1] } }); response.writeHead(404); return response.end("Audio unavailable"); } const bytes = await file.arrayBuffer(); await span.end("succeeded"); void logger.log("info", { operation: "server.media", message: "Served audio clip", outcome: "succeeded", metadata: { clip: audio[1] } }); response.writeHead(200, { "Content-Type": "audio/mpeg", "Cache-Control": "no-store" }); return response.end(bytes); } catch { await span.end("failed"); response.writeHead(500); return response.end("Audio unavailable"); }
  }
  response.writeHead(404); response.end("Not found");
};

const createApp = (storage: Storage, dispatch: <A>(effect: Effect.Effect<A>) => unknown, admission: { open: boolean }) => createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`); const path = url.pathname;
  if (!admission.open) { response.destroy(); return; }
  const route = apiRequest(storage, request, response, path, url);
  const program = route ?? Effect.uninterruptible(Effect.tryPromise({ try: () => handleLegacy(request, response, path, url), catch: (cause) => cause })).pipe(Effect.matchEffect({ onFailure: () => Effect.sync(() => { if (!response.headersSent) { response.writeHead(500); response.end("Unavailable"); } }), onSuccess: () => Effect.void }));
  dispatch(requestProgram(program, request, response));
});

const closeRequestServer = (server: ReturnType<typeof createServer>, requests: FiberSet.FiberSet<void, never>, admission: { open: boolean }) => Effect.gen(function*() {
  const closed = yield* Deferred.make<void, Error>();
  yield* Effect.sync(() => {
    admission.open = false;
    if (!server.listening) { Deferred.doneUnsafe(closed, Effect.void); return; }
    try {
      server.close((error) => { Deferred.doneUnsafe(closed, error ? Effect.fail(error) : Effect.void); });
    } catch (cause) {
      Deferred.doneUnsafe(closed, Effect.fail(cause instanceof Error ? cause : new Error("Server close failed")));
    }
  });
  yield* FiberSet.clear(requests);
  yield* Effect.sync(() => server.closeAllConnections?.());
  yield* Deferred.await(closed);
});

const listenServer = (server: ReturnType<typeof createServer>) => Effect.callback<void, Error>((resume, signal) => {
  let settled = false;
  const cleanup = () => {
    server.off("listening", onListening);
    server.off("error", onError);
    signal.removeEventListener("abort", onAbort);
  };
  const settle = (effect: Effect.Effect<void, Error>) => {
    if (settled) return;
    settled = true;
    cleanup();
    resume(effect);
  };
  const onListening = () => settle(Effect.void);
  const onError = (cause: Error) => settle(Effect.fail(cause));
  const onAbort = () => settle(Effect.fail(new Error("Server startup cancelled")));
  server.once("listening", onListening);
  server.once("error", onError);
  signal.addEventListener("abort", onAbort, { once: true });
  try { server.listen(port); } catch (cause) { settle(Effect.fail(cause instanceof Error ? cause : new Error("Server listen failed"))); }
  return Effect.sync(() => {
    cleanup();
    if (server.listening) server.close();
  });
});

const serverProgram = Effect.scoped(Effect.gen(function*() {
  const storage = yield* openStorage(dataDir);
  yield* runStorage(storage.prune(), { operation: "server.prune" });
  yield* Effect.forkScoped(Effect.forever(Effect.sleep(60 * 60 * 1_000).pipe(Effect.andThen(Effect.suspend(() => runStorage(storage.prune(), { operation: "server.prune" }))))));
  const requests = yield* FiberSet.make<void, never>();
  const dispatch = yield* FiberSet.runtime(requests)<never>();
  const admission = { open: true };
  const server = yield* Effect.acquireRelease(
    Effect.try({ try: () => createApp(storage, dispatch, admission), catch: (cause) => cause }),
    (resource) => Effect.ignore(closeRequestServer(resource, requests, admission)),
  );
  yield* listenServer(server);
  yield* Effect.sync(() => console.log(`Speak Now is running at http://localhost:${port}`));
  yield* Effect.never;
}));

const terminalServerProgram = serverProgram.pipe(Effect.matchEffect({
  onFailure: (error) => Effect.tryPromise({
    try: () => errors.report({ service: "app", operation: "server.start", category: "storage", error }),
    catch: (cause) => cause,
  }).pipe(Effect.andThen(Effect.tryPromise({ try: () => errors.flush(), catch: (cause) => cause })), Effect.matchEffect({
    onFailure: () => Effect.sync(() => { process.exitCode = 1; }),
    onSuccess: () => Effect.sync(() => { process.exitCode = 1; }),
  })),
  onSuccess: () => Effect.void,
}));

const main = Effect.runFork(terminalServerProgram);
const shutdown = () => { main.interruptUnsafe(); };
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
main.addObserver(() => {
  process.off("SIGINT", shutdown);
  process.off("SIGTERM", shutdown);
});
