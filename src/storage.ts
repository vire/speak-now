import { Database } from "bun:sqlite";
import * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";
import { createHash, randomUUID } from "node:crypto";
import { mkdir, readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { migrations } from "./migrations";
import type { Activity, ListeningScope, ParticipantId, SourceId, Topology, TraceContext, WorkspaceId, TabId } from "./shared";

const evidenceRetentionMs = 24 * 60 * 60 * 1_000;
const historyRetentionMs = 30 * 24 * 60 * 60 * 1_000;
export const leaseDurationMs = 60 * 1_000;
export const maxAttempts = 2;
const staleAfterMs = 60 * 1_000;
const eventReplayLimit = 100;

export class StorageError extends Error {
  readonly _tag = "StorageError";
  constructor(readonly operation: string, cause?: unknown) {
    super(`Storage ${operation} failed`);
    this.cause = cause;
  }
}

export type StorageEffect<A> = Effect.Effect<A, StorageError>;
export type ConflictCode = "source_epoch" | "batch_payload" | "topology_sequence" | "cursor_cas" | "event_payload" | "lease_token" | "completion_payload";

export interface CursorAdvance { participantId: ParticipantId; previous: string | null; next: string; }
export interface IngestBatch {
  sourceId: SourceId;
  sourceEpoch: string;
  batchId: string;
  listeningGeneration: number;
  topologySequence: number;
  topology: Topology;
  baselineReady: ParticipantId[];
  cursors: CursorAdvance[];
  activities: Activity[];
}
export interface BatchReceipt { batchId: string; acceptedEventIds: string[]; cursors: Array<{ participantId: ParticipantId; cursor: string }>; topologySequence: number; }
export type IngestOutcome = { kind: "accepted" | "duplicate"; receipt: BatchReceipt } | { kind: "conflict"; code: ConflictCode };
export interface BatchReceiptQuery { sourceId: SourceId; sourceEpoch: string; batchId: string; payload: Record<string, unknown>; }
export type BatchReceiptOutcome = { kind: "missing" } | { kind: "duplicate"; receipt: BatchReceipt } | { kind: "conflict"; code: "source_epoch" | "batch_payload" };

export interface ClaimedJob {
  id: string;
  attempt: number;
  leaseToken: string;
  leaseExpiresAt: string;
  leaseDurationMs: number;
  sourceId: SourceId;
  participantId: ParticipantId;
  scopeGeneration: number;
  evidenceEventIds: string[];
  evidence: Activity[];
  trace?: TraceContext;
}
export type ClaimJobOutcome = { kind: "claimed"; job: ClaimedJob } | { kind: "empty" };
export interface SummaryResult { speak: boolean; kind: "progress" | "completed" | "blocked" | "error"; text: string; evidenceEventIds: string[]; }
export interface JobCompletion { leaseToken: string; resultKey: string; result: SummaryResult; }
export interface JobReceipt { jobId: string; announcementId?: string; resultKey: string; }
export type CompleteJobOutcome = { kind: "accepted" | "duplicate"; receipt: JobReceipt } | { kind: "conflict"; code: ConflictCode } | { kind: "missing" };
export type ListeningSelection = Omit<ListeningScope, "generation"> | null;
export type SetListeningScopeOutcome = { kind: "changed" | "unchanged"; scope: ListeningScope | null; generation: number } | { kind: "unknown"; code: "source" | "workspace" | "tab" | "scope" };
export interface CaptureFact { captureMode: Activity["captureMode"]; status: Activity["status"]; truncated: boolean; observedAt: string; expiresAt: string; }
export interface CaptureByParticipant { participantId: ParticipantId; capture: CaptureFact | null; }
export interface StateDto { sources: Array<{ sourceId: SourceId; stale: boolean; observedAt: string; topologySequence: number; listeningGeneration: number }>; topology: Topology | null; scope: ListeningScope | null; captureByParticipant: CaptureByParticipant[]; jobs: { pending: number; leased: number; completed: number; expired: number }; announcements: { count: number }; eventSequence: number; }
export interface StateQuery { sourceId?: SourceId; }
export interface HistoryQuery {
  sourceId?: SourceId;
  workspaceId?: WorkspaceId;
  tabId?: TabId;
  participantId?: ParticipantId;
  from?: string;
  to?: string;
  cursor?: string;
  limit?: number;
  order?: "asc" | "desc";
}
export interface CaptureContext { workspace?: { id: string; label: string }; tab?: { id: string; label: string }; pane?: { id: string; label?: string }; participant: { id: string; kind: string }; }
export interface HistoryPage { announcements: Array<{ id: string; jobId: string; sourceId: SourceId; participantId: ParticipantId; createdAt: string; summary: SummaryResult }>; results: Array<{ jobId: string; sourceId: SourceId; participantId: ParticipantId; createdAt: string; result: SummaryResult; capture: CaptureContext }>; nextCursor?: string; }
export type EventsSinceOutcome = { kind: "events"; events: Array<{ sequence: number; kind: string; value: unknown }>; latestSequence: number } | { kind: "expired" };
export interface PruneResult { expiredEvidence: number; expiredHistory: number; expiredMedia: number; terminalizedJobs: number; }

export interface Storage {
  ingestBatch(batch: IngestBatch): StorageEffect<IngestOutcome>;
  lookupBatchReceipt(query: BatchReceiptQuery): StorageEffect<BatchReceiptOutcome>;
  claimJob(workerId: string): StorageEffect<ClaimJobOutcome>;
  completeJob(jobId: string, completion: JobCompletion): StorageEffect<CompleteJobOutcome>;
  setListeningScope(selection: ListeningSelection): StorageEffect<SetListeningScopeOutcome>;
  getState(query?: StateQuery): StorageEffect<StateDto>;
  getHistory(query: HistoryQuery): StorageEffect<HistoryPage>;
  getEventsSince(sequence: number): StorageEffect<EventsSinceOutcome>;
  prune(): StorageEffect<PruneResult>;
}

type SourceRow = { source_id: string; epoch: string; topology_sequence: number; topology_digest: string; observed_at: string; stale: number; listening_generation: number; scope_json: string | null };
type CursorRow = { source_id: string; cursor_value: string; baseline_generation: number | null };
type EventRow = { digest: string };
type JobRow = { job_id: string; source_id: string; participant_id: string; generation: number; evidence_json: string | null; status: string; attempts: number; lease_token: string | null; lease_expires_at: string | null; completion_lease_token: string | null; result_key: string | null; result_digest: string | null; result_json: string | null; capture_json: string | null; receipt_json: string | null; expires_at: string; terminal_expires_at: string | null; terminal_outcome: string | null };

const now = () => new Date().toISOString();
const after = (milliseconds: number) => new Date(Date.now() + milliseconds).toISOString();
const hash = (value: unknown) => createHash("sha256").update(canonical(value)).digest("hex");
const canonical = (value: unknown): string => {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`).join(",")}}`;
};
interface StatementAdapter { get(...values: unknown[]): unknown; all(...values: unknown[]): unknown[]; }
interface SQLiteAdapter {
  run(sql: string, ...values: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  query(sql: string): StatementAdapter;
  transaction<A>(run: () => A): () => A;
  close(): void;
}
const statement = (db: SQLiteAdapter, sql: string) => db.query(sql);
const one = <A>(db: SQLiteAdapter, sql: string, ...values: unknown[]): A | undefined => statement(db, sql).get(...values) as A | undefined;
const all = <A>(db: SQLiteAdapter, sql: string, ...values: unknown[]): A[] => statement(db, sql).all(...values) as A[];
const safe = <A>(operation: string, run: () => A): StorageEffect<A> => Effect.try({ try: run, catch: (cause) => new StorageError(operation, cause) });
const stale = (observedAt: string) => Date.parse(observedAt) + staleAfterMs <= Date.now();

function transaction<A>(db: SQLiteAdapter, run: () => A): A {
  return db.transaction(run)();
}

function applyMigrations(db: SQLiteAdapter): void {
  db.run("PRAGMA foreign_keys = ON");
  db.run("PRAGMA journal_mode = WAL");
  db.run("CREATE TABLE IF NOT EXISTS schema_migrations (id INTEGER PRIMARY KEY)");
  for (const migration of migrations) {
    if (one<{ id: number }>(db, "SELECT id FROM schema_migrations WHERE id = ?", migration.id)) continue;
    transaction(db, () => {
      for (const statement of migration.statements) db.run(statement);
      db.run("INSERT INTO schema_migrations (id) VALUES (?)", migration.id);
    });
  }
}

function assertTopology(batch: IngestBatch): void {
  const topology = batch.topology;
  if (topology.source.id !== batch.sourceId || !Number.isSafeInteger(batch.topologySequence) || batch.topologySequence <= 0) throw new Error("Invalid topology identity");
  const prefix = `${topology.source.namespace}:`;
  const unique = (values: string[]) => values.length === new Set(values).size;
  if (![topology.workspaces, topology.tabs, topology.panes, topology.participants].every((items) => unique(items.map((item) => item.id)))) throw new Error("Topology contains duplicate IDs");
  if (![topology.source.id, ...topology.workspaces.map((item) => item.id), ...topology.tabs.map((item) => item.id), ...topology.panes.map((item) => item.id), ...topology.participants.map((item) => item.id)].every((id) => id.startsWith(prefix))) throw new Error("Topology ID does not belong to source");
  const workspaces = new Set(topology.workspaces.map((item) => item.id));
  const tabs = new Set(topology.tabs.map((item) => item.id));
  const panes = new Set(topology.panes.map((item) => item.id));
  if (topology.workspaces.some((item) => item.sourceId !== batch.sourceId) || topology.tabs.some((item) => !workspaces.has(item.workspaceId)) || topology.panes.some((item) => !tabs.has(item.tabId)) || topology.participants.some((item) => item.sourceId !== batch.sourceId || !panes.has(item.paneId))) throw new Error("Topology parent is missing");
  if (!unique(batch.activities.map((item) => item.id)) || batch.activities.some((item) => !topology.participants.some((participant) => participant.id === item.participantId))) throw new Error("Invalid activity identity");
}

function writeTopology(db: SQLiteAdapter, topology: Topology): void {
  db.run("UPDATE workspaces SET present = 0 WHERE source_id = ?", topology.source.id);
  db.run("UPDATE tabs SET present = 0 WHERE workspace_id IN (SELECT workspace_id FROM workspaces WHERE source_id = ?)", topology.source.id);
  db.run("UPDATE panes SET present = 0 WHERE tab_id IN (SELECT tabs.tab_id FROM tabs JOIN workspaces ON workspaces.workspace_id = tabs.workspace_id WHERE workspaces.source_id = ?)", topology.source.id);
  db.run("UPDATE participants SET present = 0 WHERE source_id = ?", topology.source.id);
  for (const workspace of topology.workspaces) db.run("INSERT INTO workspaces (workspace_id, source_id, label, ordering, live, present) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT(workspace_id) DO UPDATE SET source_id = excluded.source_id, label = excluded.label, ordering = excluded.ordering, live = excluded.live, present = 1", workspace.id, workspace.sourceId, workspace.label, workspace.order, workspace.live ? 1 : 0);
  for (const tab of topology.tabs) db.run("INSERT INTO tabs (tab_id, workspace_id, label, ordering, present) VALUES (?, ?, ?, ?, 1) ON CONFLICT(tab_id) DO UPDATE SET workspace_id = excluded.workspace_id, label = excluded.label, ordering = excluded.ordering, present = 1", tab.id, tab.workspaceId, tab.label, tab.order);
  for (const pane of topology.panes) db.run("INSERT INTO panes (pane_id, tab_id, terminal_id, cwd, label, present) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT(pane_id) DO UPDATE SET tab_id = excluded.tab_id, terminal_id = excluded.terminal_id, cwd = excluded.cwd, label = excluded.label, present = 1", pane.id, pane.tabId, pane.terminalId, pane.cwd ?? null, pane.label ?? null);
  for (const participant of topology.participants) db.run("INSERT INTO participants (participant_id, source_id, pane_id, value_json, present) VALUES (?, ?, ?, ?, 1) ON CONFLICT(participant_id) DO UPDATE SET source_id = excluded.source_id, pane_id = excluded.pane_id, value_json = excluded.value_json, present = 1", participant.id, participant.sourceId, participant.paneId, JSON.stringify(participant));
}

function currentScope(row: SourceRow): ListeningScope | null {
  if (!row.scope_json) return null;
  return { ...(JSON.parse(row.scope_json) as Omit<ListeningScope, "generation">), generation: row.listening_generation };
}

function inScope(topology: Topology, participantId: ParticipantId, scope: ListeningScope | null): boolean {
  if (!scope) return false;
  const participant = topology.participants.find((item) => item.id === participantId);
  const pane = participant && topology.panes.find((item) => item.id === participant.paneId);
  const tab = pane && topology.tabs.find((item) => item.id === pane.tabId);
  return Boolean(tab && (!scope.workspaceId || tab.workspaceId === scope.workspaceId) && (!scope.tabId || tab.id === scope.tabId));
}

function captureContext(topology: Topology, participantId: ParticipantId): CaptureContext {
  const participant = topology.participants.find((item) => item.id === participantId);
  const pane = participant && topology.panes.find((item) => item.id === participant.paneId);
  const tab = pane && topology.tabs.find((item) => item.id === pane.tabId);
  const workspace = tab && topology.workspaces.find((item) => item.id === tab.workspaceId);
  return { ...(workspace ? { workspace: { id: workspace.id, label: workspace.label || workspace.id } } : {}), ...(tab ? { tab: { id: tab.id, label: tab.label || tab.id } } : {}), ...(pane ? { pane: { id: pane.id, label: pane.label || pane.id } } : {}), participant: { id: participantId, kind: participant?.kind ?? "unknown" } };
}

function historyCapture(capture: CaptureContext): CaptureContext {
  return {
    ...capture,
    ...(capture.workspace ? { workspace: { ...capture.workspace, label: capture.workspace.label || capture.workspace.id } } : {}),
    ...(capture.tab ? { tab: { ...capture.tab, label: capture.tab.label || capture.tab.id } } : {}),
    ...(capture.pane ? { pane: { ...capture.pane, label: capture.pane.label || capture.pane.id } } : {}),
  };
}

function event(db: SQLiteAdapter, kind: string, value: unknown): number {
  const change = db.run("INSERT INTO durable_events (kind, value_json, created_at) VALUES (?, ?, ?)", kind, JSON.stringify(value), now());
  return Number(change.lastInsertRowid);
}

async function pruneMedia(dataDir: string): Promise<number> {
  const audio = join(dataDir, "audio");
  let entries: string[];
  try { entries = await readdir(audio); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return 0; throw error; }
  let removed = 0;
  for (const name of entries) {
    const path = join(audio, name);
    const info = await stat(path);
    if (info.isDirectory()) continue;
    if (name.endsWith(".partial") || name.endsWith(".tmp") || info.mtimeMs <= Date.now() - evidenceRetentionMs) {
      await rm(path, { force: true });
      removed += 1;
    }
  }
  return removed;
}

function makeStorage(db: SQLiteAdapter, dataDir: string): Storage {
  return {
    lookupBatchReceipt: (query) => safe("lookup batch receipt", () => {
      const payload = query.payload;
      if (payload.sourceId !== query.sourceId || payload.sourceEpoch !== query.sourceEpoch || payload.batchId !== query.batchId) return { kind: "conflict", code: "batch_payload" };
      const source = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", query.sourceId);
      if (source && source.epoch !== query.sourceEpoch) return { kind: "conflict", code: "source_epoch" };
      const existing = one<{ digest: string; receipt_json: string }>(db, "SELECT digest, receipt_json FROM batches WHERE source_id = ? AND batch_id = ?", query.sourceId, query.batchId);
      if (!existing) return { kind: "missing" };
      return existing.digest === hash(payload) ? { kind: "duplicate", receipt: JSON.parse(existing.receipt_json) as BatchReceipt } : { kind: "conflict", code: "batch_payload" };
    }),
    ingestBatch: (batch) => safe("ingest batch", () => {
      assertTopology(batch);
      return transaction(db, () => {
        const digest = hash(batch);
        const existingBatch = one<{ digest: string; receipt_json: string }>(db, "SELECT digest, receipt_json FROM batches WHERE source_id = ? AND batch_id = ?", batch.sourceId, batch.batchId);
        if (existingBatch) return existingBatch.digest === digest ? { kind: "duplicate", receipt: JSON.parse(existingBatch.receipt_json) as BatchReceipt } : { kind: "conflict", code: "batch_payload" };
        let source = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", batch.sourceId);
        if (source && source.epoch !== batch.sourceEpoch) return { kind: "conflict", code: "source_epoch" };
        const topologyDigest = hash(batch.topology);
        if (source && (batch.topologySequence < source.topology_sequence || batch.topologySequence === source.topology_sequence && source.topology_digest !== "" && source.topology_digest !== topologyDigest)) return { kind: "conflict", code: "topology_sequence" };
        const currentParticipants = new Set(batch.topology.participants.map((participant) => participant.id));
        const cursorParticipants = new Set(batch.cursors.map((cursor) => cursor.participantId));
        if (cursorParticipants.size !== batch.cursors.length || batch.baselineReady.some((participantId) => !currentParticipants.has(participantId) || !cursorParticipants.has(participantId))) return { kind: "conflict", code: "cursor_cas" };
        for (const cursor of batch.cursors) {
          const existing = one<CursorRow>(db, "SELECT source_id, cursor_value, baseline_generation FROM cursors WHERE participant_id = ?", cursor.participantId);
          if (currentParticipants.has(cursor.participantId)) {
            if (existing?.source_id && existing.source_id !== batch.sourceId || (existing?.cursor_value ?? null) !== cursor.previous) return { kind: "conflict", code: "cursor_cas" };
            continue;
          }
          if (!existing || existing.source_id !== batch.sourceId || cursor.previous === null || cursor.previous !== cursor.next || existing.cursor_value !== cursor.previous) return { kind: "conflict", code: "cursor_cas" };
        }
        for (const activity of batch.activities) {
          const existing = one<EventRow>(db, "SELECT digest FROM events WHERE event_id = ?", activity.id);
          if (existing && existing.digest !== hash(activity)) return { kind: "conflict", code: "event_payload" };
        }
        if (!source) {
          db.run("INSERT INTO sources (source_id, epoch, observed_at) VALUES (?, ?, ?)", batch.sourceId, batch.sourceEpoch, batch.topology.source.observedAt);
          source = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", batch.sourceId)!;
        }
        if (batch.topologySequence > source.topology_sequence || source.topology_digest === "") writeTopology(db, batch.topology);
        db.run("UPDATE sources SET topology_sequence = ?, topology_digest = ?, observed_at = ?, stale = 0 WHERE source_id = ?", batch.topologySequence, topologyDigest, batch.topology.source.observedAt, batch.sourceId);
        for (const cursor of batch.cursors) {
          if (!currentParticipants.has(cursor.participantId)) continue;
          db.run("INSERT INTO cursors (participant_id, source_id, cursor_value, baseline_generation) VALUES (?, ?, ?, ?) ON CONFLICT(participant_id) DO UPDATE SET cursor_value = excluded.cursor_value, baseline_generation = excluded.baseline_generation", cursor.participantId, batch.sourceId, cursor.next, batch.baselineReady.includes(cursor.participantId) && batch.listeningGeneration === source!.listening_generation ? batch.listeningGeneration : one<CursorRow>(db, "SELECT source_id, cursor_value, baseline_generation FROM cursors WHERE participant_id = ?", cursor.participantId)?.baseline_generation ?? null);
        }
        const updatedSource = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", batch.sourceId)!;
        const scope = currentScope(updatedSource);
        const acceptedEventIds: string[] = [];
        for (const activity of batch.activities) {
          const existing = one<EventRow>(db, "SELECT digest FROM events WHERE event_id = ?", activity.id);
          if (existing) continue;
          db.run("INSERT INTO events (event_id, source_id, digest, participant_id, source_cursor, observed_at, evidence_json, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", activity.id, batch.sourceId, hash(activity), activity.participantId, activity.sourceCursor, activity.observedAt, JSON.stringify(activity), new Date(Date.parse(activity.observedAt) + evidenceRetentionMs).toISOString());
          acceptedEventIds.push(activity.id);
          const baseline = one<CursorRow>(db, "SELECT cursor_value, baseline_generation FROM cursors WHERE participant_id = ?", activity.participantId);
          if (batch.listeningGeneration === updatedSource.listening_generation && baseline?.baseline_generation === batch.listeningGeneration && !batch.baselineReady.includes(activity.participantId) && inScope(batch.topology, activity.participantId, scope)) {
            const inputKey = hash({ sourceId: batch.sourceId, eventId: activity.id, generation: batch.listeningGeneration });
            db.run("INSERT OR IGNORE INTO jobs (job_id, input_key, source_id, participant_id, generation, evidence_json, capture_json, status, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)", `job:${inputKey}`, inputKey, batch.sourceId, activity.participantId, batch.listeningGeneration, JSON.stringify([activity]), JSON.stringify(captureContext(batch.topology, activity.participantId)), now(), new Date(Date.parse(activity.observedAt) + evidenceRetentionMs).toISOString());
          }
        }
        const receipt: BatchReceipt = { batchId: batch.batchId, acceptedEventIds, cursors: batch.cursors.map((item) => ({ participantId: item.participantId, cursor: item.next })), topologySequence: batch.topologySequence };
        db.run("INSERT INTO batches VALUES (?, ?, ?, ?, ?)", batch.sourceId, batch.batchId, digest, JSON.stringify(receipt), now());
        event(db, "ingested", { sourceId: batch.sourceId, batchId: batch.batchId, acceptedEventIds });
        return { kind: "accepted", receipt };
      });
    }),
    claimJob: (workerId) => safe("claim job", () => transaction(db, () => {
      if (!workerId || workerId.length > 128) throw new Error("Invalid worker ID");
      const timestamp = now();
      const exhausted = all<{ job_id: string }>(db, "SELECT job_id FROM jobs WHERE status = 'leased' AND lease_expires_at <= ? AND attempts >= ?", timestamp, maxAttempts);
      if (exhausted.length) {
        db.run("UPDATE jobs SET status = 'expired', terminal_outcome = 'abandoned_outcome_unknown', evidence_json = NULL, capture_json = NULL, lease_token = NULL, lease_expires_at = NULL WHERE status = 'leased' AND lease_expires_at <= ? AND attempts >= ?", timestamp, maxAttempts);
        for (const job of exhausted) event(db, "lease_exhausted", { jobId: job.job_id, reason: "abandoned_outcome_unknown" });
      }
      const job = one<JobRow>(db, `SELECT candidate.* FROM jobs AS candidate
        WHERE (candidate.status = 'pending' OR (candidate.status = 'leased' AND candidate.lease_expires_at <= ?))
          AND candidate.expires_at > ?
          AND candidate.attempts < ?
          AND NOT EXISTS (
            SELECT 1 FROM jobs AS active
            WHERE active.source_id = candidate.source_id
              AND active.participant_id = candidate.participant_id
              AND active.status = 'leased'
              AND active.lease_expires_at > ?
          )
          AND NOT EXISTS (
            SELECT 1 FROM jobs AS earlier
            WHERE earlier.source_id = candidate.source_id
              AND earlier.participant_id = candidate.participant_id
              AND earlier.status IN ('pending', 'leased')
              AND earlier.expires_at > ?
              AND earlier.attempts < ?
              AND earlier.rowid < candidate.rowid
          )
        ORDER BY candidate.created_at, candidate.rowid
        LIMIT 1`, timestamp, timestamp, maxAttempts, timestamp, timestamp, maxAttempts);
      if (!job) return { kind: "empty" };
      const leaseToken = randomUUID();
      const expiresAt = after(leaseDurationMs);
      const attempt = job.attempts + 1;
      db.run("UPDATE jobs SET status = 'leased', attempts = ?, lease_token = ?, lease_expires_at = ? WHERE job_id = ?", attempt, leaseToken, expiresAt, job.job_id);
      const evidence = JSON.parse(job.evidence_json ?? "[]") as Activity[];
      return { kind: "claimed", job: { id: job.job_id, attempt, leaseToken, leaseExpiresAt: expiresAt, leaseDurationMs, sourceId: job.source_id as SourceId, participantId: job.participant_id as ParticipantId, scopeGeneration: job.generation, evidenceEventIds: evidence.map((item) => item.id), evidence, trace: evidence[0]?.trace } };
    })),
    completeJob: (jobId, completion) => safe("complete job", () => transaction(db, () => {
      const job = one<JobRow>(db, "SELECT * FROM jobs WHERE job_id = ?", jobId);
      if (!job) return { kind: "missing" };
      const resultDigest = hash(completion.result);
      if (job.status === "completed") {
        if (job.result_key !== completion.resultKey || job.result_digest !== resultDigest) return { kind: "conflict", code: "completion_payload" };
        return job.completion_lease_token === completion.leaseToken ? { kind: "duplicate", receipt: JSON.parse(job.receipt_json!) as JobReceipt } : { kind: "conflict", code: "lease_token" };
      }
      if (job.status !== "leased" || job.lease_token !== completion.leaseToken || !job.lease_expires_at || job.lease_expires_at <= now()) return { kind: "conflict", code: "lease_token" };
      const evidence = JSON.parse(job.evidence_json ?? "[]") as Activity[];
      if (completion.result.evidenceEventIds.length !== evidence.length || completion.result.evidenceEventIds.some((id) => !evidence.some((item) => item.id === id))) return { kind: "conflict", code: "completion_payload" };
      const source = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", job.source_id)!;
      const announcementId = completion.result.speak && source.listening_generation === job.generation ? `announcement:${job.job_id}` : undefined;
      const receipt: JobReceipt = { jobId, ...(announcementId ? { announcementId } : {}), resultKey: completion.resultKey };
      db.run("UPDATE jobs SET status = 'completed', result_key = ?, result_digest = ?, result_json = ?, receipt_json = ?, completion_lease_token = ?, lease_expires_at = NULL, terminal_expires_at = ? WHERE job_id = ?", completion.resultKey, resultDigest, JSON.stringify(completion.result), JSON.stringify(receipt), completion.leaseToken, after(historyRetentionMs), jobId);
      if (announcementId) db.run("INSERT INTO announcements VALUES (?, ?, ?, ?, ?, ?, ?, ?)", announcementId, jobId, job.source_id, job.participant_id, job.generation, JSON.stringify(completion.result), now(), after(historyRetentionMs));
      event(db, "completed", { jobId, ...(announcementId ? { announcementId } : {}) });
      return { kind: "accepted", receipt };
    })),
    setListeningScope: (selection) => safe("set listening scope", () => transaction(db, () => {
      if (!selection) {
        const active = one<SourceRow>(db, "SELECT * FROM sources WHERE scope_json IS NOT NULL LIMIT 1");
        if (!active) return { kind: "unchanged", scope: null, generation: 0 };
        const generation = active.listening_generation + 1;
        db.run("UPDATE sources SET listening_generation = listening_generation + 1, scope_json = NULL WHERE source_id = ?", active.source_id);
        db.run("UPDATE cursors SET baseline_generation = NULL WHERE source_id = ?", active.source_id);
        event(db, "listening", { sourceId: active.source_id, generation });
        return { kind: "changed", scope: null, generation };
      }
      if (selection.workspaceId && selection.tabId) return { kind: "unknown", code: "scope" };
      const source = one<SourceRow>(db, "SELECT * FROM sources WHERE source_id = ?", selection.sourceId);
      if (!source) return { kind: "unknown", code: "source" };
      if (selection.workspaceId && !one<{ workspace_id: string }>(db, "SELECT workspace_id FROM workspaces WHERE workspace_id = ? AND source_id = ? AND present = 1", selection.workspaceId, selection.sourceId)) return { kind: "unknown", code: "workspace" };
      if (selection.tabId && !one<{ tab_id: string }>(db, "SELECT tabs.tab_id FROM tabs JOIN workspaces ON workspaces.workspace_id = tabs.workspace_id WHERE tabs.tab_id = ? AND tabs.present = 1 AND workspaces.source_id = ? AND workspaces.present = 1", selection.tabId, selection.sourceId)) return { kind: "unknown", code: "tab" };
      const requested = JSON.stringify(selection);
      const otherActive = all<{ source_id: string }>(db, "SELECT source_id FROM sources WHERE source_id != ? AND scope_json IS NOT NULL", selection.sourceId);
      if (source.scope_json === requested && !otherActive.length) return { kind: "unchanged", scope: { ...selection, generation: source.listening_generation }, generation: source.listening_generation };
      for (const other of otherActive) {
        db.run("UPDATE sources SET listening_generation = listening_generation + 1, scope_json = NULL WHERE source_id = ?", other.source_id);
        db.run("UPDATE cursors SET baseline_generation = NULL WHERE source_id = ?", other.source_id);
      }
      const generation = source.scope_json === requested ? source.listening_generation : source.listening_generation + 1;
      db.run("UPDATE sources SET listening_generation = ?, scope_json = ? WHERE source_id = ?", generation, requested, selection.sourceId);
      if (source.scope_json !== requested) db.run("UPDATE cursors SET baseline_generation = NULL WHERE source_id = ?", selection.sourceId);
      event(db, "listening", { sourceId: selection.sourceId, generation });
      return { kind: "changed", scope: { ...selection, generation }, generation };
    })),
    getState: (query = {}) => safe("get state", () => transaction(db, () => {
      const snapshotAt = now();
      const sources = all<SourceRow>(db, "SELECT * FROM sources ORDER BY source_id");
      const active = sources.find((source) => source.scope_json);
      const source = query.sourceId ? sources.find((item) => item.source_id === query.sourceId) : active ?? sources[0];
      const topology = source ? {
        source: { id: source.source_id as SourceId, namespace: source.source_id.split(":", 1)[0] ?? source.source_id, stale: Boolean(source.stale) || stale(source.observed_at), observedAt: source.observed_at },
          workspaces: all<Omit<Topology["workspaces"][number], "live"> & { live: number }>(db, "SELECT workspace_id AS id, source_id AS sourceId, label, ordering AS 'order', live FROM workspaces WHERE source_id = ? AND present = 1 ORDER BY ordering", source.source_id).map(({ live, ...workspace }) => ({ ...workspace, live: live === 1 })),
        tabs: all<Topology["tabs"][number]>(db, "SELECT tabs.tab_id AS id, tabs.workspace_id AS workspaceId, tabs.label, tabs.ordering AS 'order' FROM tabs JOIN workspaces ON workspaces.workspace_id = tabs.workspace_id WHERE tabs.present = 1 AND workspaces.present = 1 AND workspaces.source_id = ? ORDER BY tabs.ordering", source.source_id),
          panes: all<Omit<Topology["panes"][number], "cwd" | "label"> & { cwd: string | null; label: string | null }>(db, "SELECT panes.pane_id AS id, panes.tab_id AS tabId, panes.terminal_id AS terminalId, panes.cwd, panes.label FROM panes JOIN tabs ON tabs.tab_id = panes.tab_id JOIN workspaces ON workspaces.workspace_id = tabs.workspace_id WHERE panes.present = 1 AND tabs.present = 1 AND workspaces.present = 1 AND workspaces.source_id = ? ORDER BY panes.pane_id", source.source_id).map(({ cwd, label, ...pane }) => ({ ...pane, ...(cwd === null ? {} : { cwd }), ...(label === null ? {} : { label }) })),
        participants: all<{ value_json: string }>(db, "SELECT value_json FROM participants WHERE source_id = ? AND present = 1 ORDER BY participant_id", source.source_id).map((row) => JSON.parse(row.value_json) as Topology["participants"][number]),
      } satisfies Topology : null;
      const captureByParticipant = topology?.participants.map((participant) => {
        const row = one<{ evidence_json: string; observed_at: string; expires_at: string }>(db, "SELECT evidence_json, observed_at, expires_at FROM events WHERE source_id = ? AND participant_id = ? AND evidence_json IS NOT NULL AND expires_at > ? ORDER BY expires_at DESC, rowid DESC LIMIT 1", topology.source.id, participant.id, snapshotAt);
        if (!row) return { participantId: participant.id, capture: null };
        const activity = JSON.parse(row.evidence_json) as Pick<Activity, "captureMode" | "status" | "truncated">;
        return { participantId: participant.id, capture: { captureMode: activity.captureMode, status: activity.status, truncated: activity.truncated, observedAt: row.observed_at, expiresAt: row.expires_at } };
      }) ?? [];
      const jobs = all<{ status: string; count: number }>(db, "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status");
      const count = (status: string) => jobs.find((row) => row.status === status)?.count ?? 0;
      const floor = Number(one<{ value: string }>(db, "SELECT value FROM storage_meta WHERE key = 'event_floor'")?.value ?? "1");
      const latestEvent = Number(one<{ value: number }>(db, "SELECT COALESCE(MAX(sequence), 0) AS value FROM durable_events")?.value ?? 0);
      return { sources: sources.map((item) => ({ sourceId: item.source_id as SourceId, stale: Boolean(item.stale) || stale(item.observed_at), observedAt: item.observed_at, topologySequence: item.topology_sequence, listeningGeneration: item.listening_generation })), topology, scope: active ? currentScope(active) : null, captureByParticipant, jobs: { pending: count("pending"), leased: count("leased"), completed: count("completed"), expired: count("expired") }, announcements: { count: Number(one<{ value: number }>(db, "SELECT COUNT(*) AS value FROM announcements")?.value ?? 0) }, eventSequence: Math.max(latestEvent, floor - 1) };
    })),
    getHistory: (query) => safe("get history", () => {
      const limit = Math.min(Math.max(query.limit ?? 50, 1), 100);
      const direction = query.order === "desc" ? "DESC" : "ASC";
      const predicates = ["status = 'completed'", "result_json IS NOT NULL", "capture_json IS NOT NULL"];
      const values: unknown[] = [];
      const add = (predicate: string, value: unknown) => { predicates.push(predicate); values.push(value); };
      if (query.sourceId) add("source_id = ?", query.sourceId);
      if (query.workspaceId) add("json_extract(capture_json, '$.workspace.id') = ?", query.workspaceId);
      if (query.tabId) add("json_extract(capture_json, '$.tab.id') = ?", query.tabId);
      if (query.participantId) add("participant_id = ?", query.participantId);
      if (query.from) add("created_at >= ?", query.from);
      if (query.to) add("created_at <= ?", query.to);
      if (query.cursor) add(`rowid ${direction === "DESC" ? "<" : ">"} ?`, Number(query.cursor));
      const rows = all<{ rowid: number; job_id: string; source_id: string; participant_id: string; created_at: string; result_json: string; capture_json: string }>(db, `SELECT rowid, job_id, source_id, participant_id, created_at, result_json, capture_json FROM jobs WHERE ${predicates.join(" AND ")} ORDER BY rowid ${direction} LIMIT ?`, ...values, limit + 1);
      const page = rows.slice(0, limit);
      const ids = page.map((row) => row.job_id);
      const announcements = ids.length ? all<{ announcement_id: string; job_id: string; source_id: string; participant_id: string; created_at: string; summary_json: string }>(db, `SELECT announcement_id, job_id, source_id, participant_id, created_at, summary_json FROM announcements WHERE job_id IN (${ids.map(() => "?").join(",")}) ORDER BY created_at`, ...ids) : [];
      return { announcements: announcements.map((row) => ({ id: row.announcement_id, jobId: row.job_id, sourceId: row.source_id as SourceId, participantId: row.participant_id as ParticipantId, createdAt: row.created_at, summary: JSON.parse(row.summary_json) as SummaryResult })), results: page.map((row) => ({ jobId: row.job_id, sourceId: row.source_id as SourceId, participantId: row.participant_id as ParticipantId, createdAt: row.created_at, result: JSON.parse(row.result_json) as SummaryResult, capture: historyCapture(JSON.parse(row.capture_json) as CaptureContext) })), ...(rows.length > limit ? { nextCursor: String(page.at(-1)!.rowid) } : {}) };
    }),
    getEventsSince: (sequence) => safe("get events", () => {
      const floor = Number(one<{ value: string }>(db, "SELECT value FROM storage_meta WHERE key = 'event_floor'")?.value ?? "1");
      if (sequence < floor - 1) return { kind: "expired" };
      const earliest = one<{ value: number | null }>(db, "SELECT MIN(sequence) AS value FROM durable_events")?.value;
      if (earliest !== null && earliest !== undefined && sequence < Number(earliest) - 1) return { kind: "expired" };
      const rows = all<{ sequence: number; kind: string; value_json: string }>(db, "SELECT sequence, kind, value_json FROM durable_events WHERE sequence > ? ORDER BY sequence LIMIT ?", sequence, eventReplayLimit);
      const latestSequence = Math.max(Number(one<{ value: number }>(db, "SELECT COALESCE(MAX(sequence), 0) AS value FROM durable_events")?.value ?? 0), floor - 1);
      return { kind: "events", events: rows.map((row) => ({ sequence: row.sequence, kind: row.kind, value: JSON.parse(row.value_json) })), latestSequence };
    }),
    prune: () => Effect.tryPromise({ try: async () => {
      const pruned = transaction(db, () => {
      const timestamp = now();
      const pending = db.run("UPDATE jobs SET status = 'expired', evidence_json = NULL, capture_json = NULL, lease_token = NULL, lease_expires_at = NULL WHERE status IN ('pending', 'leased') AND expires_at <= ?", timestamp).changes;
      const evidence = db.run("UPDATE events SET evidence_json = NULL WHERE evidence_json IS NOT NULL AND expires_at <= ?", timestamp).changes + db.run("UPDATE jobs SET evidence_json = NULL WHERE evidence_json IS NOT NULL AND expires_at <= ?", timestamp).changes + db.run("UPDATE jobs SET result_json = NULL, capture_json = NULL WHERE status = 'completed' AND terminal_expires_at IS NOT NULL AND terminal_expires_at <= ?", timestamp).changes;
      const history = db.run("DELETE FROM announcements WHERE expires_at <= ?", timestamp).changes;
      const eventCutoff = new Date(Date.now() - historyRetentionMs).toISOString();
      const removedThrough = one<{ value: number | null }>(db, "SELECT MAX(sequence) AS value FROM durable_events WHERE created_at <= ?", eventCutoff)?.value;
      db.run("DELETE FROM durable_events WHERE created_at <= ?", eventCutoff);
      if (removedThrough !== null && removedThrough !== undefined) db.run("INSERT INTO storage_meta (key, value) VALUES ('event_floor', ?) ON CONFLICT(key) DO UPDATE SET value = MAX(CAST(value AS INTEGER), CAST(excluded.value AS INTEGER))", String(Number(removedThrough) + 1));
      if (pending || history) event(db, "pruned", { pending, evidence, history });
      return { expiredEvidence: evidence, expiredHistory: history, expiredMedia: 0, terminalizedJobs: pending };
      });
      return { ...pruned, expiredMedia: await pruneMedia(dataDir) };
    }, catch: (cause) => new StorageError("prune", cause) }),
  };
}

export const openStorage = (dataDir: string): Effect.Effect<Storage, StorageError, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: async () => {
        await mkdir(dataDir, { recursive: true });
        const database = new Database(join(dataDir, "speak-now.sqlite"), { create: true }) as unknown as SQLiteAdapter;
        applyMigrations(database);
        return database;
      },
      catch: (cause) => new StorageError("open", cause),
    }),
    (database) => Effect.sync(() => database.close()),
  ).pipe(Effect.map((database) => makeStorage(database, dataDir)));
