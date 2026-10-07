import { expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import { mkdir, mkdtemp, rm, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import * as storageModule from "./storage";
import { leaseDurationMs, openStorage, type IngestBatch, type Storage } from "./storage";
import type { Activity, ParticipantId, SourceId, Topology, WorkspaceId } from "./shared";

const sourceId = "fixture:source" as SourceId;
const workspaceId = "fixture:workspace" as WorkspaceId;
const participantId = "fixture:participant" as ParticipantId;
const secondParticipantId = "fixture:participant-b" as ParticipantId;
const secondSourceId = "second:source" as SourceId;
const secondWorkspaceId = "second:workspace" as WorkspaceId;
const fixtureObservedAt = new Date().toISOString();
const fixtureActivityAt = new Date(Date.now() + 1_000).toISOString();
const NativeDate = Date;
const setClockOffset = (offsetMs: number) => {
  globalThis.Date = class extends NativeDate {
    constructor(value?: number | string | Date) { super(value === undefined ? NativeDate.now() + offsetMs : value); }
    static now() { return NativeDate.now() + offsetMs; }
  } as DateConstructor;
};
const restoreClock = () => { globalThis.Date = NativeDate; };

const topology = (): Topology => ({
  source: { id: sourceId, namespace: "fixture", stale: false, observedAt: fixtureObservedAt },
  workspaces: [{ id: workspaceId, sourceId, label: "Fixture", order: 0, live: true }],
  tabs: [{ id: "fixture:tab" as never, workspaceId, label: "Fixture tab", order: 0 }],
  panes: [{ id: "fixture:pane" as never, tabId: "fixture:tab" as never, terminalId: "terminal-a" }],
  participants: [{ id: participantId, sourceId, paneId: "fixture:pane" as never, rawPaneId: "pane-a", terminalId: "terminal-a", kind: "codex", sessionId: "session-a", sessionReferenceKind: "id", sessionReferenceSource: "fixture", generation: 1, active: true }],
});

const emptyTopology = (): Topology => ({ ...topology(), workspaces: [], tabs: [], panes: [], participants: [] });
const secondTopology = (): Topology => ({
  source: { id: secondSourceId, namespace: "second", stale: false, observedAt: fixtureObservedAt },
  workspaces: [{ id: secondWorkspaceId, sourceId: secondSourceId, label: "Second", order: 0, live: true }],
  tabs: [{ id: "second:tab" as never, workspaceId: secondWorkspaceId, label: "Second tab", order: 0 }],
  panes: [{ id: "second:pane" as never, tabId: "second:tab" as never, terminalId: "terminal-b" }],
  participants: [{ id: "second:participant" as never, sourceId: secondSourceId, paneId: "second:pane" as never, rawPaneId: "pane-b", terminalId: "terminal-b", kind: "codex", sessionId: "session-b", sessionReferenceKind: "id", sessionReferenceSource: "fixture", generation: 1, active: true }],
});
const twoParticipantTopology = (): Topology => {
  const value = topology();
  value.panes.push({ id: "fixture:pane-b" as never, tabId: "fixture:tab" as never, terminalId: "terminal-b" });
  value.participants.push({ id: secondParticipantId, sourceId, paneId: "fixture:pane-b" as never, rawPaneId: "pane-b", terminalId: "terminal-b", kind: "codex", sessionId: "session-b", sessionReferenceKind: "id", sessionReferenceSource: "fixture", generation: 1, active: true });
  return value;
};

const activity = (id: string, cursor: string, observedAt = fixtureActivityAt, targetParticipantId = participantId): Activity => ({
  id,
  participantId: targetParticipantId,
  sourceCursor: cursor,
  observedAt,
  kind: "assistant",
  text: `activity ${id}`,
  captureMode: "structured",
  status: "complete",
  truncated: false,
  excerpt: "full",
  originalTextBytes: 16,
});

const batch = (overrides: Partial<IngestBatch> = {}): IngestBatch => ({
  sourceId,
  sourceEpoch: "epoch-a",
  batchId: "batch-a",
  listeningGeneration: 1,
  topologySequence: 1,
  topology: topology(),
  baselineReady: [],
  cursors: [{ participantId, previous: "cursor:baseline", next: "cursor:activity" }],
  activities: [activity("event-a", "cursor:activity")],
  ...overrides,
});

const useStorage = async <A>(dataDir: string, use: (storage: Storage) => Effect.Effect<A, unknown>) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const storage = yield* openStorage(dataDir);
    return yield* use(storage);
  })));

const establishBaseline = (storage: Storage) => Effect.gen(function*() {
  expect(yield* storage.ingestBatch(batch({ batchId: "seed-a", listeningGeneration: 0, cursors: [{ participantId, previous: null, next: "cursor:seed" }], activities: [] }))).toMatchObject({ kind: "accepted" });
  expect(yield* storage.setListeningScope({ sourceId, workspaceId })).toMatchObject({ kind: "changed", generation: 1 });
  expect(yield* storage.ingestBatch(batch({ batchId: "baseline-a", baselineReady: [participantId], cursors: [{ participantId, previous: "cursor:seed", next: "cursor:baseline" }], activities: [] }))).toMatchObject({ kind: "accepted" });
});

test("reopened exact batch retry keeps one durable activity acknowledgement and job", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const first = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      return yield* storage.ingestBatch(batch());
    }));
    const retried = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      const duplicate = yield* storage.ingestBatch(batch());
      const claimed = yield* storage.claimJob("worker-a");
      const empty = yield* storage.claimJob("worker-b");
      return { duplicate, claimed, empty };
    }));

    expect(first).toMatchObject({ kind: "accepted" });
    if (first.kind === "conflict") throw new Error(`unexpected ingest conflict: ${first.code}`);
    expect(retried.duplicate).toEqual({ kind: "duplicate", receipt: first.receipt });
    expect(retried.claimed).toMatchObject({ kind: "claimed", job: { evidenceEventIds: ["event-a"] } });
    expect(retried.empty).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a stale opaque cursor rejects the complete batch without admitting its activity", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      const conflict = yield* storage.ingestBatch(batch({ batchId: "stale-cursor", cursors: [{ participantId, previous: "unrelated:opaque", next: "cursor:activity" }] }));
      const claim = yield* storage.claimJob("worker-a");
      return { conflict, claim };
    }));

    expect(result.conflict).toEqual({ kind: "conflict", code: "cursor_cas" });
    expect(result.claim).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("expired lease token cannot complete a job claimed again by another worker", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const outcome = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const initial = yield* storage.claimJob("worker-a");
      expect(initial.kind).toBe("claimed");
      if (initial.kind !== "claimed") throw new Error("expected initial claim");
      setClockOffset(61_000);
      const reclaimed = yield* storage.claimJob("worker-b");
      expect(reclaimed.kind).toBe("claimed");
      if (reclaimed.kind !== "claimed") throw new Error("expected reclaimed job");
      const stale = yield* storage.completeJob(initial.job.id, { leaseToken: initial.job.leaseToken, resultKey: "result-a", result: { speak: false, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] } });
      const accepted = yield* storage.completeJob(reclaimed.job.id, { leaseToken: reclaimed.job.leaseToken, resultKey: "result-a", result: { speak: false, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] } });
      const duplicate = yield* storage.completeJob(reclaimed.job.id, { leaseToken: reclaimed.job.leaseToken, resultKey: "result-a", result: { speak: false, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] } });
      return { stale, accepted, duplicate };
    }));

    expect(outcome.stale).toEqual({ kind: "conflict", code: "lease_token" });
    expect(outcome.accepted).toMatchObject({ kind: "accepted" });
    if (outcome.accepted.kind !== "accepted") throw new Error("expected accepted completion receipt");
    if (outcome.duplicate.kind !== "duplicate") throw new Error("expected completion retry receipt");
    expect(outcome.duplicate).toEqual({ kind: "duplicate", receipt: outcome.accepted.receipt });
  } finally {
    restoreClock();
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("pruning old pending evidence terminalizes the job but preserves dedupe and cursor safety", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      const old = batch({ batchId: "old-batch", activities: [activity("old-event", "cursor:activity", "2026-08-01T12:00:00.000Z")] });
      yield* storage.ingestBatch(old);
      const pruned = yield* storage.prune();
      const replay = yield* storage.ingestBatch(old);
      const claim = yield* storage.claimJob("worker-a");
      return { pruned, replay, claim };
    }));

    expect(result.pruned.terminalizedJobs).toBe(1);
    expect(result.replay).toMatchObject({ kind: "duplicate" });
    expect(result.claim).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a newer authoritative snapshot hides absent inventory without losing its cursor tombstone", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      const removed = yield* storage.ingestBatch(batch({ batchId: "snapshot-removed", topologySequence: 2, topology: emptyTopology(), cursors: [], activities: [] }));
      const state = yield* storage.getState();
      const restored = yield* storage.ingestBatch(batch({ batchId: "snapshot-restored", topologySequence: 3, cursors: [{ participantId, previous: "cursor:baseline", next: "cursor:restored" }], activities: [] }));
      return { removed, state, restored };
    }));

    expect(result.removed).toMatchObject({ kind: "accepted" });
    expect(result.state.topology?.participants).toEqual([]);
    expect(result.restored).toMatchObject({ kind: "accepted" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a baseline marker and activity in one batch do not enqueue old activity", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* storage.ingestBatch(batch({ batchId: "seed-a", listeningGeneration: 0, cursors: [{ participantId, previous: null, next: "cursor:seed" }], activities: [] }));
      yield* storage.setListeningScope({ sourceId, workspaceId });
      const admitted = yield* storage.ingestBatch(batch({ batchId: "baseline-with-activity", baselineReady: [participantId], cursors: [{ participantId, previous: "cursor:seed", next: "cursor:baseline" }], activities: [activity("baseline-event", "cursor:baseline")] }));
      const claim = yield* storage.claimJob("worker-a");
      return { admitted, claim };
    }));

    expect(result.admitted).toMatchObject({ kind: "accepted" });
    expect(result.claim).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("speak false completion is history without an announcement", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected job");
      const completed = yield* storage.completeJob(claim.job.id, { leaseToken: claim.job.leaseToken, resultKey: "silent", result: { speak: false, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] } });
      const history = yield* storage.getHistory({});
      return { completed, history };
    }));

    expect(result.completed).toMatchObject({ kind: "accepted" });
    expect(leaseDurationMs).toBe(60_000);
    expect(result.history.announcements).toEqual([]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("accepted completion cannot be replayed by a different lease token", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected job");
      const completion = { resultKey: "result-a", result: { speak: false as const, kind: "progress" as const, text: "stored", evidenceEventIds: ["event-a"] } };
      const accepted = yield* storage.completeJob(claim.job.id, { ...completion, leaseToken: claim.job.leaseToken });
      const stale = yield* storage.completeJob(claim.job.id, { ...completion, leaseToken: "replaced-token" });
      return { accepted, stale };
    }));

    expect(result.accepted).toMatchObject({ kind: "accepted" });
    expect(result.stale).toEqual({ kind: "conflict", code: "lease_token" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("state reports a complete current topology and stale source from its last seen time", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const state = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* storage.ingestBatch(batch({ batchId: "old-source", topology: { ...topology(), source: { ...topology().source, observedAt: "2026-08-01T12:00:00.000Z" } }, listeningGeneration: 0, cursors: [{ participantId, previous: null, next: "cursor:seed" }], activities: [] }));
      return yield* storage.getState();
    }));

    expect(state.topology?.participants.map((participant) => participant.id)).toEqual([participantId]);
    expect(state.sources).toEqual([expect.objectContaining({ sourceId, stale: true, topologySequence: 1 })]);
    expect(state.eventSequence).toBeGreaterThan(0);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("generation zero topology bootstrap is durable and leaves listening scope unchanged", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      const bootstrap = batch({ batchId: "bootstrap", listeningGeneration: 0, cursors: [], baselineReady: [], activities: [] });
      const accepted = yield* storage.ingestBatch(bootstrap);
      const duplicate = yield* storage.ingestBatch(bootstrap);
      const state = yield* storage.getState();
      const claim = yield* storage.claimJob("worker-a");
      return { accepted, duplicate, state, claim };
    }));

    expect(result.accepted).toMatchObject({ kind: "accepted" });
    if (result.accepted.kind === "conflict") throw new Error(`unexpected bootstrap conflict: ${result.accepted.code}`);
    expect(result.duplicate).toEqual({ kind: "duplicate", receipt: result.accepted.receipt });
    expect(result.state.scope).toBeNull();
    expect(result.claim).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("state indicators and history retain capture-time location after a topology rename", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const renamed = topology();
      renamed.workspaces[0]!.label = "Renamed workspace";
      renamed.tabs[0]!.label = "Renamed tab";
      renamed.panes[0]!.label = "Renamed pane";
      yield* storage.ingestBatch(batch({ batchId: "renamed-topology", topologySequence: 2, topology: renamed, cursors: [], activities: [] }));
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected job");
      yield* storage.completeJob(claim.job.id, { leaseToken: claim.job.leaseToken, resultKey: "spoken", result: { speak: true, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] } });
      return { state: yield* storage.getState(), history: yield* storage.getHistory({}) };
    }));

    expect(result.state.jobs).toEqual({ pending: 0, leased: 0, completed: 1, expired: 0 });
    expect(result.state.announcements).toEqual({ count: 1 });
    expect(result.history.results).toEqual([expect.objectContaining({ result: expect.objectContaining({ text: "stored" }), capture: expect.objectContaining({ workspace: expect.objectContaining({ label: "Fixture" }), tab: expect.objectContaining({ label: "Fixture tab" }), pane: expect.objectContaining({ id: "fixture:pane" }) }) })]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("nonspoken completion survives reopen as history without an announcement", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected job");
      return yield* storage.completeJob(claim.job.id, { leaseToken: claim.job.leaseToken, resultKey: "silent-reopen", result: { speak: false, kind: "completed", text: "saved without speech", evidenceEventIds: ["event-a"] } });
    }));
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      return { state: yield* storage.getState(), history: yield* storage.getHistory({}) };
    }));

    expect(result.state.announcements).toEqual({ count: 0 });
    expect(result.history.results).toEqual([expect.objectContaining({ result: { speak: false, kind: "completed", text: "saved without speech", evidenceEventIds: ["event-a"] } })]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("listening selection is singular and null clears the selected source", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* storage.ingestBatch(batch({ batchId: "bootstrap-a", listeningGeneration: 0, cursors: [], activities: [] }));
      yield* storage.ingestBatch(batch({ sourceId: secondSourceId, sourceEpoch: "epoch-b", batchId: "bootstrap-b", topology: secondTopology(), listeningGeneration: 0, cursors: [], activities: [] }));
      yield* storage.setListeningScope({ sourceId, workspaceId });
      const switched = yield* storage.setListeningScope({ sourceId: secondSourceId, workspaceId: secondWorkspaceId });
      const afterSwitch = yield* storage.getState();
      const cleared = yield* storage.setListeningScope(null);
      return { switched, afterSwitch, cleared, afterClear: yield* storage.getState() };
    }));

    expect(result.switched).toMatchObject({ kind: "changed", scope: { sourceId: secondSourceId, workspaceId: secondWorkspaceId } });
    expect(result.afterSwitch.scope).toMatchObject({ sourceId: secondSourceId, workspaceId: secondWorkspaceId });
    expect(result.afterSwitch.topology?.source.id).toBe(secondSourceId);
    expect(result.afterSwitch.topology?.workspaces.map((workspace) => workspace.sourceId)).toEqual([secondSourceId]);
    expect(result.afterSwitch.topology?.tabs.map((tab) => tab.workspaceId)).toEqual([secondWorkspaceId]);
    expect(result.cleared).toMatchObject({ kind: "changed", scope: null });
    expect(result.afterClear.scope).toBeNull();
    expect(result.afterClear.sources).toEqual(expect.arrayContaining([
      expect.objectContaining({ sourceId, listeningGeneration: 2 }),
      expect.objectContaining({ sourceId: secondSourceId, listeningGeneration: 2 }),
    ]));
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a valid stale-scope completion is history but not a new announcement", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected job");
      yield* storage.setListeningScope(null);
      const completion = { leaseToken: claim.job.leaseToken, resultKey: "stale-scope", result: { speak: true as const, kind: "completed" as const, text: "finished after leave", evidenceEventIds: ["event-a"] } };
      const accepted = yield* storage.completeJob(claim.job.id, completion);
      const duplicate = yield* storage.completeJob(claim.job.id, completion);
      return { accepted, duplicate, state: yield* storage.getState(), history: yield* storage.getHistory({}) };
    }));

    expect(result.accepted).toMatchObject({ kind: "accepted" });
    expect(result.duplicate).toMatchObject({ kind: "duplicate" });
    expect(result.state.announcements).toEqual({ count: 0 });
    expect(result.history.results).toEqual([expect.objectContaining({ result: expect.objectContaining({ text: "finished after leave" }) })]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("prune removes expired and partial media from the owned data directory", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const audio = join(dataDir, "audio");
    await mkdir(audio, { recursive: true });
    const old = join(audio, "old.mp3");
    const partial = join(audio, "generation.partial");
    const temporary = join(audio, "orphan.tmp");
    await Promise.all([writeFile(old, "old"), writeFile(partial, "partial"), writeFile(temporary, "temporary")]);
    const expired = new Date(Date.now() - 25 * 60 * 60 * 1_000);
    await utimes(old, expired, expired);

    const pruned = await useStorage(dataDir, (storage) => storage.prune());

    expect(pruned.expiredMedia).toBe(3);
    await expect(Promise.all([Bun.file(old).exists(), Bun.file(partial).exists(), Bun.file(temporary).exists()])).resolves.toEqual([false, false, false]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a first cursor conflict leaves no durable source or event sequence", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      const conflict = yield* storage.ingestBatch(batch({ batchId: "first-conflict", cursors: [{ participantId, previous: "unknown-opaque-cursor", next: "next" }] }));
      return { conflict, state: yield* storage.getState() };
    }));

    expect(result.conflict).toEqual({ kind: "conflict", code: "cursor_cas" });
    expect(result.state.sources).toEqual([]);
    expect(result.state.topology).toBeNull();
    expect(result.state.eventSequence).toBe(0);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("fully pruned events leave fresh state at a resumable durable high-water", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* storage.ingestBatch(batch({ batchId: "event-bootstrap", listeningGeneration: 0, cursors: [], activities: [] }));
      const before = yield* storage.getState();
      setClockOffset(31 * 24 * 60 * 60 * 1_000);
      yield* storage.prune();
      const fresh = yield* storage.getState();
      return { before, fresh, resume: yield* storage.getEventsSince(fresh.eventSequence), old: yield* storage.getEventsSince(0) };
    }));

    expect(result.fresh.eventSequence).toBeGreaterThanOrEqual(result.before.eventSequence);
    expect(result.resume).toEqual({ kind: "events", events: [], latestSequence: result.fresh.eventSequence });
    expect(result.old).toEqual({ kind: "expired" });
  } finally {
    restoreClock();
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("retention clears terminal and expired pending context while receipts and gaps remain durable", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const claim = yield* storage.claimJob("worker-a");
      if (claim.kind !== "claimed") throw new Error("expected completion job");
      const completion = { leaseToken: claim.job.leaseToken, resultKey: "retained-receipt", result: { speak: false as const, kind: "completed" as const, text: "expires from history", evidenceEventIds: ["event-a"] } };
      yield* storage.completeJob(claim.job.id, completion);
      const pendingBatch = batch({ batchId: "pending-retention", cursors: [{ participantId, previous: "cursor:activity", next: "cursor:pending" }], activities: [activity("pending-event", "cursor:pending")] });
      yield* storage.ingestBatch(pendingBatch);
      setClockOffset(31 * 24 * 60 * 60 * 1_000);
      const pruned = yield* storage.prune();
      return { pruned, history: yield* storage.getHistory({}), duplicate: yield* storage.completeJob(claim.job.id, completion), state: yield* storage.getState(), claim: yield* storage.claimJob("worker-b") };
    }));

    expect(result.pruned.terminalizedJobs).toBe(1);
    expect(result.history.results).toEqual([]);
    expect(result.duplicate).toMatchObject({ kind: "duplicate" });
    expect(result.state.jobs).toEqual({ pending: 0, leased: 0, completed: 1, expired: 1 });
    expect(result.claim).toEqual({ kind: "empty" });
  } finally {
    restoreClock();
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("a sixty-second lease admits an eleven-second worker and terminalizes the exhausted retry budget", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      yield* establishBaseline(storage);
      yield* storage.ingestBatch(batch());
      const first = yield* storage.claimJob("worker-a");
      if (first.kind !== "claimed") throw new Error("expected first claim");
      setClockOffset(11_000);
      const completed = yield* storage.completeJob(first.job.id, { leaseToken: first.job.leaseToken, resultKey: "eleven-seconds", result: { speak: false, kind: "progress", text: "within lease", evidenceEventIds: ["event-a"] } });
      restoreClock();
      const retryBatch = batch({ batchId: "retry-budget", cursors: [{ participantId, previous: "cursor:activity", next: "cursor:retry" }], activities: [activity("retry-event", "cursor:retry")] });
      yield* storage.ingestBatch(retryBatch);
      const attemptOne = yield* storage.claimJob("worker-b");
      if (attemptOne.kind !== "claimed") throw new Error("expected retry attempt one");
      setClockOffset(61_000);
      const attemptTwo = yield* storage.claimJob("worker-c");
      if (attemptTwo.kind !== "claimed") throw new Error("expected retry attempt two");
      setClockOffset(122_000);
      const exhausted = yield* storage.claimJob("worker-d");
      return { completed, attemptOne, attemptTwo, exhausted, state: yield* storage.getState(), events: yield* storage.getEventsSince(0) };
    }));

    expect(result.completed).toMatchObject({ kind: "accepted" });
    expect(result.attemptOne).toMatchObject({ kind: "claimed", job: { leaseDurationMs: 60_000 } });
    expect(result.attemptOne.job.leaseExpiresAt).toBeTruthy();
    expect(storageModule.maxAttempts).toBe(2);
    expect(result.attemptTwo.job.attempt).toBe(storageModule.maxAttempts);
    expect(result.exhausted).toEqual({ kind: "empty" });
    expect(result.state.jobs).toEqual({ pending: 0, leased: 0, completed: 1, expired: 1 });
    expect(result.events).toMatchObject({ kind: "events", events: expect.arrayContaining([expect.objectContaining({ kind: "lease_exhausted", value: expect.objectContaining({ reason: "abandoned_outcome_unknown" }) })]) });
  } finally {
    restoreClock();
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("claim serializes a participant while allowing another participant to make progress", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-storage-"));
  try {
    const result = await useStorage(dataDir, (storage) => Effect.gen(function*() {
      const tree = twoParticipantTopology();
      yield* storage.ingestBatch(batch({ batchId: "two-seed", topology: tree, listeningGeneration: 0, cursors: [], activities: [] }));
      yield* storage.setListeningScope({ sourceId, workspaceId });
      yield* storage.ingestBatch(batch({ batchId: "two-baseline", topology: tree, baselineReady: [participantId, secondParticipantId], cursors: [{ participantId, previous: null, next: "a-base" }, { participantId: secondParticipantId, previous: null, next: "b-base" }], activities: [] }));
      yield* storage.ingestBatch(batch({ batchId: "two-events", topology: tree, cursors: [{ participantId, previous: "a-base", next: "a-next" }, { participantId: secondParticipantId, previous: "b-base", next: "b-next" }], activities: [activity("a-one", "a-next"), activity("a-two", "a-next"), activity("b-one", "b-next", fixtureActivityAt, secondParticipantId)] }));
      return { first: yield* storage.claimJob("worker-a"), second: yield* storage.claimJob("worker-b"), third: yield* storage.claimJob("worker-c") };
    }));

    expect(result.first).toMatchObject({ kind: "claimed", job: { participantId } });
    expect(result.first).toMatchObject({ kind: "claimed", job: { evidenceEventIds: ["a-one"] } });
    expect(result.second).toMatchObject({ kind: "claimed", job: { participantId: secondParticipantId } });
    expect(result.third).toEqual({ kind: "empty" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});
