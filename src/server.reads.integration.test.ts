import { expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { openStorage, type IngestBatch, type Storage } from "./storage";
import type { Activity, ParticipantId, SourceId, Topology, WorkspaceId } from "./shared";

const sourceA = "alpha:source" as SourceId;
const sourceB = "bravo:source" as SourceId;
const workspaceA = "alpha:workspace" as WorkspaceId;
const workspaceB = "bravo:workspace" as WorkspaceId;
const participantA = "alpha:participant" as ParticipantId;
const participantB = "bravo:participant" as ParticipantId;
const observedAt = new Date().toISOString();
const offsetTimestamp = (instant: number, offsetMinutes: number) => {
  const sign = offsetMinutes < 0 ? "-" : "+";
  const absolute = Math.abs(offsetMinutes);
  const offset = `${sign}${String(Math.floor(absolute / 60)).padStart(2, "0")}:${String(absolute % 60).padStart(2, "0")}`;
  return new Date(instant + offsetMinutes * 60_000).toISOString().replace("Z", offset);
};
const latestByInstantAt = offsetTimestamp(Date.parse(observedAt) + 2 * 60 * 60 * 1_000, -60);
const olderByInstantAt = offsetTimestamp(Date.parse(observedAt), 120);
const plusFifteenLatestAt = offsetTimestamp(Date.parse(observedAt) + 60_000, 15 * 60);
type CaptureOrderScenario = "normal" | "plus_fifteen";

const topology = (sourceId: SourceId, workspaceId: WorkspaceId, participantId: ParticipantId, label: string): Topology => {
  const namespace = sourceId.split(":", 1)[0]!;
  const tabId = `${namespace}:tab` as never;
  const paneId = `${namespace}:pane` as never;
  return {
    source: { id: sourceId, namespace, stale: false, observedAt },
    workspaces: [{ id: workspaceId, sourceId, label: `${label} workspace`, order: 0, live: true }],
    tabs: [{ id: tabId, workspaceId, label: `${label} tab`, order: 0 }],
    panes: [{ id: paneId, tabId, terminalId: `${label}-terminal`, label: `${label} pane` }],
    participants: [{ id: participantId, sourceId, paneId, rawPaneId: `${label}-raw-pane`, terminalId: `${label}-terminal`, kind: "codex", sessionId: `${label}-session`, sessionReferenceKind: "id", sessionReferenceSource: "fixture", generation: 1, active: true }],
  };
};

const activity = (id: string, participantId: ParticipantId, cursor: string, activityObservedAt = observedAt): Activity => ({
  id,
  participantId,
  sourceCursor: cursor,
  observedAt: activityObservedAt,
  kind: "assistant",
  text: id,
  captureMode: "structured",
  status: "complete",
  truncated: false,
  excerpt: "full",
  originalTextBytes: id.length,
});

const batch = (sourceId: SourceId, sourceEpoch: string, topologyValue: Topology, options: Pick<IngestBatch, "batchId" | "listeningGeneration" | "topologySequence" | "baselineReady" | "cursors" | "activities">): IngestBatch => ({
  sourceId,
  sourceEpoch,
  topology: topologyValue,
  ...options,
});

const useStorage = async <A>(dataDir: string, use: (storage: Storage) => Effect.Effect<A, unknown>) =>
  Effect.runPromise(Effect.scoped(Effect.gen(function*() {
    const storage = yield* openStorage(dataDir);
    return yield* use(storage);
  })));

const completeNext = (storage: Storage, resultKey: string) => Effect.gen(function*() {
  const claimed = yield* storage.claimJob(`worker-${resultKey}`);
  if (claimed.kind !== "claimed") throw new Error("expected claimed history job");
  yield* storage.completeJob(claimed.job.id, {
    leaseToken: claimed.job.leaseToken,
    resultKey,
    result: { speak: false, kind: "completed", text: resultKey, evidenceEventIds: [resultKey] },
  });
});

const seed = async (dataDir: string, activityObservedAt = observedAt, captureOrderScenario: CaptureOrderScenario = "normal") => useStorage(dataDir, (storage) => Effect.gen(function*() {
  const alpha = topology(sourceA, workspaceA, participantA, "alpha");
  const bravo = topology(sourceB, workspaceB, participantB, "bravo");
  yield* storage.ingestBatch(batch(sourceA, "epoch-alpha", alpha, { batchId: "alpha-bootstrap", listeningGeneration: 0, topologySequence: 1, baselineReady: [], cursors: [], activities: [] }));
  yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-bootstrap", listeningGeneration: 0, topologySequence: 1, baselineReady: [], cursors: [], activities: [] }));
  yield* storage.setListeningScope({ sourceId: sourceB, workspaceId: workspaceB });
  yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-baseline", listeningGeneration: 1, topologySequence: 1, baselineReady: [participantB], cursors: [{ participantId: participantB, previous: null, next: "bravo-base" }], activities: [] }));
  const firstBravoObservedAt = activityObservedAt === observedAt ? captureOrderScenario === "plus_fifteen" ? observedAt : latestByInstantAt : activityObservedAt;
  yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-history", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantB, previous: "bravo-base", next: "bravo-history" }], activities: [activity("bravo-history", participantB, "bravo-history", firstBravoObservedAt)] }));
  if (activityObservedAt === observedAt) {
    yield* completeNext(storage, "bravo-history");
    if (captureOrderScenario === "plus_fifteen") {
      yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-latest", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantB, previous: "bravo-history", next: "bravo-latest" }], activities: [{ ...activity("bravo-latest", participantB, "bravo-latest", plusFifteenLatestAt), status: "partial", truncated: true }] }));
    } else {
      yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-older", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantB, previous: "bravo-history", next: "bravo-older" }], activities: [{ ...activity("bravo-older", participantB, "bravo-older", olderByInstantAt), status: "limited", truncated: true }] }));
      yield* completeNext(storage, "bravo-older");
      yield* storage.ingestBatch(batch(sourceB, "epoch-bravo", bravo, { batchId: "bravo-latest", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantB, previous: "bravo-older", next: "bravo-latest" }], activities: [{ ...activity("bravo-latest", participantB, "bravo-latest", latestByInstantAt), status: "partial", truncated: true }] }));
    }
    yield* completeNext(storage, "bravo-latest");
  }
  yield* storage.setListeningScope({ sourceId: sourceA, workspaceId: workspaceA });
  yield* storage.ingestBatch(batch(sourceA, "epoch-alpha", alpha, { batchId: "alpha-baseline", listeningGeneration: 1, topologySequence: 1, baselineReady: [participantA], cursors: [{ participantId: participantA, previous: null, next: "alpha-base" }], activities: [] }));
  yield* storage.ingestBatch(batch(sourceA, "epoch-alpha", alpha, { batchId: "alpha-history-one", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantA, previous: "alpha-base", next: "alpha-one" }], activities: [activity("alpha-one", participantA, "alpha-one")] }));
  yield* completeNext(storage, "alpha-one");
  yield* storage.ingestBatch(batch(sourceA, "epoch-alpha", alpha, { batchId: "alpha-history-two", listeningGeneration: 1, topologySequence: 1, baselineReady: [], cursors: [{ participantId: participantA, previous: "alpha-one", next: "alpha-two" }], activities: [activity("alpha-two", participantA, "alpha-two")] }));
  yield* completeNext(storage, "alpha-two");
  yield* storage.ingestBatch(batch(sourceA, "epoch-alpha", { ...alpha, workspaces: [], tabs: [], panes: [], participants: [] }, { batchId: "alpha-exit", listeningGeneration: 1, topologySequence: 2, baselineReady: [], cursors: [], activities: [] }));
}));

const seedNullWireTopology = async (dataDir: string) => useStorage(dataDir, (storage) => Effect.gen(function*() {
  const topologyValue: Topology = {
    source: { id: sourceA, namespace: "alpha", stale: false, observedAt },
    workspaces: [{ id: workspaceA, sourceId: sourceA, label: "Nullable", order: 0, live: true }],
    tabs: [{ id: "alpha:tab" as never, workspaceId: workspaceA, label: "Tab", order: 0 }],
    panes: [{ id: "alpha:pane" as never, tabId: "alpha:tab" as never, terminalId: "nullable-terminal" }],
    participants: [],
  };
  yield* storage.ingestBatch(batch(sourceA, "null-wire-epoch", topologyValue, { batchId: "null-wire-topology-1", listeningGeneration: 0, topologySequence: 1, baselineReady: [], cursors: [], activities: [] }));
}));

async function unusedPort() {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing test port");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function withServer(check: (origin: string) => Promise<void>, activityObservedAt = observedAt, captureOrderScenario: CaptureOrderScenario = "normal", seedData: (dataDir: string, activityObservedAt?: string, captureOrderScenario?: CaptureOrderScenario) => Promise<void> = seed) {
  const root = await mkdtemp(join(tmpdir(), "speak-now-read-api-"));
  const dataDir = join(root, "data");
  const port = await unusedPort();
  const origin = `http://127.0.0.1:${port}`;
  let app: ReturnType<typeof Bun.spawn> | undefined;
  let stderr: Promise<string> | undefined;
  try {
    await seedData(dataDir, activityObservedAt, captureOrderScenario);
    app = Bun.spawn([process.execPath, "--no-env-file", "src/server.ts"], {
      cwd: process.cwd(),
      stdout: "ignore",
      stderr: "pipe",
      env: { PATH: process.env.PATH!, PORT: String(port), SPEAK_NOW_DATA_DIR: dataDir },
    });
    stderr = new Response(app.stderr as ReadableStream<Uint8Array>).text();
    const deadline = performance.now() + 5_000;
    while (performance.now() < deadline) {
      try {
        if ((await fetch(`${origin}/api/health`, { signal: AbortSignal.timeout(100) })).ok) break;
      } catch { /* Server is starting. */ }
      await Bun.sleep(20);
    }
    await check(origin);
  } finally {
    if (app && app.exitCode === null) app.kill("SIGTERM");
    if (app) {
      const timer = setTimeout(() => { if (app!.exitCode === null) app!.kill("SIGKILL"); }, 2_000);
      await app.exited;
      clearTimeout(timer);
      expect(app.exitCode, await stderr).toBe(0);
    }
    await rm(root, { recursive: true, force: true });
  }
}

test("GET /api/state reads an explicitly selected source without changing listening", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/api/state?sourceId=${encodeURIComponent(sourceB)}`);
    expect(response.status).toBe(200);
    const state = await response.json() as { topology: { source: { id: string } } | null; scope: { sourceId: string; workspaceId?: string } | null };
    expect(state.topology?.source.id).toBe(sourceB);
    expect(state.scope).toEqual(expect.objectContaining({ sourceId: sourceA, workspaceId: workspaceA }));
  });
}, 15_000);

test("GET /api/state projects SQLite topology values as the public boolean and optional-pane DTO", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/api/state?sourceId=${encodeURIComponent(sourceA)}`);
    expect(response.status).toBe(200);
    const state = await response.json() as { topology: { workspaces: Array<{ live: unknown }>; panes: Array<Record<string, unknown>> } };
    expect({ live: state.topology.workspaces[0]?.live, pane: state.topology.panes[0] }).toEqual({
      live: true,
      pane: { id: "alpha:pane", tabId: "alpha:tab", terminalId: "nullable-terminal" },
    });
  }, observedAt, "normal", seedNullWireTopology);
}, 15_000);

test("GET /assets/client.css serves only the emitted CSS asset with its CSS MIME type", async () => {
  const assets = join(process.cwd(), "public", "assets");
  const cssPath = join(assets, "client.css");
  const arbitraryPath = join(assets, "not-allowed.txt");
  const css = ".shell { display: grid; }\n";
  const previousCss = await Bun.file(cssPath).exists() ? await Bun.file(cssPath).text() : undefined;
  const previousArbitrary = await Bun.file(arbitraryPath).exists() ? await Bun.file(arbitraryPath).text() : undefined;
  try {
    await mkdir(assets, { recursive: true });
    await Promise.all([writeFile(cssPath, css), writeFile(arbitraryPath, "not a public route\n")]);
    await withServer(async (origin) => {
      const [served, missing, arbitrary] = await Promise.all([
        fetch(`${origin}/assets/client.css`),
        fetch(`${origin}/assets/missing.css`),
        fetch(`${origin}/assets/not-allowed.txt`),
      ]);
      expect({
        css: { status: served.status, contentType: served.headers.get("content-type"), body: await served.text() },
        missing: missing.status,
        arbitrary: arbitrary.status,
      }).toEqual({
        css: { status: 200, contentType: "text/css; charset=utf-8", body: css },
        missing: 404,
        arbitrary: 404,
      });
    });
  } finally {
    if (previousCss === undefined) await rm(cssPath, { force: true }); else await writeFile(cssPath, previousCss);
    if (previousArbitrary === undefined) await rm(arbitraryPath, { force: true }); else await writeFile(arbitraryPath, previousArbitrary);
  }
}, 15_000);

test("GET /api/state orders retained Date.parse-admitted +15:00 capture facts by instant after restart", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/api/state?sourceId=${encodeURIComponent(sourceB)}`);
    expect(response.status).toBe(200);
    const state = await response.json() as { captureByParticipant?: Array<{ participantId: string; capture: Record<string, unknown> | null }> };
    expect(state.captureByParticipant?.find((item) => item.participantId === participantB)?.capture).toEqual(expect.objectContaining({ status: "partial", observedAt: plusFifteenLatestAt }));
  }, observedAt, "plus_fifteen");
}, 15_000);

test("GET /api/state exposes only the latest retained capture fact for the selected source", async () => {
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/api/state?sourceId=${encodeURIComponent(sourceB)}`);
    expect(response.status).toBe(200);
    const state = await response.json() as { captureByParticipant?: Array<{ participantId: string; capture: Record<string, unknown> | null }> };
    const fact = state.captureByParticipant?.find((item) => item.participantId === participantB)?.capture;
    expect(fact).toEqual(expect.objectContaining({ captureMode: "structured", status: "partial", truncated: true, observedAt: latestByInstantAt }));
    expect(Object.keys(fact ?? {}).sort()).toEqual(["captureMode", "expiresAt", "observedAt", "status", "truncated"]);
    expect(JSON.stringify(fact)).not.toContain("bravo-latest");
  });
}, 15_000);

test("GET /api/state omits expired capture facts after restart without pruning", async () => {
  const expiredActivityAt = new Date(Date.now() - 25 * 60 * 60 * 1_000).toISOString();
  await withServer(async (origin) => {
    const response = await fetch(`${origin}/api/state?sourceId=${encodeURIComponent(sourceB)}`);
    expect(response.status).toBe(200);
    const state = await response.json() as { captureByParticipant?: Array<{ participantId: string; capture: unknown }> };
    expect(state.captureByParticipant).toEqual([{ participantId: participantB, capture: null }]);
  }, expiredActivityAt);
}, 15_000);

test("GET /api/history filters captured rows before descending pagination after an exit and restart", async () => {
  await withServer(async (origin) => {
    const ascending = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&order=asc&limit=1`);
    expect(ascending.status).toBe(200);
    const oldest = await ascending.json() as { results: Array<{ result: { text: string } }>; nextCursor?: string };
    expect(oldest.results[0]?.result.text).toBe("alpha-one");
    expect(oldest.nextCursor).toBeTruthy();

    const first = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&workspaceId=${encodeURIComponent(workspaceA)}&participantId=${encodeURIComponent(participantA)}&order=desc&limit=1`);
    expect(first.status).toBe(200);
    const newest = await first.json() as { results: Array<{ sourceId: string; result: { text: string }; capture: { workspace?: { id: string }; participant: { id: string } } }>; nextCursor?: string };
    expect(newest.results).toEqual([expect.objectContaining({ sourceId: sourceA, capture: expect.objectContaining({ workspace: expect.objectContaining({ id: workspaceA }), participant: expect.objectContaining({ id: participantA }) }) })]);
    expect(newest.results[0]?.result.text).toBe("alpha-two");
    expect(newest.nextCursor).toBeTruthy();

    const older = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&workspaceId=${encodeURIComponent(workspaceA)}&participantId=${encodeURIComponent(participantA)}&order=desc&limit=1&cursor=${newest.nextCursor}`);
    expect(older.status).toBe(200);
    const page = await older.json() as { results: Array<{ sourceId: string; result: { text: string } }> };
    expect(page.results).toEqual([expect.objectContaining({ sourceId: sourceA })]);
    expect(page.results[0]?.result.text).toBe("alpha-one");
    expect(new Set([newest.results[0]?.result.text, page.results[0]?.result.text]).size).toBe(2);
  });
}, 15_000);

test("read selectors reject malformed, duplicate, and unknown source IDs", async () => {
  await withServer(async (origin) => {
    await expect(fetch(`${origin}/api/state?sourceId=&sourceId=${encodeURIComponent(sourceA)}`)).resolves.toMatchObject({ status: 400 });
    await expect(fetch(`${origin}/api/state?sourceId=${encodeURIComponent("missing:source")}`)).resolves.toMatchObject({ status: 404 });
    await expect(fetch(`${origin}/api/history?workspaceId=${encodeURIComponent(workspaceA)}&tabId=alpha%3Atab`)).resolves.toMatchObject({ status: 400 });
  });
}, 15_000);

test("GET /api/history rejects duplicate filter keys instead of broadening the page", async () => {
  await withServer(async (origin) => {
    await expect(fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&sourceId=${encodeURIComponent(sourceB)}`)).resolves.toMatchObject({ status: 400 });
  });
}, 15_000);

test("GET /api/history rejects noncanonical ranges and compares accepted ISO instants", async () => {
  await withServer(async (origin) => {
    const newest = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&order=desc&limit=1`);
    expect(newest.status).toBe(200);
    const latest = await newest.json() as { results: Array<{ createdAt: string; result: { text: string } }> };
    const createdAt = latest.results[0]?.createdAt;
    if (!createdAt) throw new Error("expected retained history");
    const sameInstantWithOffset = new Date(new Date(createdAt).getTime() + 60 * 60 * 1_000).toISOString().replace("Z", "+01:00");

    const exactInstant = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&from=${encodeURIComponent(sameInstantWithOffset)}&to=${encodeURIComponent(sameInstantWithOffset)}`);
    expect(exactInstant.status).toBe(200);
    expect((await exactInstant.json() as { results: Array<{ result: { text: string } }> }).results.map((row) => row.result.text)).toContain("alpha-two");
    await expect(fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&from=2026-10-07`)).resolves.toMatchObject({ status: 400 });
    await expect(fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&from=2026-10-08T00%3A00%3A00.000Z&to=2026-10-07T00%3A00%3A00.000Z`)).resolves.toMatchObject({ status: 400 });
    const contradictory = await fetch(`${origin}/api/history?sourceId=${encodeURIComponent(sourceA)}&workspaceId=${encodeURIComponent(workspaceB)}`);
    expect(contradictory.status).toBe(200);
    expect((await contradictory.json() as { results: unknown[] }).results).toEqual([]);
  });
}, 15_000);
