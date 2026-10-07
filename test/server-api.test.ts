import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Effect } from "effect";
import { openStorage, type IngestBatch } from "../src/storage";

const startServer = async (collectorToken?: string, options: { dataDirectory?: string; cwd?: string } = {}) => {
  const ownsDataDirectory = options.dataDirectory === undefined;
  const dataDirectory = options.dataDirectory ?? await mkdtemp(join(tmpdir(), "speak-now-server-api-"));
  const port = 45_000 + Math.floor(Math.random() * 1_000);
  const origin = `http://localhost:${port}`;
  const server = Bun.spawn([Bun.which("bun")!, "src/server.ts"], {
    cwd: options.cwd ?? process.cwd(),
    stdout: "ignore",
    stderr: "pipe",
    env: {
      ...process.env,
      PORT: String(port),
      SPEAK_NOW_DATA_DIR: dataDirectory,
      ...(collectorToken ? { SPEAK_NOW_COLLECTOR_TOKEN: collectorToken } : {}),
    },
  });
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
    if ((await fetch(`${origin}/api/health`)).ok) return { dataDirectory, origin, server, ownsDataDirectory };
    } catch {
      // The server is starting.
    }
    await Bun.sleep(10);
  }
  server.kill();
  await server.exited;
  if (ownsDataDirectory) await rm(dataDirectory, { recursive: true, force: true });
  throw new Error("server did not start");
};

const stopServer = async ({ dataDirectory, server, ownsDataDirectory }: Awaited<ReturnType<typeof startServer>>, removeData = ownsDataDirectory) => {
  server.kill();
  await server.exited;
  if (removeData) await rm(dataDirectory, { recursive: true, force: true });
};

const reports = async (dataDirectory: string) => {
  const directory = join(dataDirectory, "errors");
  try {
    return (await Promise.all((await readdir(directory)).filter((name) => name.endsWith(".jsonl")).map(async (name) => (await readFile(join(directory, name), "utf8")).trim().split("\n").filter(Boolean)))).flat();
  } catch { return []; }
};

const settledReports = async (dataDirectory: string) => {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    const lines = await reports(dataDirectory);
    if (lines.length) return lines;
    await Bun.sleep(10);
  }
  return [] as string[];
};

const jsonLines = async (directory: string): Promise<Record<string, unknown>[]> => {
  const visit = async (path: string): Promise<Record<string, unknown>[]> => {
    try {
      const entries = await readdir(path, { withFileTypes: true });
      return (await Promise.all(entries.map(async (entry) => {
        const target = join(path, entry.name);
        if (entry.isDirectory()) return visit(target);
        if (!entry.isFile() || !entry.name.endsWith(".jsonl")) return [];
        return (await readFile(target, "utf8")).split("\n").filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>);
      }))).flat();
    } catch { return []; }
  };
  return visit(directory);
};

const collectorHeaders = { Authorization: "Bearer collector-test-token", "Content-Type": "application/json" };
const sourceNamespace = "source-a";
const sourceId = `${sourceNamespace}:herdr`;
const source = sourceId;
const workspaceId = `${sourceNamespace}:workspace-a`;
const tabId = `${sourceNamespace}:tab-a`;
const paneId = `${sourceNamespace}:pane-a`;
const participantId = `${sourceNamespace}:participant-a`;
const fixtureObservedAt = new Date().toISOString();

const batch = (overrides: Record<string, unknown> = {}) => ({
  sourceId,
  sourceEpoch: "epoch-a",
  batchId: "batch-a",
  listeningGeneration: 0,
  topologySequence: 1,
  topology: {
    source: { id: source, namespace: sourceNamespace, stale: false, observedAt: fixtureObservedAt },
    workspaces: [{ id: workspaceId, sourceId: source, label: "Workspace A", order: 0, live: true }],
    tabs: [{ id: tabId, workspaceId, label: "Tab A", order: 0 }],
    panes: [{ id: paneId, tabId, terminalId: "terminal-a", label: "Pane A" }],
    participants: [{ id: participantId, sourceId: source, paneId, rawPaneId: "pane-a", terminalId: "terminal-a", kind: "codex", sessionId: "session-a", sessionReferenceKind: "id", sessionReferenceSource: "herdr:codex", generation: 0, active: true }],
  },
  baselineReady: [],
  cursors: [{ participantId, previous: null, next: "cursor-1" }],
  activities: [],
  ...overrides,
});

const postBatch = (origin: string, value: Record<string, unknown>) => fetch(`${origin}/api/collector/batches`, { method: "POST", headers: collectorHeaders, body: JSON.stringify(value) });

const activity = (id: string, cursor: string) => ({
  id,
  participantId,
  sourceCursor: cursor,
  observedAt: fixtureObservedAt,
  kind: "assistant",
  text: "Finished the requested work.",
  captureMode: "structured",
  status: "complete",
  truncated: false,
  excerpt: "full",
  originalTextBytes: 28,
});

const collectSseIds = async (response: Response, expectedCount: number) => {
  const reader = response.body?.getReader();
  expect(reader).toBeDefined();
  const decoder = new TextDecoder();
  const ids: number[] = [];
  let pending = "";
  try {
    while (ids.length < expectedCount) {
      const next = await reader!.read();
      if (next.done) throw new Error("SSE stream ended before replay completed");
      pending += decoder.decode(next.value, { stream: true });
      const frames = pending.split("\n\n");
      pending = frames.pop() ?? "";
      for (const frame of frames) {
        const match = frame.match(/^id: ([0-9]+)$/m);
        if (match) ids.push(Number(match[1]));
      }
    }
  } finally {
    await reader!.cancel();
  }
  return ids;
};

const prepareClaimedJob = async (origin: string) => {
  const accepted = await postBatch(origin, batch());
  expect(accepted.status).toBe(201);
  const selected = await fetch(`${origin}/api/listening`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sourceId, workspaceId }),
  });
  expect(selected.status).toBe(200);
  const selection = await selected.json() as { generation: number };
  expect((await postBatch(origin, batch({ batchId: "baseline-batch", listeningGeneration: selection.generation, baselineReady: [participantId], cursors: [{ participantId, previous: "cursor-1", next: "cursor-2" }] }))).status).toBe(201);
  expect((await postBatch(origin, batch({ batchId: "activity-batch", listeningGeneration: selection.generation, baselineReady: [], cursors: [{ participantId, previous: "cursor-2", next: "cursor-3" }], activities: [activity("event-a", "cursor-3")] }))).status).toBe(201);
  const claimed = await fetch(`${origin}/api/collector/jobs/claim`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ workerId: "worker-a" }) });
  expect(claimed.status).toBe(200);
  return await claimed.json() as { jobId: string; leaseToken: string };
};

test("collector routes authenticate before parsing an invalid request", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const response = await fetch(`${runtime.origin}/api/collector/batches`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{",
    });
    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({ code: "unauthorized" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("collector routes reject unsafe and malformed bounded input after authentication", async () => {
  const runtime = await startServer("collector-test-token");
  const headers = { Authorization: "Bearer collector-test-token", "Content-Type": "application/json" };
  try {
    const command = await fetch(`${runtime.origin}/api/collector/batches`, {
      method: "POST",
      headers,
      body: JSON.stringify({ command: "herdr pane send-text", path: "/private/secret" }),
    });
    expect(command.status).toBe(400);
    expect(await command.json()).toEqual({ code: "invalid_request" });

    const wrongMediaType = await fetch(`${runtime.origin}/api/collector/batches`, {
      method: "POST",
      headers: { Authorization: "Bearer collector-test-token", "Content-Type": "text/plain" },
      body: "not-json",
    });
    expect(wrongMediaType.status).toBe(400);
    expect(await wrongMediaType.json()).toEqual({ code: "invalid_request" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("collector routes report disabled configuration without exposing state", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/collector/config?sourceId=source-a`, {
      headers: { Authorization: "Bearer any-value" },
    });
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ code: "collector_unavailable" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("public config exposes safe diagnostics without server paths or commands", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/config`);
    expect(response.status).toBe(200);
    const config = await response.json() as Record<string, unknown>;
    expect(config).not.toHaveProperty("dataDir");
    expect(config).not.toHaveProperty("collector");
    expect(JSON.stringify(config)).not.toContain(runtime.dataDirectory);
    expect(config).toHaveProperty("liveSpeechConfigured");
    expect((await fetch(`${runtime.origin}/api/health`)).status).toBe(200);
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("listening rejects an ambiguous workspace and tab selection before storage", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/listening`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sourceId: "source-a", workspaceId: "source-a:workspace", tabId: "source-a:tab" }),
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("events rejects an invalid Last-Event-ID before requesting durable replay", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/events`, { headers: { "Last-Event-ID": "not-a-sequence" } });
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("collector claim rejects an empty worker ID before requesting a durable lease", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const response = await fetch(`${runtime.origin}/api/collector/jobs/claim`, {
      method: "POST",
      headers: collectorHeaders,
      body: JSON.stringify({ workerId: "" }),
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("collector rejects non-string enums, present null scope IDs, and unknown claim fields without mutation", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "strict-bootstrap" }))).status).toBe(201);
    const before = await (await fetch(`${runtime.origin}/api/state`)).json() as { eventSequence: number; sources: Array<{ topologySequence: number }> };
    const initialTopology = batch().topology;
    const participantArrayEnum = await postBatch(runtime.origin, batch({
      batchId: "strict-participant-enum",
      topologySequence: 2,
      topology: { ...initialTopology, participants: [{ ...initialTopology.participants[0], kind: ["codex"] }] },
    }));
    expect(participantArrayEnum.status).toBe(400);
    const activityArrayEnum = await postBatch(runtime.origin, batch({
      batchId: "strict-activity-enum",
      topologySequence: 2,
      activities: [{ ...activity("strict-event", "cursor-2"), kind: ["assistant"] }],
      cursors: [{ participantId, previous: "cursor-1", next: "cursor-2" }],
    }));
    expect(activityArrayEnum.status).toBe(400);
    const nullScopeId = await fetch(`${runtime.origin}/api/listening`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sourceId, workspaceId, tabId: null }),
    });
    expect(nullScopeId.status).toBe(400);
    const unknownClaimField = await fetch(`${runtime.origin}/api/collector/jobs/claim`, {
      method: "POST",
      headers: collectorHeaders,
      body: JSON.stringify({ workerId: "worker-a", command: "synthetic", path: "/private" }),
    });
    expect(unknownClaimField.status).toBe(400);
    const after = await (await fetch(`${runtime.origin}/api/state`)).json() as { eventSequence: number; sources: Array<{ topologySequence: number }> };
    expect(after.eventSequence).toBe(before.eventSequence);
    expect(after.sources[0]?.topologySequence).toBe(before.sources[0]?.topologySequence);
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("history rejects a page size outside the bounded public range", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/history?limit=101`);
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("history rejects a noncanonical cursor before querying storage", async () => {
  const runtime = await startServer();
  try {
    const response = await fetch(`${runtime.origin}/api/history?cursor=abc`);
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally { await stopServer(runtime); }
}, 10_000);

test("topology-only bootstrap registers a source before authenticated config", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const registered = await postBatch(runtime.origin, batch({ batchId: "bootstrap-batch", cursors: [], activities: [], baselineReady: [] }));
    expect(registered.status).toBe(201);
    expect(await registered.json()).toEqual({
      batchId: "bootstrap-batch",
      acceptedEventIds: [],
      cursors: [],
      topologySequence: 1,
    });
    const config = await fetch(`${runtime.origin}/api/collector/config?sourceId=${encodeURIComponent(sourceId)}`, { headers: { Authorization: "Bearer collector-test-token" } });
    expect(config.status).toBe(200);
    expect(await config.json()).toEqual({
      sourceId,
      listeningScope: null,
      listeningGeneration: 0,
      freshness: { observedAt: fixtureObservedAt, stale: false, topologySequence: 1 },
      worker: { leaseDurationMs: 60_000, maxAttempts: 2 },
    });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("nested command fields reject a batch without changing registered topology", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "safe-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const valid = batch();
    const unsafeTopology = { ...valid.topology, workspaces: [{ ...valid.topology.workspaces[0], command: "herdr pane send-text" }] };
    const rejected = await postBatch(runtime.origin, batch({ batchId: "nested-command", topologySequence: 2, topology: unsafeTopology, cursors: [], activities: [], baselineReady: [] }));
    expect(rejected.status).toBe(400);
    const state = await fetch(`${runtime.origin}/api/state`);
    expect((await state.json() as { sources: Array<{ topologySequence: number }> }).sources[0]?.topologySequence).toBe(1);
  } finally { await stopServer(runtime); }
}, 10_000);

test("gap lifecycle activity permits empty evidence text", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const gap = { ...activity("gap-event", "cursor-gap"), kind: "lifecycle", status: "gap", text: "", originalTextBytes: 0 };
    const accepted = await postBatch(runtime.origin, batch({ batchId: "gap-batch", activities: [gap] }));
    expect(accepted.status).toBe(201);
  } finally { await stopServer(runtime); }
}, 10_000);

test("collector batches accept supported UTF-8 and escaped evidence through the shared request bound", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const multibyte = "🙂".repeat(5_000);
    expect(Buffer.byteLength(multibyte)).toBe(20_000);
    const escaped = "\u0000".repeat(20_000);
    expect(Buffer.byteLength(escaped)).toBe(20_000);
    const evidence = [multibyte, escaped, escaped, escaped].map((text, index) => ({
      ...activity(`wire-event-${index}`, `wire-cursor-${index}`),
      text,
      originalTextBytes: Buffer.byteLength(text),
    }));
    const request = batch({ batchId: "wire-bound-batch", activities: evidence });
    const encoded = JSON.stringify(request);
    expect(Buffer.byteLength(encoded)).toBeLessThanOrEqual(524_288);
    const accepted = await postBatch(runtime.origin, request);
    expect(accepted.status).toBe(201);
    expect((await accepted.json()).acceptedEventIds).toEqual(evidence.map((item) => item.id));

    const oversized = `${encoded}${" ".repeat(524_289 - Buffer.byteLength(encoded))}`;
    expect(Buffer.byteLength(oversized)).toBe(524_289);
    const rejected = await fetch(`${runtime.origin}/api/collector/batches`, { method: "POST", headers: collectorHeaders, body: oversized });
    expect(rejected.status).toBe(413);
    expect(await rejected.json()).toEqual({ code: "request_too_large" });
  } finally { await stopServer(runtime); }
}, 10_000);

test("an old accepted multibyte batch retries by receipt before fresh UTF-8 validation", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-old-public-api-"));
  let runtime: Awaited<ReturnType<typeof startServer>> | undefined;
  try {
    const oldPublicText = "界".repeat(8_000);
    expect(Buffer.byteLength(oldPublicText)).toBe(24_000);
    const legacy = batch({
      batchId: "old-public-lost-ack",
      activities: [{ ...activity("old-public-event", "old-public-cursor"), text: oldPublicText, originalTextBytes: Buffer.byteLength(oldPublicText) }],
    });
    const committed = await Effect.runPromise(Effect.scoped(openStorage(dataDirectory).pipe(Effect.flatMap((storage) => storage.ingestBatch(legacy as IngestBatch)))));
    expect(committed.kind).toBe("accepted");
    if (committed.kind !== "accepted") throw new Error("legacy precondition did not commit");
    const receipt = committed.receipt;
    const database = new Database(join(dataDirectory, "speak-now.sqlite"));
    expect(database.query("SELECT batch_id FROM batches WHERE batch_id = ?").get("old-public-lost-ack")).toEqual({ batch_id: "old-public-lost-ack" });
    database.close();
    runtime = await startServer("collector-test-token", { dataDirectory });
    const before = await (await fetch(`${runtime.origin}/api/state`)).json() as { eventSequence: number };

    const retry = await postBatch(runtime.origin, legacy);
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual(receipt);

    const changed = await postBatch(runtime.origin, {
      ...legacy,
      activities: [{ ...legacy.activities[0], text: `${oldPublicText}!`, originalTextBytes: Buffer.byteLength(`${oldPublicText}!`) }],
    });
    expect(changed.status).toBe(409);
    expect(await changed.json()).toEqual({ code: "batch_payload" });

    const fresh = await postBatch(runtime.origin, { ...legacy, batchId: "old-public-fresh-over-limit" });
    expect(fresh.status).toBe(400);
    expect(await fresh.json()).toEqual({ code: "invalid_request" });
    const after = await (await fetch(`${runtime.origin}/api/state`)).json() as { eventSequence: number };
    expect(after.eventSequence).toBe(before.eventSequence);
  } finally {
    if (runtime) await stopServer(runtime, true);
    else await rm(dataDirectory, { recursive: true, force: true });
  }
}, 10_000);

test("a persisted source accepts only an unchanged retired cursor during authoritative participant removal", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "retired-bootstrap" }))).status).toBe(201);
    const absentTopology = { ...batch().topology, workspaces: [], tabs: [], panes: [], participants: [] };
    const removal = await postBatch(runtime.origin, batch({
      batchId: "retired-removal",
      topologySequence: 2,
      topology: absentTopology,
      cursors: [{ participantId, previous: "cursor-1", next: "cursor-1" }],
      baselineReady: [],
      activities: [],
    }));
    expect(removal.status).toBe(201);
    expect(await removal.json()).toEqual({
      batchId: "retired-removal",
      acceptedEventIds: [],
      cursors: [{ participantId, cursor: "cursor-1" }],
      topologySequence: 2,
    });

    const unknown = await postBatch(runtime.origin, batch({
      batchId: "retired-unknown",
      topologySequence: 3,
      topology: absentTopology,
      cursors: [{ participantId: `${sourceNamespace}:unknown-retired`, previous: "cursor-1", next: "cursor-1" }],
      baselineReady: [],
      activities: [],
    }));
    expect(unknown.status).toBe(409);
    expect(await unknown.json()).toEqual({ code: "cursor_cas" });

    const changed = await postBatch(runtime.origin, batch({
      batchId: "retired-mutation",
      topologySequence: 3,
      topology: absentTopology,
      cursors: [{ participantId, previous: "cursor-1", next: "must-not-advance" }],
      baselineReady: [],
      activities: [],
    }));
    expect(changed.status).toBe(409);
    expect(await changed.json()).toEqual({ code: "cursor_cas" });
    const state = await (await fetch(`${runtime.origin}/api/state`)).json() as { sources: Array<{ topologySequence: number }> };
    expect(state.sources[0]?.topologySequence).toBe(2);
  } finally { await stopServer(runtime); }
}, 10_000);

test("malformed topology rejects before storage and leaves the registered source unchanged", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "valid-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const valid = batch();
    const malformedTopology = {
      ...valid.topology,
      tabs: [{ ...valid.topology.tabs[0], workspaceId: `${sourceNamespace}:missing-workspace` }],
    };
    const rejected = await postBatch(runtime.origin, batch({ batchId: "invalid-parent", topologySequence: 2, topology: malformedTopology, cursors: [], activities: [], baselineReady: [] }));
    expect(rejected.status).toBe(400);
    expect(await rejected.json()).toEqual({ code: "invalid_request" });
    const state = await fetch(`${runtime.origin}/api/state`);
    expect(state.status).toBe(200);
    const snapshot = await state.json() as { sources: Array<{ sourceId: string; topologySequence: number; stale: boolean; observedAt: string; listeningGeneration: number }> };
    expect(snapshot.sources).toContainEqual({ sourceId, topologySequence: 1, stale: false, observedAt: fixtureObservedAt, listeningGeneration: 0 });
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("batches return the original durable receipt after restart and conflict on changed retry payload", async () => {
  const first = await startServer("collector-test-token");
  const preservedDataDirectory = first.dataDirectory;
  let firstStopped = false;
  try {
    const accepted = await postBatch(first.origin, batch());
    expect(accepted.status).toBe(201);
    const receipt = await accepted.json();
    expect(receipt).toEqual({
      batchId: "batch-a",
      acceptedEventIds: [],
      cursors: [{ participantId, cursor: "cursor-1" }],
      topologySequence: 1,
    });
    first.server.kill();
    await first.server.exited;
    firstStopped = true;

    const port = 46_000 + Math.floor(Math.random() * 1_000);
    const origin = `http://localhost:${port}`;
    const restarted = Bun.spawn([Bun.which("bun")!, "src/server.ts"], {
      cwd: process.cwd(),
      stdout: "ignore",
      stderr: "pipe",
      env: { ...process.env, PORT: String(port), SPEAK_NOW_DATA_DIR: preservedDataDirectory, SPEAK_NOW_COLLECTOR_TOKEN: "collector-test-token" },
    });
    try {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        try { if ((await fetch(`${origin}/api/health`)).ok) break; } catch { /* The server is starting. */ }
        await Bun.sleep(10);
      }
      const duplicate = await postBatch(origin, batch());
      expect(duplicate.status).toBe(200);
      expect(await duplicate.json()).toEqual(receipt);
      const conflict = await postBatch(origin, batch({ cursors: [{ participantId, previous: null, next: "changed-cursor" }] }));
      expect(conflict.status).toBe(409);
      expect((await conflict.json()).code).toBe("batch_payload");
    } finally {
      restarted.kill();
      await restarted.exited;
    }
  } finally {
    if (!firstStopped) {
      first.server.kill();
      await first.server.exited;
    }
    await rm(preservedDataDirectory, { recursive: true, force: true });
  }
}, 10_000);

test("scope baseline stays silent before accepted activity creates a leased job", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch())).status).toBe(201);
    const selected = await fetch(`${runtime.origin}/api/listening`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sourceId, workspaceId }),
    });
    expect(selected.status).toBe(200);
    const selection = await selected.json() as { generation: number };
    expect(selection.generation).toBeGreaterThan(0);
    expect((await postBatch(runtime.origin, batch({ batchId: "baseline-batch", listeningGeneration: selection.generation, baselineReady: [participantId], cursors: [{ participantId, previous: "cursor-1", next: "cursor-2" }] }))).status).toBe(201);
    const empty = await fetch(`${runtime.origin}/api/collector/jobs/claim`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ workerId: "worker-a" }) });
    expect(empty.status).toBe(204);
    expect((await postBatch(runtime.origin, batch({ batchId: "activity-batch", listeningGeneration: selection.generation, baselineReady: [], cursors: [{ participantId, previous: "cursor-2", next: "cursor-3" }], activities: [activity("event-a", "cursor-3")] }))).status).toBe(201);
    const claimed = await fetch(`${runtime.origin}/api/collector/jobs/claim`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ workerId: "worker-a" }) });
    expect(claimed.status).toBe(200);
    const job = await claimed.json() as { jobId: string; leaseToken: string; leaseDurationMs?: number };
    expect(job.jobId).toEqual(expect.any(String));
    expect(job.leaseToken).toEqual(expect.any(String));
    expect(job.leaseDurationMs).toBe(60_000);
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("completion rejects extra nested fields without history mutation, then accepts the closed result", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const job = await prepareClaimedJob(runtime.origin);
    const unsafe = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, {
      method: "POST",
      headers: collectorHeaders,
      body: JSON.stringify({ leaseToken: job.leaseToken, resultKey: "unsafe", result: { speak: false, kind: "progress", text: "safe summary", evidenceEventIds: ["event-a"], command: "synthetic", path: "/private", rawEvidence: "not-history" } }),
    });
    const history = await fetch(`${runtime.origin}/api/history`);
    expect(unsafe.status).toBe(400);
    expect((await history.json() as { results: unknown[] }).results).toEqual([]);
    const accepted = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, {
      method: "POST",
      headers: collectorHeaders,
      body: JSON.stringify({ leaseToken: job.leaseToken, resultKey: "closed", result: { speak: false, kind: "progress", text: "safe summary", evidenceEventIds: ["event-a"] } }),
    });
    expect(accepted.status).toBe(201);
  } finally { await stopServer(runtime); }
}, 10_000);

test("completion accepts the summarizer's valid silent result", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const job = await prepareClaimedJob(runtime.origin);
    const response = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, {
      method: "POST",
      headers: collectorHeaders,
      body: JSON.stringify({ leaseToken: job.leaseToken, resultKey: "silent", result: { speak: false, kind: "progress", text: "", evidenceEventIds: ["event-a"] } }),
    });
    expect(response.status).toBe(201);
  } finally { await stopServer(runtime); }
}, 10_000);

test("batch rejects malformed shared DTO fields without changing durable topology", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "dto-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const malformed = [
      batch({ batchId: "dto-label", topologySequence: 2, topology: { ...batch().topology, workspaces: [{ ...batch().topology.workspaces[0], label: { unsafe: true } }] }, cursors: [], activities: [], baselineReady: [] }),
      batch({ batchId: "dto-participant", topologySequence: 2, topology: { ...batch().topology, participants: [{ ...batch().topology.participants[0], kind: "invented-agent", generation: -1 }] }, cursors: [], activities: [], baselineReady: [] }),
      batch({ batchId: "dto-activity", topologySequence: 2, cursors: [], baselineReady: [], activities: [{ ...activity("invalid-activity", "cursor-invalid"), captureMode: "unknown", originalTextBytes: -1 }] }),
    ];
    for (const value of malformed) expect((await postBatch(runtime.origin, value)).status).toBe(400);
    const state = await fetch(`${runtime.origin}/api/state`);
    expect((await state.json() as { sources: Array<{ topologySequence: number }> }).sources[0]?.topologySequence).toBe(1);
  } finally { await stopServer(runtime); }
}, 10_000);

test("foreign Origin listening mutation is rejected without changing scope or event sequence", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "origin-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const before = await (await fetch(`${runtime.origin}/api/state`)).json() as { scope: unknown; eventSequence: number };
    const rejected = await fetch(`${runtime.origin}/api/listening`, {
      method: "PUT",
      headers: { "Content-Type": "application/json", Origin: "http://untrusted.invalid" },
      body: JSON.stringify({ sourceId, workspaceId }),
    });
    expect(rejected.status).toBe(403);
    const after = await (await fetch(`${runtime.origin}/api/state`)).json() as { scope: unknown; eventSequence: number };
    expect(after.scope).toEqual(before.scope);
    expect(after.eventSequence).toBe(before.eventSequence);
  } finally { await stopServer(runtime); }
}, 10_000);

test("a request-time SQLite failure reports once with redacted collector context", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const database = new Database(join(runtime.dataDirectory, "speak-now.sqlite"));
    database.run("DROP TABLE batches");
    database.close();
    const failed = await fetch(`${runtime.origin}/api/collector/batches`, {
      method: "POST",
      headers: { ...collectorHeaders, traceparent: "00-11111111111111111111111111111111-2222222222222222-01" },
      body: JSON.stringify(batch({ batchId: "diagnostic-batch", cursors: [], activities: [], baselineReady: [] })),
    });
    expect(failed.status).toBe(500);
    const lines = await settledReports(runtime.dataDirectory);
    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain("diagnostic-batch");
    expect(lines[0]).toContain("11111111111111111111111111111111");
    expect(lines[0]).not.toContain("collector-test-token");
    expect(lines[0]).not.toContain("sourceEpoch");
    expect((await fetch(`${runtime.origin}/api/health`)).status).toBe(200);
  } finally { await stopServer(runtime); }
}, 10_000);

test("storage failure diagnostics use the emitted span IDs with and without an incoming parent", async () => {
  for (const traceparent of ["00-11111111111111111111111111111111-2222222222222222-01", undefined]) {
    const runtime = await startServer("collector-test-token");
    try {
      const database = new Database(join(runtime.dataDirectory, "speak-now.sqlite"));
      database.run("DROP TABLE batches");
      database.close();
      const failed = await fetch(`${runtime.origin}/api/collector/batches`, {
        method: "POST",
        headers: { ...collectorHeaders, ...(traceparent ? { traceparent } : {}) },
        body: JSON.stringify(batch({ batchId: traceparent ? "span-parent" : "span-originless", cursors: [], activities: [], baselineReady: [] })),
      });
      expect(failed.status).toBe(500);
      await Bun.sleep(100);
      const traceRecord = (await jsonLines(join(runtime.dataDirectory, "traces"))).find((line) => line.operation === "server.collector.receipt_lookup" && line.outcome === "failed");
      const errorRecord = (await jsonLines(join(runtime.dataDirectory, "errors"))).find((line) => line.operation === "server.collector.receipt_lookup" && "reportId" in line);
      const logRecord = (await jsonLines(join(runtime.dataDirectory, "logs"))).find((line) => line.operation === "server.collector.receipt_lookup" && line.message === "Durable storage request failed");
      expect(traceRecord).toBeDefined();
      expect(errorRecord).toBeDefined();
      expect(logRecord).toBeDefined();
      expect(errorRecord).toMatchObject({ traceId: traceRecord!.traceId, spanId: traceRecord!.spanId });
      expect(logRecord).toMatchObject({ traceId: traceRecord!.traceId, spanId: traceRecord!.spanId });
      expect(traceRecord!.spanId).not.toBe("2222222222222222");
      if (traceparent) expect(traceRecord!.parentSpanId).toBe("2222222222222222");
      else {
        expect(traceRecord!.traceId).toMatch(/^[a-f0-9]{32}$/i);
        expect(traceRecord!.spanId).toMatch(/^[a-f0-9]{16}$/i);
      }
    } finally {
      await stopServer(runtime);
    }
  }
}, 20_000);

test("a post-header SSE SQLite failure reports once and ends with a fixed SSE error", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "sse-diagnostic-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const stream = await fetch(`${runtime.origin}/api/events`, { headers: { "Last-Event-ID": "0" } });
    expect(stream.status).toBe(200);
    const reader = stream.body?.getReader();
    expect(reader).toBeDefined();
    await reader!.read();
    const database = new Database(join(runtime.dataDirectory, "speak-now.sqlite"));
    database.run("DROP TABLE durable_events");
    database.close();
    const next = await reader!.read();
    expect(new TextDecoder().decode(next.value)).toContain("event: error");
    expect(next.done).toBe(false);
    expect((await reader!.read()).done).toBe(true);
    const lines = await settledReports(runtime.dataDirectory);
    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toContain("collector-test-token");
    expect((await fetch(`${runtime.origin}/api/health`)).status).toBe(200);
  } finally { await stopServer(runtime); }
}, 10_000);

test("a raw SSE disconnect interrupts polling before a later storage fault", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "raw-close-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const port = Number(new URL(runtime.origin).port);
    await new Promise<void>((resolve, reject) => {
      const socket = connect({ host: "127.0.0.1", port });
      let received = "";
      const timeout = setTimeout(() => { socket.destroy(); reject(new Error("SSE bootstrap did not arrive")); }, 2_000);
      socket.setEncoding("utf8");
      socket.once("error", reject);
      socket.on("data", (chunk: string) => {
        received += chunk;
        if (received.includes("event: ingested")) {
          clearTimeout(timeout);
          socket.destroy();
        }
      });
      socket.once("close", () => { clearTimeout(timeout); resolve(); });
      socket.write(`GET /api/events HTTP/1.1\r\nHost: localhost:${port}\r\nLast-Event-ID: 0\r\nConnection: keep-alive\r\n\r\n`);
    });
    await Bun.sleep(20);
    const database = new Database(join(runtime.dataDirectory, "speak-now.sqlite"));
    database.run("DROP TABLE durable_events");
    database.close();
    await Bun.sleep(250);
    expect(await reports(runtime.dataDirectory)).toEqual([]);
    expect((await fetch(`${runtime.origin}/api/health`)).status).toBe(200);
  } finally { await stopServer(runtime); }
}, 10_000);

test("SIGTERM settles an owned idle SSE request before the server exits without a cancellation report", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "shutdown-sse-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const state = await (await fetch(`${runtime.origin}/api/state`)).json() as { eventSequence: number };
    const settled = new Promise<"closed" | "error">((resolve) => {
      const stream = httpRequest(`${runtime.origin}/api/events`, { headers: { "Last-Event-ID": String(state.eventSequence) } }, (response) => {
        response.resume();
        response.once("close", () => resolve("closed"));
      });
      stream.once("error", () => resolve("error"));
      stream.end();
    });
    await Bun.sleep(150);
    runtime.server.kill("SIGTERM");
    await Promise.race([runtime.server.exited, Bun.sleep(2_000).then(() => { throw new Error("server did not drain active SSE work"); })]);
    expect(runtime.server.exitCode).toBe(0);
    expect(await settled).toBe("closed");
    expect(await reports(runtime.dataDirectory)).toEqual([]);
  } finally {
    if (runtime.server.exitCode === null) runtime.server.kill();
    await runtime.server.exited;
    await rm(runtime.dataDirectory, { recursive: true, force: true });
  }
}, 10_000);

test("occupied listener exits with one safe startup report and no ready signal", async () => {
  const occupied = createServer((_request, response) => response.end());
  await new Promise<void>((resolve) => occupied.listen(0, resolve));
  const port = (occupied.address() as { port: number }).port;
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-server-listen-"));
  const child = Bun.spawn([Bun.which("bun")!, "src/server.ts"], {
    cwd: process.cwd(),
    stdout: "pipe",
    stderr: "pipe",
    env: { ...process.env, PORT: String(port), SPEAK_NOW_DATA_DIR: dataDirectory, SPEAK_NOW_COLLECTOR_TOKEN: "collector-test-token" },
  });
  try {
    await Promise.race([child.exited, Bun.sleep(2_000).then(() => { throw new Error("occupied listener did not exit"); })]);
    expect(child.exitCode).toBe(1);
    expect(await new Response(child.stdout).text()).not.toContain("Speak Now is running");
    const lines = await settledReports(dataDirectory);
    expect(lines).toHaveLength(1);
    expect(lines[0]).not.toContain("collector-test-token");
  } finally {
    if (child.exitCode === null) child.kill();
    await child.exited;
    await new Promise<void>((resolve) => occupied.close(() => resolve()));
    await rm(dataDirectory, { recursive: true, force: true });
  }
}, 10_000);

test("job completion fences stale lease tokens and preserves an accepted result receipt", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    const job = await prepareClaimedJob(runtime.origin);
    const result = { resultKey: "result-a", result: { speak: false, kind: "progress", text: "Work is complete.", evidenceEventIds: ["event-a"] } };
    const stale = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ ...result, leaseToken: "stale-token" }) });
    expect(stale.status).toBe(409);
    expect((await stale.json()).code).toBe("lease_token");
    const accepted = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ ...result, leaseToken: job.leaseToken }) });
    expect(accepted.status).toBe(201);
    const receipt = await accepted.json();
    const duplicate = await fetch(`${runtime.origin}/api/collector/jobs/${job.jobId}/result`, { method: "POST", headers: collectorHeaders, body: JSON.stringify({ ...result, leaseToken: job.leaseToken }) });
    expect(duplicate.status).toBe(200);
    expect(await duplicate.json()).toEqual(receipt);
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("state cursor bridges to SSE replay after a committed scope change", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch())).status).toBe(201);
    const state = await fetch(`${runtime.origin}/api/state`);
    expect(state.status).toBe(200);
    const snapshot = await state.json() as { eventSequence: number };
    expect(snapshot.eventSequence).toEqual(expect.any(Number));
    const selection = await fetch(`${runtime.origin}/api/listening`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sourceId, workspaceId }),
    });
    expect(selection.status).toBe(200);
    const events = await fetch(`${runtime.origin}/api/events`, { headers: { "Last-Event-ID": String(snapshot.eventSequence) }, signal: AbortSignal.timeout(1_000) });
    expect(events.status).toBe(200);
    expect(events.headers.get("content-type")).toContain("text/event-stream");
    const reader = events.body?.getReader();
    expect(reader).toBeDefined();
    const next = await reader!.read();
    await reader!.cancel();
    const frame = new TextDecoder().decode(next.value);
    expect(frame).toMatch(/^id: [1-9][0-9]*\n/m);
    expect(frame).toContain("event:");
  } finally {
    await stopServer(runtime);
  }
}, 10_000);

test("SSE rejects a cursor beyond the durable state high water", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "future-cursor-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const state = await fetch(`${runtime.origin}/api/state`);
    const snapshot = await state.json() as { eventSequence: number };
    const response = await fetch(`${runtime.origin}/api/events`, { headers: { "Last-Event-ID": String(snapshot.eventSequence + 1) }, signal: AbortSignal.timeout(1_000) });
    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({ code: "invalid_request" });
  } finally { await stopServer(runtime); }
}, 10_000);

test("SSE replay advances across durable event pages without skipping committed sequences", async () => {
  const runtime = await startServer("collector-test-token");
  try {
    expect((await postBatch(runtime.origin, batch({ batchId: "sse-bootstrap", cursors: [], activities: [], baselineReady: [] }))).status).toBe(201);
    const state = await fetch(`${runtime.origin}/api/state`);
    expect(state.status).toBe(200);
    const snapshot = await state.json() as { eventSequence: number };
    const changes = 130;
    for (let index = 0; index < changes; index += 1) {
      const selection = index % 2 === 0 ? { sourceId, workspaceId } : null;
      const changed = await fetch(`${runtime.origin}/api/listening`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(selection),
      });
      expect(changed.status).toBe(200);
    }
    const events = await fetch(`${runtime.origin}/api/events`, {
      headers: { "Last-Event-ID": String(snapshot.eventSequence) },
      signal: AbortSignal.timeout(5_000),
    });
    expect(events.status).toBe(200);
    const ids = await collectSseIds(events, changes);
    expect(ids).toEqual(Array.from({ length: changes }, (_, index) => snapshot.eventSequence + index + 1));
  } finally {
    await stopServer(runtime);
  }
}, 15_000);
