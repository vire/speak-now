import { expect, test } from "bun:test";
import { appendFile, chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { COLLECTOR_WIRE_LIMITS, encodeCollectorJson } from "./shared";

const responseItem = (id: string, text: string) => JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } }) + "\n";

test("collector atomically packs one multi-participant capture group into bounded FIFO envelopes", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-outbox-pack-"));
    const data = join(root, "data");
    const captureMarker = join(root, "capture-marker");
  const transcripts = Array.from({ length: 19 }, (_, index) => join(root, "source-" + index + ".jsonl"));
  const received: string[] = [];
  let rejected = 0;
  let bootstrapped = false;
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/config") {
        if (!bootstrapped) return Response.json({ code: "unknown_source" }, { status: 404 });
        return Response.json({ sourceId: "fixture:herdr", listeningScope: { workspaceId: "fixture:workspace" }, listeningGeneration: 4 });
      }
      if (url.pathname === "/api/collector/jobs/claim") return new Response(null, { status: 204 });
      if (url.pathname !== "/api/collector/batches") return Response.json({ code: "missing" }, { status: 404 });
      const raw = await request.text();
      received.push(raw);
      const body = JSON.parse(raw) as { batchId: string; listeningGeneration: number; activities: Array<{ id: string }>; cursors: Array<{ participantId: string; next: string }>; topologySequence: number };
      if (body.listeningGeneration === 0) bootstrapped = true;
      if (Buffer.byteLength(raw) > COLLECTOR_WIRE_LIMITS.requestBytes) {
        rejected += 1;
        return Response.json({ code: "request_too_large" }, { status: 413 });
      }
      return Response.json({
        batchId: body.batchId,
        acceptedEventIds: body.activities.map((activity) => activity.id),
        cursors: body.cursors.map(({ participantId, next }) => ({ participantId, cursor: next })),
        topologySequence: body.topologySequence,
      }, { status: 201 });
    },
  });
  try {
    await mkdir(data);
    const messages = Array.from({ length: 3 }, (_, index) => responseItem("event-" + index, "\u0001".repeat(8_000))).join("");
    for (const [index, transcript] of transcripts.entries()) {
      await writeFile(transcript, JSON.stringify({ type: "session_meta", payload: { id: "session-" + index } }) + "\n" + responseItem("baseline", "baseline"));
    }
    const snapshot = JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }],
      tabs: [{ tab_id: "tab", workspace_id: "workspace", label: "Tab" }],
      panes: transcripts.map((_, index) => ({ pane_id: "pane-" + index, tab_id: "tab", terminal_id: "terminal-" + index })),
      agents: transcripts.map((_, index) => ({ agent: "codex", pane_id: "pane-" + index, terminal_id: "terminal-" + index, agent_session: { kind: "id", source: "fixture", value: "session-" + index } })),
    } } });
    const bin = join(root, "bin");
    await mkdir(bin);
    await writeFile(join(root, "snapshot.json"), snapshot);
    await writeFile(join(root, "paths"), transcripts.join("\n") + "\n");
    await writeFile(join(root, "messages"), messages);
    await writeFile(join(bin, "herdr"), "#!/bin/sh\n/bin/cat " + join(root, "snapshot.json") + "\n");
    await writeFile(join(bin, "rg"), "#!/bin/sh\nprintf '%s\\n' capture >> " + captureMarker + "\n/bin/cat " + join(root, "paths") + "\n");
    await writeFile(join(bin, "claude"), "#!/bin/sh\nexit 91\n");
    for (const command of ["herdr", "rg", "claude"]) await chmod(join(bin, command), 0o755);
    const script = [
      'import { appendFile, readFile } from "node:fs/promises";',
      'import { initialState, runOnce } from "./src/collector.ts";',
      "const config = { sourceNamespace: " + JSON.stringify("fixture") + ", dataDir: " + JSON.stringify(data) + ", excludePaneIds: [], collectorUrl: " + JSON.stringify(server.url.origin) + ", collectorToken: " + JSON.stringify("fixture-token") + ", workerId: " + JSON.stringify("fixture-worker") + " };",
      "const state = initialState();",
      "await runOnce(config, state);",
      "const messages = await readFile(" + JSON.stringify(join(root, "messages")) + ", \"utf8\");",
      "for (const path of " + JSON.stringify(transcripts) + ") await appendFile(path, messages);",
      "await runOnce(config, state);",
      "const stagedPending = state.remote.pendingBatches.length;",
      "await appendFile(" + JSON.stringify(transcripts[0]) + ", " + JSON.stringify(responseItem("late-event", "late while prior FIFO chunks remain")) + ");",
      "await runOnce(config, state);",
      "for (let index = 0; index < 8 && state.remote.pendingBatches.length; index += 1) await runOnce(config, state);",
      "console.log(JSON.stringify({ pending: state.remote.pendingBatches.length, stagedPending }));",
    ].join("\n");
    const child = Bun.spawn([Bun.which("bun")!, "--no-env-file", "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: bin + ":/usr/bin:/bin", HOME: join(root, "home"), TMPDIR: root, XDG_CONFIG_HOME: join(root, "config"), XDG_CACHE_HOME: join(root, "cache"), SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect((await readFile(captureMarker, "utf8")).trim().split("\n")).toHaveLength(57);
    expect(rejected).toBe(0);
    expect(received.every((body) => Buffer.byteLength(body) <= COLLECTOR_WIRE_LIMITS.requestBytes)).toBe(true);
    const activityBodies = received.map((body) => JSON.parse(body) as { activities: Array<{ id: string; text: string }> }).filter((body) => body.activities.length > 0);
    expect(activityBodies.length).toBeGreaterThan(2);
    const allActivities = activityBodies.flatMap((body) => body.activities);
    const initialActivities = allActivities.filter((activity) => activity.id !== "late-event");
    expect(Buffer.byteLength(encodeCollectorJson({ ...activityBodies[0], activities: initialActivities }))).toBeGreaterThan(COLLECTOR_WIRE_LIMITS.requestBytes);
    expect(allActivities).toHaveLength(58);
    expect(allActivities.map((activity) => activity.text)).toContain("late while prior FIFO chunks remain");
    expect(JSON.parse(stdout)).toEqual({ pending: 0, stagedPending: expect.any(Number) });
    expect(JSON.parse(stdout).stagedPending).toBeGreaterThan(1);
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);

test("collector omits retired cursors and silently tails an exact-ID return after authoritative removal", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-outbox-retired-"));
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  let bootstrapped = false;
  let invalidCursorBodies = 0;
  const publishedTexts: string[] = [];
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/config") {
        if (!bootstrapped) return Response.json({ code: "unknown_source" }, { status: 404 });
        return Response.json({ sourceId: "fixture:herdr", listeningScope: { workspaceId: "fixture:workspace" }, listeningGeneration: 4 });
      }
      if (url.pathname === "/api/collector/jobs/claim") return new Response(null, { status: 204 });
      if (url.pathname !== "/api/collector/batches") return Response.json({ code: "missing" }, { status: 404 });
      const body = await request.json() as { batchId: string; listeningGeneration: number; activities: Array<{ id: string; text: string }>; cursors: Array<{ participantId: string; next: string }>; topology: { participants: Array<{ id: string }> }; topologySequence: number };
      if (body.listeningGeneration === 0) bootstrapped = true;
      if (body.cursors.some((cursor) => !body.topology.participants.some((participant) => participant.id === cursor.participantId))) {
        invalidCursorBodies += 1;
        return Response.json({ code: "invalid_request" }, { status: 400 });
      }
      publishedTexts.push(...body.activities.map((activity) => activity.text));
      return Response.json({
        batchId: body.batchId,
        acceptedEventIds: body.activities.map((activity) => activity.id),
        cursors: body.cursors.map(({ participantId, next }) => ({ participantId, cursor: next })),
        topologySequence: body.topologySequence,
      }, { status: 201 });
    },
  });
  try {
    await mkdir(data);
    await writeFile(transcript, JSON.stringify({ type: "session_meta", payload: { id: "session-a" } }) + "\n" + responseItem("baseline", "baseline"));
    const snapshot = JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }],
      tabs: [{ tab_id: "tab", workspace_id: "workspace", label: "Tab" }],
      panes: [{ pane_id: "pane-a", tab_id: "tab", terminal_id: "terminal-a" }],
      agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }],
    } } });
    const bin = join(root, "bin");
    await mkdir(bin);
    await writeFile(join(root, "snapshot.json"), snapshot);
    await writeFile(join(root, "paths"), transcript + "\n");
    await writeFile(join(bin, "herdr"), "#!/bin/sh\n/bin/cat " + join(root, "snapshot.json") + "\n");
    await writeFile(join(bin, "rg"), "#!/bin/sh\n/bin/cat " + join(root, "paths") + "\n");
    await writeFile(join(bin, "claude"), "#!/bin/sh\nexit 91\n");
    for (const command of ["herdr", "rg", "claude"]) await chmod(join(bin, command), 0o755);
    const script = [
      'import { appendFile, readFile, writeFile } from "node:fs/promises";',
      'import { initialState, restore, runOnce } from "./src/collector.ts";',
      "const config = { sourceNamespace: " + JSON.stringify("fixture") + ", dataDir: " + JSON.stringify(data) + ", excludePaneIds: [], collectorUrl: " + JSON.stringify(server.url.origin) + ", collectorToken: " + JSON.stringify("fixture-token") + ", workerId: " + JSON.stringify("fixture-worker") + " };",
      "const state = initialState();",
      "await runOnce(config, state);",
      "await appendFile(" + JSON.stringify(transcript) + ", " + JSON.stringify(responseItem("before-exit", "visible before authoritative removal")) + ");",
      "await runOnce(config, state);",
      "const path = " + JSON.stringify(join(root, "snapshot.json")) + ";",
      'const snapshot = JSON.parse(await readFile(path, "utf8"));',
      "const panes = snapshot.result.snapshot.panes; const agents = snapshot.result.snapshot.agents;",
      "snapshot.result.snapshot.panes = []; snapshot.result.snapshot.agents = [];",
      "await writeFile(path, JSON.stringify(snapshot));",
      "await runOnce(config, state);",
      "await appendFile(" + JSON.stringify(transcript) + ", " + JSON.stringify(responseItem("while-absent", "must become the exact return baseline")) + ");",
      "snapshot.result.snapshot.panes = panes; snapshot.result.snapshot.agents = agents;",
      "await writeFile(path, JSON.stringify(snapshot));",
      "const returned = await restore(config.dataDir);",
      "await runOnce(config, returned);",
      "await appendFile(" + JSON.stringify(transcript) + ", " + JSON.stringify(responseItem("after-return", "visible after exact return")) + ");",
      "await runOnce(config, returned);",
      "console.log(JSON.stringify({ pending: returned.remote.pendingBatches.length }));",
    ].join("\n");
    const child = Bun.spawn([Bun.which("bun")!, "--no-env-file", "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: bin + ":/usr/bin:/bin", HOME: join(root, "home"), TMPDIR: root, XDG_CONFIG_HOME: join(root, "config"), XDG_CACHE_HOME: join(root, "cache"), SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(invalidCursorBodies).toBe(0);
    expect(publishedTexts).toEqual(["visible before authoritative removal", "visible after exact return"]);
    expect(JSON.parse(stdout)).toEqual({ pending: 0 });
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);

test("collector preserves an oversized saved sender image without posting or advancing capture", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-outbox-blocked-"));
  const data = join(root, "data");
  const bin = join(root, "bin");
  let batchPosts = 0;
  const server = Bun.serve({
    port: 0,
    fetch: (request) => {
      if (new URL(request.url).pathname === "/api/collector/batches") batchPosts += 1;
      return Response.json({ code: "unexpected" }, { status: 500 });
    },
  });
  try {
    await mkdir(data);
    await mkdir(bin);
    const raw = JSON.stringify({ batchId: "legacy-over-limit", padding: "x".repeat(COLLECTOR_WIRE_LIMITS.requestBytes) });
    const saved = {
      cursors: { "fixture:session-a": { participantId: "fixture:session-a", sourceCursor: "preserved-cursor", offset: 42, initialized: true } },
      files: {}, activityIds: [], jobs: [], sourceStatuses: [],
      remote: { pendingBatches: [{ batchId: "legacy-over-limit", body: { batchId: "legacy-over-limit" }, encoded: raw, topologyDigest: "legacy" }], acknowledgedCursors: {}, baselineGenerations: {}, topologySequence: 9, sourceEpoch: "fixture-epoch", bootstrapped: true },
    };
    await writeFile(join(data, "collector-state.json"), JSON.stringify(saved));
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }], tabs: [{ tab_id: "tab", workspace_id: "workspace", label: "Tab" }],
      panes: [{ pane_id: "pane-a", tab_id: "tab", terminal_id: "terminal-a" }],
      agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }],
    } } }));
    await writeFile(join(bin, "herdr"), "#!/bin/sh\n/bin/cat " + join(root, "snapshot.json") + "\n");
    await writeFile(join(bin, "rg"), "#!/bin/sh\nexit 91\n");
    for (const command of ["herdr", "rg"]) await chmod(join(bin, command), 0o755);
    const script = [
      'import { restore, runOnce } from "./src/collector.ts";',
      "const state = await restore(" + JSON.stringify(data) + ");",
      "await runOnce({ sourceNamespace: \"fixture\", dataDir: " + JSON.stringify(data) + ", excludePaneIds: [], collectorUrl: " + JSON.stringify(server.url.origin) + ", collectorToken: \"fixture-token\", workerId: \"fixture-worker\", sourceEpoch: \"fixture-epoch\" }, state);",
      "console.log(JSON.stringify({ pending: state.remote.pendingBatches.length, encoded: state.remote.pendingBatches[0]?.encoded, cursor: state.cursors[\"fixture:session-a\"]?.sourceCursor, blocked: state.sourceStatuses.some((status) => status.kind === \"overflow\") }));",
    ].join("\n");
    const child = Bun.spawn([Bun.which("bun")!, "--no-env-file", "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: bin + ":/usr/bin:/bin", HOME: join(root, "home"), TMPDIR: root, XDG_CONFIG_HOME: join(root, "config"), XDG_CACHE_HOME: join(root, "cache") } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(batchPosts).toBe(0);
    expect(JSON.parse(stdout)).toEqual({ pending: 1, encoded: raw, cursor: "preserved-cursor", blocked: true });
    const restored = JSON.parse(await readFile(join(data, "collector-state.json"), "utf8"));
    expect(restored.remote.pendingBatches[0].encoded).toBe(raw);
    expect(restored.sourceStatuses.some((status: { kind: string }) => status.kind === "overflow")).toBe(true);
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
