import { expect, test } from "bun:test";
import { appendFile, chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("collector retries one immutable acknowledged batch after restart and completes a leased fixture job", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-transport-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  const batches: Array<{ authorization: string | null; body: Record<string, unknown> }> = [];
  const results: Array<{ path: string; body: Record<string, unknown> }> = [];
  const operations: string[] = [];
  let failedBatchId: string | undefined;
  let mismatchedBatchId: string | undefined;
  let wrongReceiptBatchId: string | undefined;
  let claimed = false;
  let bootstrapped = false;
  const idleJobFlag = join(root, "offer-idle-job");
  const evidence = [{ id: "event-a", participantId: "fixture:[\"session\",\"codex\",\"id\",\"fixture\",\"session-a\"]:0", sourceCursor: "1", observedAt: "2026-10-06T00:00:00.000Z", kind: "assistant", text: "fixture update", captureMode: "structured", status: "complete", truncated: false, excerpt: "full", originalTextBytes: 14 }];
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/config" && request.method === "GET") {
        operations.push("config");
        if (!bootstrapped) return Response.json({ code: "unknown_source" }, { status: 404 });
        return Response.json({
          sourceId: "fixture:herdr",
          listeningScope: { workspaceId: "fixture:workspace-a" },
          listeningGeneration: 5,
          worker: { retryBudget: 1 },
        });
      }
      if (url.pathname === "/api/collector/batches" && request.method === "POST") {
        operations.push("batch");
        const body = await request.json() as Record<string, unknown>;
        batches.push({ authorization: request.headers.get("authorization"), body });
        if (body.listeningGeneration === 0 && Array.isArray(body.activities) && body.activities.length === 0 && Array.isArray(body.cursors) && body.cursors.length === 0 && Array.isArray(body.baselineReady) && body.baselineReady.length === 0) bootstrapped = true;
        const batchId = String(body.batchId);
        const activities = body.activities as unknown[];
        if (activities.length > 0 && failedBatchId === undefined) {
          failedBatchId = batchId;
          return Response.json({ code: "temporarily_unavailable" }, { status: 503 });
        }
        if (activities.length > 0 && mismatchedBatchId === undefined) {
          mismatchedBatchId = batchId;
          return Response.json({ batchId, acceptedEventIds: activities.map((activity) => (activity as { id: string }).id), cursors: [{ participantId: "wrong", cursor: "wrong" }], topologySequence: body.topologySequence }, { status: 201 });
        }
        if (activities.length > 0 && wrongReceiptBatchId === undefined) {
          wrongReceiptBatchId = batchId;
          return Response.json({ batchId: batchId + "-wrong", acceptedEventIds: activities.map((activity) => (activity as { id: string }).id), cursors: (body.cursors as Array<{ participantId: string; next: string }>).map(({ participantId, next }) => ({ participantId, cursor: next })), topologySequence: body.topologySequence }, { status: 201 });
        }
        return Response.json({
          batchId,
          acceptedEventIds: activities.map((activity) => (activity as { id: string }).id),
          cursors: (body.cursors as Array<{ participantId: string; next: string }>).map(({ participantId, next }) => ({ participantId, cursor: next })),
          topologySequence: body.topologySequence,
        }, { status: 201 });
      }
      if (url.pathname === "/api/collector/jobs/claim" && request.method === "POST") {
        if (!failedBatchId) return new Response(null, { status: 204 });
        if (claimed && !(await Bun.file(idleJobFlag).exists())) return new Response(null, { status: 204 });
        claimed = true;
        return Response.json({
          jobId: (await Bun.file(idleJobFlag).exists()) ? "job-idle" : "job-a",
          attempt: 1,
          leaseToken: "lease-a",
          leaseDurationMs: 60_000,
          expiresAt: "2030-01-01T00:00:00.000Z",
          sourceId: "fixture:herdr",
          participantId: "fixture:[\"session\",\"codex\",\"id\",\"fixture\",\"session-a\"]:0",
          scopeGeneration: 5,
          evidenceEventIds: ["event-a"],
          evidence,
        });
      }
      if ((url.pathname === "/api/collector/jobs/job-a/result" || url.pathname === "/api/collector/jobs/job-idle/result") && request.method === "POST") {
        results.push({ path: url.pathname, body: await request.json() as Record<string, unknown> });
        return Response.json({ announcementId: "announcement-a" }, { status: 201 });
      }
      return Response.json({ code: "missing" }, { status: 404 });
    },
  });
  try {
    await mkdir(bin);
    await mkdir(data);
    const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline output")}`);
    await writeFile(join(root, "paths"), `${transcript}\n`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace-a", label: "Workspace" }],
      tabs: [{ tab_id: "tab-a", workspace_id: "workspace-a", label: "Tab" }],
      panes: [
        { pane_id: "pane-a", tab_id: "tab-a", terminal_id: "terminal-a" },
        { pane_id: "worker-pane", tab_id: "tab-a", terminal_id: "terminal-worker" },
      ],
      agents: [
        { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } },
        { agent: "codex", pane_id: "worker-pane", terminal_id: "terminal-worker", agent_session: { kind: "id", source: "fixture", value: "worker-session" } },
      ],
    } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/cat ${join(root, "paths")}\n`);
    await writeFile(join(bin, "claude"), "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"system\",\"tools\":[],\"mcp_servers\":[]}' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"{\\\"speak\\\":false,\\\"kind\\\":\\\"progress\\\",\\\"text\\\":\\\"fixture summary\\\",\\\"evidenceEventIds\\\":[\\\"event-a\\\"]}\"}'\n");
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);
    const script = `
      import { appendFile, readFile, writeFile } from "node:fs/promises";
      import { initialState, restore, runOnce } from "./src/collector.ts";
      const config = {
        sourceNamespace: "fixture",
        dataDir: ${JSON.stringify(data)},
        excludePaneIds: ["raw:worker-pane"],
        collectorUrl: ${JSON.stringify(server.url.origin)},
        collectorToken: "fixture-token",
        workerId: "fixture-worker",
      };
      const state = initialState();
      await runOnce(config, state);
      await appendFile(${JSON.stringify(transcript)}, ${JSON.stringify(message("event-a", "published update"))});
      await runOnce(config, state);
      const restarted = await restore(config.dataDir);
      await runOnce(config, restarted);
      await runOnce(config, restarted);
      const snapshotPath = ${JSON.stringify(join(root, "snapshot.json"))};
      const snapshot = JSON.parse(await readFile(snapshotPath, "utf8"));
      snapshot.result.snapshot.tabs.push({ tab_id: "empty-tab", workspace_id: "workspace-a", label: "Empty" });
      await writeFile(snapshotPath, JSON.stringify(snapshot));
      await runOnce(config, restarted);
      restarted.remote.lastTopologyPublishedAt = "1970-01-01T00:00:00.000Z";
      await runOnce(config, restarted);
      await writeFile(${JSON.stringify(idleJobFlag)}, "ready");
      await runOnce(config, restarted);
      console.log(JSON.stringify({ cursor: restarted.cursors[Object.keys(restarted.cursors)[0]]?.sourceCursor }));
    `;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout).cursor).toBeDefined();

    expect(operations.slice(0, 2)).toEqual(["batch", "config"]);
    expect(batches[0]).toEqual(expect.objectContaining({ authorization: "Bearer fixture-token", body: expect.objectContaining({ sourceId: "fixture:herdr", listeningGeneration: 0, activities: [], cursors: [], baselineReady: [], topologySequence: 1 }) }));
    expect(batches).toContainEqual(expect.objectContaining({ body: expect.objectContaining({ listeningGeneration: 5, activities: [], baselineReady: [expect.any(String)], cursors: [expect.objectContaining({ previous: null, next: expect.any(String) })] }) }));
    const updateBatches = batches.filter(({ body }) => Array.isArray(body.activities) && body.activities.length > 0);
    expect(updateBatches).toHaveLength(4);
    expect(updateBatches[0]).toEqual(updateBatches[1]);
    expect(updateBatches[0]).toEqual(updateBatches[2]);
    expect(updateBatches[0]).toEqual(updateBatches[3]);
    expect(updateBatches[0]?.authorization).toBe("Bearer fixture-token");
    expect(updateBatches[0]?.body).toMatchObject({
      sourceId: "fixture:herdr",
      listeningGeneration: 5,
      topologySequence: expect.any(Number),
      activities: [expect.objectContaining({ id: expect.any(String), text: "published update" })],
    });
    const sourceEpochs = batches.map(({ body }) => body.sourceEpoch);
    expect(sourceEpochs[0]).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    expect(sourceEpochs).toEqual(sourceEpochs.map(() => sourceEpochs[0]));
    const topologyOnly = batches.filter(({ body }) => Array.isArray(body.activities) && body.activities.length === 0 && ((body.topology as { tabs: unknown[] }).tabs.length === 2));
    expect(topologyOnly.length).toBeGreaterThanOrEqual(2);
    expect(topologyOnly.every(({ body }) => Boolean(body.cursors))).toBe(true);
    expect(topologyOnly.every(({ body }) => (body.cursors as unknown[]).length === 0)).toBe(true);
    expect(Number(topologyOnly[1]?.body.topologySequence)).toBeGreaterThan(Number(topologyOnly[0]?.body.topologySequence));
    expect((updateBatches[0]?.body.topology as { participants: Array<{ rawPaneId: string }> }).participants.map((participant) => participant.rawPaneId)).toEqual(["pane-a"]);
    expect(results).toEqual([
      expect.objectContaining({ path: "/api/collector/jobs/job-a/result", body: expect.objectContaining({ leaseToken: "lease-a", resultKey: expect.any(String), result: expect.objectContaining({ speak: false, evidenceEventIds: ["event-a"] }) }) }),
      expect.objectContaining({ path: "/api/collector/jobs/job-idle/result", body: expect.objectContaining({ leaseToken: "lease-a", resultKey: expect.any(String), result: expect.objectContaining({ speak: false, evidenceEventIds: ["event-a"] }) }) }),
    ]);
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
