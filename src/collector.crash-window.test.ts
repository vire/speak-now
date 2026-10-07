import { expect, test } from "bun:test";
import { appendFile, chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("a process crash after post-capture persistence recovers the staged activity envelope", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-crash-window-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  const publishedActivities: Array<{ text: string }> = [];
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/batches") {
        const body = await request.json() as { batchId: string; activities: Array<{ id: string; text: string }>; cursors: Array<{ participantId: string; next: string }>; topologySequence: number };
        publishedActivities.push(...body.activities);
        return Response.json({
          batchId: body.batchId,
          acceptedEventIds: body.activities.map((activity) => activity.id),
          cursors: body.cursors.map(({ participantId, next }) => ({ participantId, cursor: next })),
          topologySequence: body.topologySequence,
        }, { status: 201 });
      }
      if (url.pathname === "/api/collector/config") return Response.json({ sourceId: "fixture:herdr", listeningScope: { workspaceId: "workspace-a" }, listeningGeneration: 5 });
      if (url.pathname === "/api/collector/jobs/claim") return new Response(null, { status: 204 });
      return Response.json({ code: "missing" }, { status: 404 });
    },
  });
  try {
    await mkdir(bin);
    const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline output")}`);
    await writeFile(join(root, "paths"), `${transcript}\n`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace-a", label: "Workspace" }],
      tabs: [{ tab_id: "tab-a", workspace_id: "workspace-a", label: "Tab" }],
      panes: [{ pane_id: "pane-a", tab_id: "tab-a", terminal_id: "terminal-a" }],
      agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }],
    } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/cat ${join(root, "paths")}\n`);
    for (const command of ["herdr", "rg"]) await chmod(join(bin, command), 0o755);
    const config = `{sourceNamespace:"fixture",dataDir:${JSON.stringify(data)},excludePaneIds:[],collectorUrl:${JSON.stringify(server.url.origin)},collectorToken:"fixture-token",workerId:"fixture-worker"}`;
    const crashScript = `
      import { appendFile } from "node:fs/promises";
      import { initialState, runOnce } from "./src/collector.ts";
      const state = initialState();
      await runOnce(${config}, state);
      await appendFile(${JSON.stringify(transcript)}, ${JSON.stringify(message("event-after-crash", "recover after crash"))});
      const crashing = new Proxy(state, {
        set(target, key, value, receiver) {
          if (key === "cursors") process.exit(93);
          return Reflect.set(target, key, value, receiver);
        },
      });
      await runOnce(${config}, crashing);
    `;
    const crashed = Bun.spawn([Bun.which("bun")!, "-e", crashScript], {
      cwd: process.cwd(), stdout: "pipe", stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root },
    });
    const crashStderr = await new Response(crashed.stderr).text();
    await crashed.exited;
    expect(crashed.exitCode, crashStderr).toBe(93);

    const saved = JSON.parse(await readFile(join(data, "collector-state.json"), "utf8")) as { remote: { pendingBatches: Array<{ body: { activities: Array<{ text: string }> } }> }; cursors: Record<string, { sourceCursor: string }>; activityIds: string[] };
    expect(saved.remote.pendingBatches).toHaveLength(1);
    expect(saved.remote.pendingBatches[0]?.body.activities).toEqual([expect.objectContaining({ text: "recover after crash" })]);
    expect(saved.activityIds).toHaveLength(1);
    expect(Object.values(saved.cursors).map((cursor) => cursor.sourceCursor)).not.toEqual([]);

    const restartScript = `
      import { restore, runOnce } from "./src/collector.ts";
      const state = await restore(${JSON.stringify(data)});
      await runOnce(${config}, state);
    `;
    const restarted = Bun.spawn([Bun.which("bun")!, "-e", restartScript], {
      cwd: process.cwd(), stdout: "pipe", stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root },
    });
    const restartStderr = await new Response(restarted.stderr).text();
    await restarted.exited;
    expect(restarted.exitCode, restartStderr).toBe(0);
    expect(publishedActivities).toContainEqual(expect.objectContaining({ text: "recover after crash" }));
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
