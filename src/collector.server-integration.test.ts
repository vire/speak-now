import { expect, test } from "bun:test";
import { Database } from "bun:sqlite";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:net";
import { createHash } from "node:crypto";

const token = "synthetic-collector-boundary-token";
const assistantRecord = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;

async function unusedPort() {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Missing test port");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function waitForHealth(origin: string) {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    try {
      if ((await fetch(`${origin}/api/health`, { signal: AbortSignal.timeout(100) })).ok) return;
    } catch { /* Server is starting. */ }
    await Bun.sleep(20);
  }
  throw new Error("Isolated server did not become healthy");
}

test("real collector partitions an escaped activity burst and advances its cursor only after durable receipts", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-real-boundary-"));
  const bin = join(root, "bin");
  const home = join(root, "home");
  const collectorData = join(root, "collector-data");
  const appData = join(root, "app-data");
  const snapshotPath = join(root, "snapshot.json");
  const transcriptDir = join(root, "transcripts");
  const sessionIds = ["session-a", "session-b", "session-c"];
  const herdrPath = join(bin, "herdr");
  const rgPath = join(bin, "rg");
  const summaryPath = join(bin, "claude");
  const herdrMarker = join(root, "herdr-marker");
  const rgMarker = join(root, "rg-marker");
  const summaryMarker = join(root, "summary-marker");
  const port = await unusedPort();
  const origin = `http://127.0.0.1:${port}`;
  let app: ReturnType<typeof Bun.spawn> | undefined;
  let appError: Promise<string> | undefined;
  let appOutput: Promise<string> | undefined;
  let proxy: ReturnType<typeof Bun.serve> | undefined;
  try {
    await Promise.all([mkdir(bin), mkdir(home), mkdir(transcriptDir)]);
    await writeFile(herdrPath, '#!/bin/sh\nprintf "%s" "$0" > "$SN05_HERDR_MARKER"\n/bin/cat "$SN05_SNAPSHOT"\n');
    await writeFile(rgPath, '#!/bin/sh\nprintf "%s" "$0" > "$SN05_RG_MARKER"\nprintf "%s\\n" "$SN05_TRANSCRIPT_DIR/$5.jsonl"\n');
    await writeFile(summaryPath, '#!/bin/sh\nprintf "%s" "$0" > "$SN05_SUMMARY_MARKER"\nexit 91\n');
    await Promise.all([herdrPath, rgPath, summaryPath].map((path) => chmod(path, 0o755)));
    await writeFile(snapshotPath, JSON.stringify({ result: { snapshot: {
      workspaces: [{ workspace_id: "workspace-a", label: "Synthetic" }],
      tabs: [{ tab_id: "tab-a", workspace_id: "workspace-a", label: "Synthetic tab" }],
      panes: sessionIds.map((_, index) => ({ pane_id: `pane-${index}`, tab_id: "tab-a", terminal_id: `terminal-${index}` })),
      agents: sessionIds.map((sessionId, index) => ({ agent: "codex", pane_id: `pane-${index}`, terminal_id: `terminal-${index}`, agent_session: { kind: "id", source: "fixture", value: sessionId } })),
    } } }));
    await Promise.all(sessionIds.map((sessionId) => writeFile(join(transcriptDir, `${sessionId}.jsonl`), `${JSON.stringify({ type: "session_meta", payload: { id: sessionId } })}\n${assistantRecord(`baseline-${sessionId}`, "synthetic baseline")}`)));

    app = Bun.spawn([process.execPath, "--no-env-file", "src/server.ts"], {
      cwd: process.cwd(), stdout: "pipe", stderr: "pipe",
      env: { PATH: "/usr/bin:/bin", HOME: home, TMPDIR: root, XDG_CONFIG_HOME: join(home, "config"), XDG_CACHE_HOME: join(home, "cache"), PORT: String(port), SPEAK_NOW_DATA_DIR: appData, SPEAK_NOW_COLLECTOR_TOKEN: token },
    });
    appOutput = new Response(app.stdout as ReadableStream<Uint8Array>).text();
    appError = new Response(app.stderr as ReadableStream<Uint8Array>).text();
    await waitForHealth(origin);

    const callerPath = join(root, "collector-caller.ts");
    await writeFile(callerPath, `
      import { realpathSync } from "node:fs";
      import { restore, runOnce } from ${JSON.stringify(join(process.cwd(), "src/collector.ts"))};
      const selected = { herdr: realpathSync(Bun.which("herdr")!), rg: realpathSync(Bun.which("rg")!), summary: realpathSync(Bun.which("claude")!) };
      const state = await restore(${JSON.stringify(collectorData)});
      const result = await runOnce({ sourceNamespace: "fixture", dataDir: ${JSON.stringify(collectorData)}, excludePaneIds: [], collectorUrl: process.env.SN05_COLLECTOR_URL!, collectorToken: ${JSON.stringify(token)}, workerId: "fixture-boundary-worker" }, state);
      console.log(JSON.stringify({ selected, captured: result.activities.length, pending: state.remote.pendingBatches.length }));
    `);
    const runCollector = async (collectorUrl = origin) => {
      const child = Bun.spawn([process.execPath, "--no-env-file", callerPath], {
        cwd: process.cwd(), stdout: "pipe", stderr: "pipe",
        env: { PATH: `${bin}:/usr/bin:/bin`, HOME: home, TMPDIR: root, XDG_CONFIG_HOME: join(home, "config"), XDG_CACHE_HOME: join(home, "cache"), SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "synthetic", SN05_HERDR_MARKER: herdrMarker, SN05_RG_MARKER: rgMarker, SN05_SUMMARY_MARKER: summaryMarker, SN05_SNAPSHOT: snapshotPath, SN05_TRANSCRIPT_DIR: transcriptDir, SN05_COLLECTOR_URL: collectorUrl },
      });
      const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      await child.exited;
      expect(child.exitCode, stderr).toBe(0);
      expect(stderr).toBe("");
      const result = JSON.parse(stdout) as { selected: { herdr: string; rg: string; summary: string }; captured: number; pending: number };
      expect(result.selected).toEqual({ herdr: await realpath(herdrPath), rg: await realpath(rgPath), summary: await realpath(summaryPath) });
      expect(await realpath(await readFile(herdrMarker, "utf8"))).toBe(await realpath(herdrPath));
      expect(await realpath(await readFile(rgMarker, "utf8"))).toBe(await realpath(rgPath));
      if (await Bun.file(summaryMarker).exists()) expect(await realpath(await readFile(summaryMarker, "utf8"))).toBe(await realpath(summaryPath));
      return result;
    };

    const baseline = await runCollector();
    expect(baseline.pending).toBe(0);
    const databasePath = join(appData, "speak-now.sqlite");
    const readDatabase = () => {
      const db = new Database(databasePath, { readonly: true });
      try {
        const counts = db.query("SELECT COUNT(*) AS total, COUNT(DISTINCT event_id) AS distinct_ids FROM events WHERE source_id = 'fixture:herdr'").get() as { total: number; distinct_ids: number };
        const cursorRows = db.query("SELECT participant_id, cursor_value FROM cursors WHERE source_id = 'fixture:herdr' ORDER BY participant_id").all() as Array<{ participant_id: string; cursor_value: string }>;
        const present = db.query("SELECT COUNT(*) AS count FROM participants WHERE source_id = 'fixture:herdr' AND present = 1").get() as { count: number };
        const workspace = db.query("SELECT workspace_id FROM workspaces WHERE source_id = 'fixture:herdr' LIMIT 1").get() as { workspace_id: string };
        return { counts, cursorRows, present: present.count, workspaceId: workspace.workspace_id };
      } finally { db.close(); }
    };
    const baselineDatabase = readDatabase();
    expect(baselineDatabase.present).toBe(3);
    expect(baselineDatabase.cursorRows).toHaveLength(3);
    const escapedText = "\u0000".repeat(8_000);
    expect(Buffer.byteLength(escapedText)).toBe(8_000);
    await Promise.all(sessionIds.map(async (sessionId) => {
      const path = join(transcriptDir, `${sessionId}.jsonl`);
      const records = Array.from({ length: 5 }, (_, index) => assistantRecord(`burst-${sessionId}-${index}`, escapedText));
      await Bun.write(path, `${await readFile(path, "utf8")}${records.join("")}`);
    }));

    let discardFirstReceipt = true;
    const observedPosts: Array<{ batchId: string; bodyHash: string; upstreamStatus: number }> = [];
    proxy = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: async (request) => {
      const url = new URL(request.url);
      const headers = new Headers(request.headers);
      headers.delete("host");
      headers.delete("content-length");
      const body = request.method === "GET" || request.method === "HEAD" ? undefined : await request.text();
      const upstream = await fetch(`${origin}${url.pathname}${url.search}`, { method: request.method, headers, body });
      if (url.pathname === "/api/collector/batches" && body) {
        const batchId = (JSON.parse(body) as { batchId: string }).batchId;
        observedPosts.push({ batchId, bodyHash: createHash("sha256").update(body).digest("hex"), upstreamStatus: upstream.status });
        if (discardFirstReceipt && upstream.status === 201) {
          discardFirstReceipt = false;
          await upstream.arrayBuffer();
          return Response.json({ code: "synthetic_lost_response" }, { status: 503 });
        }
      }
      return upstream;
    } });

    const interrupted = await runCollector(proxy.url.origin);
    expect(interrupted.captured).toBe(15);
    const staged = await Bun.file(join(collectorData, "collector-state.json")).json() as { remote: { pendingBatches: Array<{ body: { batchId: string }; encoded: string }>; acknowledgedCursors: Record<string, string>; topologySequence: number } };
    expect(staged.remote.pendingBatches.length).toBeGreaterThan(1);
    expect(staged.remote.pendingBatches.map((pending) => Buffer.byteLength(pending.encoded)).every((bytes) => bytes <= 524_288)).toBe(true);
    const firstHead = staged.remote.pendingBatches[0]!;
    const firstBodyHash = createHash("sha256").update(firstHead.encoded).digest("hex");
    const interruptedDatabase = readDatabase();
    expect(interruptedDatabase.counts.total).toBeGreaterThan(0);
    expect(interruptedDatabase.counts.total).toBeLessThan(15);
    expect(interruptedDatabase.cursorRows).toEqual(baselineDatabase.cursorRows);
    expect(observedPosts[0]).toEqual({ batchId: firstHead.body.batchId, bodyHash: firstBodyHash, upstreamStatus: 201 });

    const restarted = await runCollector(proxy.url.origin);
    expect(restarted.pending).toBeGreaterThan(0);
    const afterRetryDatabase = readDatabase();
    expect(afterRetryDatabase.counts).toEqual(interruptedDatabase.counts);
    expect(afterRetryDatabase.cursorRows).toEqual(baselineDatabase.cursorRows);
    expect(observedPosts[1]).toEqual({ batchId: firstHead.body.batchId, bodyHash: firstBodyHash, upstreamStatus: 200 });

    let remaining = restarted.pending;
    for (let attempt = 0; attempt < 10 && remaining > 0; attempt++) remaining = (await runCollector(proxy.url.origin)).pending;
    expect(remaining).toBe(0);
    const state = await Bun.file(join(collectorData, "collector-state.json")).json() as { remote: { pendingBatches: Array<{ encoded: string }>; acknowledgedCursors: Record<string, string>; topologySequence: number } };
    expect(state.remote.pendingBatches).toHaveLength(0);
    expect(Object.keys(state.remote.acknowledgedCursors)).toHaveLength(3);
    expect(state.remote.topologySequence).toBeGreaterThan(2);
    expect(readDatabase().counts).toEqual({ total: 15, distinct_ids: 15 });

    const longText = `${"界".repeat(7_000)}${"\u0000".repeat(1_000)}`;
    expect(Buffer.byteLength(longText)).toBe(22_000);
    const firstTranscript = join(transcriptDir, `${sessionIds[0]}.jsonl`);
    await Bun.write(firstTranscript, `${await readFile(firstTranscript, "utf8")}${assistantRecord("long-update", longText)}`);
    expect((await runCollector()).pending).toBe(0);
    const afterLong = readDatabase();
    expect(afterLong.counts).toEqual({ total: 16, distinct_ids: 16 });
    const longDb = new Database(databasePath, { readonly: true });
    try {
      const row = longDb.query("SELECT evidence_json FROM events WHERE source_id = 'fixture:herdr' ORDER BY rowid DESC LIMIT 1").get() as { evidence_json: string };
      const evidence = JSON.parse(row.evidence_json) as { text: string; originalTextBytes: number; truncated: boolean };
      expect(Buffer.byteLength(evidence.text)).toBeLessThanOrEqual(20_000);
      expect(evidence.originalTextBytes).toBe(22_000);
      expect(evidence.truncated).toBe(true);
    } finally { longDb.close(); }

    const selected = await fetch(`${origin}/api/listening`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sourceId: "fixture:herdr", workspaceId: readDatabase().workspaceId }) });
    expect(selected.status).toBe(200);
    expect((await runCollector()).pending).toBe(0);
    expect(readDatabase().counts).toEqual({ total: 16, distinct_ids: 16 });

    const snapshot = JSON.parse(await readFile(snapshotPath, "utf8")) as { result: { snapshot: { panes: Array<{ pane_id: string }>; agents: Array<{ pane_id: string }> } } };
    const removedPane = snapshot.result.snapshot.panes.shift()!;
    const removedAgent = snapshot.result.snapshot.agents.shift()!;
    await writeFile(snapshotPath, JSON.stringify(snapshot));
    expect((await runCollector()).pending).toBe(0);
    expect(readDatabase().present).toBe(2);
    const retiredCursor = afterLong.cursorRows.find((row) => row.participant_id === baselineDatabase.cursorRows[0]?.participant_id);
    expect(retiredCursor).toBeDefined();
    expect(readDatabase().cursorRows).toContainEqual(retiredCursor!);

    await Bun.write(firstTranscript, `${await readFile(firstTranscript, "utf8")}${assistantRecord("absent-update", "should remain silent on exact return")}`);
    snapshot.result.snapshot.panes.unshift(removedPane);
    snapshot.result.snapshot.agents.unshift(removedAgent);
    await writeFile(snapshotPath, JSON.stringify(snapshot));
    expect((await runCollector()).pending).toBe(0);
    expect(readDatabase().present).toBe(3);
    expect(readDatabase().counts).toEqual({ total: 16, distinct_ids: 16 });

    await Bun.write(firstTranscript, `${await readFile(firstTranscript, "utf8")}${assistantRecord("after-return", "new visible activity after return")}`);
    expect((await runCollector()).pending).toBe(0);
    expect(readDatabase().counts).toEqual({ total: 17, distinct_ids: 17 });
  } finally {
    proxy?.stop(true);
    if (app && app.exitCode === null) app.kill("SIGTERM");
    if (app) {
      const timer = setTimeout(() => { if (app!.exitCode === null) app!.kill("SIGKILL"); }, 2_000);
      await app.exited;
      clearTimeout(timer);
      expect(app.exitCode, await appError).toBe(0);
      await appOutput;
    }
    await rm(root, { recursive: true, force: true });
  }
}, 60_000);
