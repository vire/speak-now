import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initialState, runOnce } from "./collector";
import { COLLECTOR_WIRE_LIMITS } from "./shared";

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  const started = performance.now();
  while (!(await predicate())) {
    if (performance.now() - started > timeoutMs) throw new Error("fixture condition did not become true");
    await Bun.sleep(10);
  }
}

interface NativeRecord { method: string; authorization: string | undefined; body: string; requestEnded: boolean; responseClosed: boolean; socketClosed: boolean; }

const nodeFixture = `
  import { createServer } from "node:http";
  import { writeFileSync } from "node:fs";
  const records = [];
  const publish = () => writeFileSync(process.env.RECORDS_FILE, JSON.stringify(records));
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const record = { method: request.method, authorization: request.headers.authorization, body: Buffer.concat(chunks).toString("utf8"), requestEnded: request.complete, responseClosed: false, socketClosed: false };
    records.push(record);
    response.on("close", () => { record.responseClosed = true; publish(); });
    response.socket.on("close", () => { record.socketClosed = true; publish(); });
    publish();
    response.writeHead(201, { "Content-Type": "application/json" });
    response.flushHeaders();
    response.write(process.env.MODE === "overflow" ? "x".repeat(Number(process.env.RESPONSE_BYTES)) : "{");
  });
  server.listen(0, "127.0.0.1", () => writeFileSync(process.env.PORT_FILE, String(server.address().port)));
`;

async function startNodeFixture(root: string, mode: "stall" | "overflow") {
  const script = join(root, `node-${mode}-fixture.mjs`);
  const portFile = join(root, `node-${mode}-port`);
  const recordsFile = join(root, `node-${mode}-records.json`);
  await writeFile(script, nodeFixture);
  const child = Bun.spawn([Bun.which("node")!, script], { stdout: "ignore", stderr: "pipe", env: { PATH: "/usr/bin:/bin", HOME: join(root, "node-home"), TMPDIR: root, PORT_FILE: portFile, RECORDS_FILE: recordsFile, MODE: mode, RESPONSE_BYTES: String(COLLECTOR_WIRE_LIMITS.responseBytes + 1) } });
  await waitFor(async () => Bun.file(portFile).exists(), 2_000);
  const port = Number(await Bun.file(portFile).text());
  return {
    url: `http://127.0.0.1:${port}`,
    records: async (): Promise<NativeRecord[]> => {
      try { return JSON.parse(await Bun.file(recordsFile).text()) as NativeRecord[]; }
      catch { return []; }
    },
    stop: async () => { child.kill(); await child.exited; },
  };
}

const exactControlEnvelope = { batchId: "fixture-control" };

interface IsolatedCollectorRun {
  result: { outcome: "resolved" | "rejected" };
  selected: { herdr: string; summary: string };
  executed: { herdr: string; summary: string | null };
}

async function runIsolatedCollector(root: string, collectorUrl: string): Promise<IsolatedCollectorRun> {
  const bin = join(root, "isolated-bin");
  const home = join(root, "isolated-home");
  const temporary = join(root, "isolated-tmp");
  const herdr = join(bin, "herdr");
  const summary = join(bin, "claude");
  const selection = join(root, "command-selection.json");
  const herdrExecution = join(root, "herdr-executed");
  const summaryExecution = join(root, "summary-executed");
  const caller = join(root, "isolated-collector.ts");
  await Promise.all([mkdir(bin), mkdir(home), mkdir(temporary)]);
  await writeFile(herdr, "#!/bin/sh\nprintf '%s' \"$0\" > \"$SN05_HERDR_EXECUTION_MARKER\"\nprintf '%s' '{\"result\":{\"snapshot\":{\"workspaces\":[],\"tabs\":[],\"panes\":[],\"agents\":[]}}}'\n");
  await writeFile(summary, "#!/bin/sh\nprintf '%s' \"$0\" > \"$SN05_SUMMARY_EXECUTION_MARKER\"\nprintf '%s' '{\"title\":\"fixture\",\"summary\":\"fixture\"}'\n");
  await Promise.all([chmod(herdr, 0o755), chmod(summary, 0o755)]);
  await writeFile(caller, `
    import { writeFileSync, realpathSync } from "node:fs";
    import { initialState, runOnce } from ${JSON.stringify(join(process.cwd(), "src/collector.ts"))};
    const selected = { herdr: realpathSync(Bun.which("herdr")!), summary: realpathSync(Bun.which("claude")!) };
    writeFileSync(Bun.env.SN05_COMMAND_SELECTION!, JSON.stringify(selected));
    try {
      await runOnce({ sourceNamespace: "fixture", dataDir: ${JSON.stringify(join(root, "data"))}, excludePaneIds: [], collectorUrl: ${JSON.stringify(collectorUrl)}, collectorToken: "fixture-token", workerId: "fixture-worker" }, initialState());
      console.log(JSON.stringify({ outcome: "resolved" }));
    } catch {
      console.log(JSON.stringify({ outcome: "rejected" }));
    }
  `);
  const child = Bun.spawn([Bun.which("bun")!, "--no-env-file", caller], {
    cwd: process.cwd(),
    stdout: "pipe",
    stderr: "pipe",
    env: {
      PATH: `${bin}:/usr/bin:/bin`,
      HOME: home,
      TMPDIR: temporary,
      XDG_CONFIG_HOME: join(home, "config"),
      XDG_CACHE_HOME: join(home, "cache"),
      OBSERVATION_EXCLUDE_PANES: "fixture:excluded",
      HERDR_ENV: "",
      HERDR_WORKSPACE_ID: "",
      HERDR_TAB_ID: "",
      HERDR_PANE_ID: "",
      SOURCE_EPOCH: "",
      SPEAK_NOW_COLLECTOR_URL: "",
      SPEAK_NOW_COLLECTOR_TOKEN: "",
      SUMMARY_BACKEND: "claude",
      SUMMARY_MODEL: "fixture",
      SN05_COMMAND_SELECTION: selection,
      SN05_HERDR_EXECUTION_MARKER: herdrExecution,
      SN05_SUMMARY_EXECUTION_MARKER: summaryExecution,
    },
  });
  const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
  await child.exited;
  expect(child.exitCode, stderr).toBe(0);
  const selected = JSON.parse(await readFile(selection, "utf8")) as IsolatedCollectorRun["selected"];
  const executed = {
    herdr: await readFile(herdrExecution, "utf8"),
    summary: await Bun.file(summaryExecution).exists() ? await readFile(summaryExecution, "utf8") : null,
  };
  return { result: JSON.parse(stdout) as IsolatedCollectorRun["result"], selected, executed };
}

test("collector transport deadline aborts a stalled native response", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-stalled-http-"));
  const fixture = await startNodeFixture(root, "stall");
  try {
    const baseline = new AbortController();
    let baselineBodyStarted = false;
    const baselineRequest = fetch(`${fixture.url}/api/collector/batches`, { method: "POST", headers: { Authorization: "Bearer fixture-token", "Content-Type": "application/json" }, body: JSON.stringify(exactControlEnvelope), signal: baseline.signal })
      .then(async (response) => {
        const reader = response.body!.getReader();
        await reader.read();
        baselineBodyStarted = true;
        await reader.read();
      })
      .catch(() => undefined);
    await waitFor(async () => (await fixture.records()).length === 1 && baselineBodyStarted, 1_000);
    await Bun.sleep(100);
    expect(await fixture.records()).toEqual([expect.objectContaining({ method: "POST", authorization: "Bearer fixture-token", body: JSON.stringify(exactControlEnvelope), requestEnded: true, responseClosed: false, socketClosed: false })]);
    baseline.abort();
    await baselineRequest;
    await waitFor(async () => Boolean((await fixture.records())[0]?.responseClosed && (await fixture.records())[0]?.socketClosed), 1_000);

    const started = performance.now();
    const isolated = await runIsolatedCollector(root, fixture.url);
    const elapsed = performance.now() - started;
    await waitFor(async () => Boolean((await fixture.records())[1]?.responseClosed && (await fixture.records())[1]?.socketClosed), 1_000);
    expect(elapsed).toBeLessThan(6_500);
    expect(isolated.result).toEqual({ outcome: "rejected" });
    const commands = { herdr: await realpath(join(root, "isolated-bin", "herdr")), summary: await realpath(join(root, "isolated-bin", "claude")) };
    expect(isolated.selected).toEqual(commands);
    expect(await realpath(isolated.executed.herdr)).toBe(commands.herdr);
    expect(isolated.executed.summary).toBeNull();
    expect(JSON.parse((await fixture.records())[1]!.body)).toMatchObject({ sourceId: "fixture:herdr", batchId: expect.any(String), sourceEpoch: expect.any(String), activities: [] });
    expect((await fixture.records())[1]).toMatchObject({ method: "POST", authorization: "Bearer fixture-token", requestEnded: true, responseClosed: true, socketClosed: true });
  } finally {
    await fixture.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 12_000);

test("collector aborts a remote response body that exceeds the bounded transport limit", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-oversized-http-"));
  const fixture = await startNodeFixture(root, "overflow");
  try {
    const started = performance.now();
    const isolated = await runIsolatedCollector(root, fixture.url);
    await waitFor(async () => Boolean((await fixture.records())[0]?.responseClosed && (await fixture.records())[0]?.socketClosed), 1_000);
    expect(performance.now() - started).toBeLessThan(1_500);
    expect(isolated.result).toEqual({ outcome: "rejected" });
    const commands = { herdr: await realpath(join(root, "isolated-bin", "herdr")), summary: await realpath(join(root, "isolated-bin", "claude")) };
    expect(isolated.selected).toEqual(commands);
    expect(await realpath(isolated.executed.herdr)).toBe(commands.herdr);
    expect(isolated.executed.summary).toBeNull();
    expect((await fixture.records())[0]).toMatchObject({ method: "POST", authorization: "Bearer fixture-token", requestEnded: true, responseClosed: true, socketClosed: true });
  } finally {
    await fixture.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 5_000);

test("collector reserves five seconds of a granted lease before starting its isolated worker", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-lease-deadline-"));
  const bin = join(root, "bin");
  let completedResults = 0;
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/batches") {
        const body = await request.json() as { batchId: string; activities: Array<{ id: string }>; cursors: Array<{ participantId: string; next: string }>; topologySequence: number };
        return Response.json({ batchId: body.batchId, acceptedEventIds: body.activities.map((activity) => activity.id), cursors: body.cursors.map(({ participantId, next }) => ({ participantId, cursor: next })), topologySequence: body.topologySequence }, { status: 201 });
      }
      if (url.pathname === "/api/collector/config") return Response.json({ sourceId: "fixture:herdr", listeningScope: null, listeningGeneration: 1 });
      if (url.pathname === "/api/collector/jobs/claim") return Response.json({ jobId: "lease-job", attempt: 1, leaseToken: "lease-token", leaseDurationMs: 5_200, evidenceEventIds: ["event-a"], evidence: [{ id: "event-a", participantId: "fixture:participant", sourceCursor: "1", observedAt: "2026-10-06T00:00:00.000Z", kind: "assistant", text: "fixture", captureMode: "structured", status: "complete", truncated: false, excerpt: "full", originalTextBytes: 7 }] });
      if (url.pathname === "/api/collector/jobs/lease-job/result") { completedResults += 1; return Response.json({ announcementId: "unexpected" }, { status: 201 }); }
      return new Response(null, { status: 204 });
    },
  });
  try {
    await mkdir(bin);
    await writeFile(join(bin, "herdr"), "#!/bin/sh\nprintf '%s' '{\"result\":{\"snapshot\":{\"workspaces\":[],\"tabs\":[],\"panes\":[],\"agents\":[]}}}'\n");
    await writeFile(join(bin, "claude"), "#!/bin/sh\nsleep 10\n");
    for (const command of ["herdr", "claude"]) await chmod(join(bin, command), 0o755);
    const script = `import {initialState,runOnce} from './src/collector.ts'; const started=performance.now(); await runOnce({sourceNamespace:'fixture',dataDir:${JSON.stringify(join(root, "data"))},excludePaneIds:[],collectorUrl:${JSON.stringify(server.url.origin)},collectorToken:'fixture-token',workerId:'fixture-worker'},initialState()); console.log(JSON.stringify({elapsedMs:performance.now()-started}));`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: process.env.HOME ?? root, SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout).elapsedMs).toBeLessThan(2_000);
    expect(completedResults).toBe(0);
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}, 15_000);
