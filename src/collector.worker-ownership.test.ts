import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

type Cancellation = "deadline" | "caller";

interface WorkerProbe {
  cancellation: Cancellation;
  summaryBackend: string | undefined;
  resolvedHerdr: string | null;
  resolvedClaude: string | null;
  workerPid: number;
  aliveAtReturn: boolean;
  processStateAtReturn: string;
  termReceived: boolean;
  sourceStatuses: unknown[];
  reports: string;
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  const started = performance.now();
  while (!(await predicate())) {
    if (performance.now() - started > timeoutMs) throw new Error("fixture condition did not become true");
    await Bun.sleep(10);
  }
}

async function runProbe(cancellation: Cancellation): Promise<{ probe: WorkerProbe; claims: number; resultsPosted: number; herdrCommand: string; summaryCommand: string; expectedHerdr: string; expectedClaude: string }> {
  const root = await mkdtemp(join(tmpdir(), `speak-now-worker-owner-${cancellation}-`));
  const bin = join(root, "bin");
  const home = join(root, "home");
  const temporary = join(root, "tmp");
  const workerPid = join(home, "worker.pid");
  const workerTerm = join(home, "worker.term");
  const herdrMarker = join(home, "herdr-command");
  const summaryMarker = join(home, "summary-command");
  let resultsPosted = 0;
  let claims = 0;
  const server = Bun.serve({
    port: 0,
    fetch: async (request) => {
      const url = new URL(request.url);
      if (url.pathname === "/api/collector/batches") {
        const body = await request.json() as { batchId: string; activities: Array<{ id: string }>; cursors: Array<{ participantId: string; next: string }>; topologySequence: number };
        return Response.json({ batchId: body.batchId, acceptedEventIds: body.activities.map((activity) => activity.id), cursors: body.cursors.map(({ participantId, next }) => ({ participantId, cursor: next })), topologySequence: body.topologySequence }, { status: 201 });
      }
      if (url.pathname === "/api/collector/config") return Response.json({ sourceId: "fixture:herdr", listeningScope: null, listeningGeneration: 1 });
      if (url.pathname === "/api/collector/jobs/claim") { claims += 1; return Response.json({ jobId: "worker-job", attempt: 1, leaseToken: "lease-token", leaseDurationMs: cancellation === "deadline" ? 6_200 : 60_000, evidenceEventIds: ["event-a"], evidence: [{ id: "event-a", participantId: "fixture:participant", sourceCursor: "1", observedAt: "2026-10-06T00:00:00.000Z", kind: "assistant", text: "fixture evidence", captureMode: "structured", status: "complete", truncated: false, excerpt: "full", originalTextBytes: 16 }] }); }
      if (url.pathname === "/api/collector/jobs/worker-job/result") { resultsPosted += 1; return Response.json({ announcementId: "unexpected" }, { status: 201 }); }
      return new Response(null, { status: 204 });
    },
  });
  try {
    await mkdir(bin);
    await mkdir(home);
    await mkdir(temporary);
    const herdr = join(bin, "herdr");
    const claude = join(bin, "claude");
    await writeFile(herdr, `#!${Bun.which("bun")!}\nimport { writeFileSync } from "node:fs"; import { join } from "node:path"; writeFileSync(join(process.env.HOME, "herdr-command"), process.argv[1]); process.stdout.write('{"result":{"snapshot":{"workspaces":[],"tabs":[],"panes":[],"agents":[]}}}');\n`);
    await writeFile(claude, `#!${Bun.which("bun")!}\nimport { writeFileSync } from "node:fs"; import { join } from "node:path"; writeFileSync(join(process.env.HOME, "summary-command"), process.argv[1]); writeFileSync(join(process.env.HOME, "worker.pid"), String(process.pid)); process.on("SIGTERM", () => writeFileSync(join(process.env.HOME, "worker.term"), "received")); setInterval(() => {}, 1_000);\n`);
    await chmod(herdr, 0o755);
    await chmod(claude, 0o755);
    const caller = `
      import { existsSync, realpathSync } from "node:fs";
      import { initialState, runOnce } from "./src/collector.ts";
      const controller = new AbortController();
      const workerPid = ${JSON.stringify(workerPid)};
      const cancellation = ${JSON.stringify(cancellation)};
      const abortWatcher = cancellation === "caller" ? setInterval(() => { if (existsSync(workerPid)) controller.abort(); }, 5) : undefined;
      const state = initialState();
      await runOnce({ sourceNamespace: "fixture", dataDir: ${JSON.stringify(join(root, "data"))}, excludePaneIds: [], collectorUrl: ${JSON.stringify(server.url.origin)}, collectorToken: "fixture-token", workerId: "fixture-worker" }, state, controller.signal);
      if (abortWatcher) clearInterval(abortWatcher);
      const workerStarted = await Bun.file(workerPid).exists();
      const pid = workerStarted ? Number(await Bun.file(workerPid).text()) : 0;
      let alive = true;
      try { if (pid > 0) process.kill(pid, 0); else alive = false; } catch { alive = false; }
      const ps = Bun.spawn(["ps", "-o", "stat=", "-p", String(pid)], { stdout: "pipe", stderr: "ignore" });
      const processStateAtReturn = (await new Response(ps.stdout).text()).trim();
      await ps.exited;
      console.log(JSON.stringify({ cancellation, summaryBackend: Bun.env.SUMMARY_BACKEND, resolvedHerdr: realpathSync(Bun.which("herdr")), resolvedClaude: realpathSync(Bun.which("claude")), workerStarted, workerPid: pid, aliveAtReturn: alive, processStateAtReturn, termReceived: await Bun.file(${JSON.stringify(workerTerm)}).exists(), sourceStatuses: state.sourceStatuses, reports: await Bun.file(${JSON.stringify(join(root, "data", "errors", "reports.jsonl"))}).exists() ? await Bun.file(${JSON.stringify(join(root, "data", "errors", "reports.jsonl"))}).text() : "" }));
    `;
    const child = Bun.spawn([Bun.which("bun")!, "--no-env-file", "-e", caller], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: `${bin}:/usr/bin:/bin`,
        HOME: home,
        TMPDIR: temporary,
        SUMMARY_BACKEND: "claude",
        SUMMARY_MODEL: "fixture",
        HERDR_ENV: "",
        HERDR_PANE_ID: "",
        HERDR_TAB_ID: "",
        HERDR_WORKSPACE_ID: "",
        OBSERVATION_EXCLUDE_PANES: "",
        SPEAK_NOW_COLLECTOR_URL: "",
        SPEAK_NOW_COLLECTOR_TOKEN: "",
        SOURCE_EPOCH: "",
        ELEVENLABS_API_KEY: "",
        ELEVENLABS_VOICE_ID: "",
      },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    return {
      probe: JSON.parse(stdout) as WorkerProbe,
      claims,
      resultsPosted,
      herdrCommand: await Bun.file(herdrMarker).exists() ? await Bun.file(herdrMarker).text() : "",
      summaryCommand: await Bun.file(summaryMarker).exists() ? await Bun.file(summaryMarker).text() : "",
      expectedHerdr: await realpath(herdr),
      expectedClaude: await realpath(claude),
    };
  } finally {
    server.stop(true);
    await rm(root, { recursive: true, force: true });
  }
}

for (const cancellation of ["deadline", "caller"] as const) {
  test(`collector joins its resistant fake worker before ${cancellation} return`, async () => {
    const result = await runProbe(cancellation);
    expect(result.probe).toMatchObject({ cancellation, summaryBackend: "claude", resolvedHerdr: result.expectedHerdr, resolvedClaude: result.expectedClaude, workerStarted: true, aliveAtReturn: false, processStateAtReturn: "", termReceived: true, sourceStatuses: [], reports: "" });
    expect(result.herdrCommand).toBe(result.expectedHerdr);
    expect(result.summaryCommand).toBe(result.expectedClaude);
    expect(result.claims).toBe(1);
    expect(result.resultsPosted).toBe(0);
  }, 12_000);
}
