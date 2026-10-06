import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

type StopCase = "deadline" | "caller-cancel" | "output-limit" | "descendant";

async function killOwnedWorker(pidFile: string): Promise<void> {
  let pid: number;
  try {
    pid = Number((await readFile(pidFile, "utf8")).trim());
  } catch {
    return;
  }
  if (!Number.isSafeInteger(pid) || pid <= 0) return;
  try {
    process.kill(pid, "SIGKILL");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
  }
}

async function exitsWithin(child: ReturnType<typeof Bun.spawn>, timeoutMs: number): Promise<boolean> {
  return await Promise.race([child.exited.then(() => true), Bun.sleep(timeoutMs).then(() => false)]);
}

async function runUncooperativeWorker(kind: StopCase) {
  const root = await mkdtemp(join(tmpdir(), "speak-now-summary-hard-stop-"));
  const bin = join(root, "bin");
  const runtimes = join(root, "runtimes");
  const started = join(root, "started");
  const terminated = join(root, "terminated");
  const workerPid = join(root, "worker.pid");
  try {
    await Promise.all([mkdir(bin), mkdir(runtimes)]);
    const flood = kind === "output-limit" ? "printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; printf 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy' >&2" : ":";
    const body = kind === "descendant" ? `#!/bin/sh
sh -c 'echo $$ > ${join(root, "descendant.pid")}; trap "touch ${terminated}" TERM; while :; do :; done' &
echo $$ > ${workerPid}
touch ${started}
exit 0
` : `#!/bin/sh
echo $$ > ${workerPid}
touch ${started}
trap 'touch ${terminated}' TERM
while :; do ${flood}; done
`;
    await writeFile(join(bin, "claude"), body);
    await chmod(join(bin, "claude"), 0o755);
    const runner = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};
import { createErrorReporter } from ${JSON.stringify(join(process.cwd(), "src/errors.ts"))};
const reporter = createErrorReporter({ dataDirectory: ${JSON.stringify(root)} });
const controller = new AbortController();
const work = summarize({ text: "evidence", evidenceEventIds: ["event-a"] }, "claude", "fixture", controller.signal, undefined, undefined, undefined, reporter);
if (${JSON.stringify(kind)} === "caller-cancel") {
  while (!(await Bun.file(${JSON.stringify(started)}).exists())) await Bun.sleep(5);
  controller.abort();
}
try {
  await work;
} catch (error) {
  console.log(error instanceof Error ? error.message : "unknown");
}
await reporter.flush();
`;
    const startedAt = Date.now();
    const child = Bun.spawn([Bun.which("bun")!, "-e", runner], {
      cwd: root,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: `${bin}:${process.env.PATH}`,
        HOME: root,
        TMPDIR: runtimes,
        USER: "fixture-user",
        SUMMARY_TIMEOUT_MS: kind === "deadline" || kind === "descendant" ? "1000" : "30000",
      },
    });
    let cleanup: Promise<void> | undefined;
    const stopFixture = () => cleanup ??= (async () => {
      if (child.exitCode !== null) return;
      await killOwnedWorker(workerPid);
      if (child.exitCode === null && !await exitsWithin(child, 250)) child.kill("SIGKILL");
      await child.exited;
    })();
    const guard = setTimeout(() => { void stopFixture(); }, 3_000);
    try {
      const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      await child.exited;
      const reportsFile = Bun.file(join(root, "errors", "reports.jsonl"));
      const reports = await reportsFile.exists() ? (await reportsFile.text()).trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : [];
      return { reports, root, elapsedMs: Date.now() - startedAt, exitCode: child.exitCode, stdout, stderr, started, terminated, runtimes, workerPid };
    } finally {
      clearTimeout(guard);
      await stopFixture();
      await killOwnedWorker(join(root, "descendant.pid"));
    }
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

// Contract: every stop source escalates an owned TERM-ignoring worker to SIGKILL,
// reaps it, and cleans its private runtime before the caller returns.
for (const kind of ["deadline", "caller-cancel", "output-limit", "descendant"] as const) {
  test(`hard-stops and reaps an uncooperative worker after ${kind}`, async () => {
    const result = await runUncooperativeWorker(kind);
    try {
      expect(result.exitCode, result.stderr).toBe(0);
      expect(result.reports).toHaveLength(kind === "caller-cancel" ? 0 : 1);
      if (kind !== "caller-cancel") expect(result.reports[0].category).toBe(kind === "output-limit" ? "subprocess" : "timeout");
      expect(result.elapsedMs).toBeLessThan(2_000);
      expect(await Bun.file(result.started).exists()).toBe(true);
      expect(await Bun.file(result.terminated).exists()).toBe(true);
      expect(await readdir(result.runtimes)).toEqual([]);
      expect(result.stdout).toContain(kind === "deadline" || kind === "descendant" ? "timed out" : kind === "caller-cancel" ? "cancelled" : "output exceeded its limit");
    } finally {
      await rm(result.root, { recursive: true, force: true });
    }
  }, 5_000);
}

// Contract: malformed timeout configuration fails before creating a private runtime
// or attempting to start the configured provider executable.
test("rejects an invalid summary timeout before worker or runtime startup", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-summary-invalid-timeout-"));
  const bin = join(root, "bin");
  const runtimes = join(root, "runtimes");
  const started = join(root, "started");
  try {
    await Promise.all([mkdir(bin), mkdir(runtimes)]);
    await writeFile(join(bin, "claude"), `#!/bin/sh\ntouch ${started}\n`);
    await chmod(join(bin, "claude"), 0o755);
    const runner = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};
try {
  await summarize({ text: "evidence", evidenceEventIds: ["event-a"] }, "claude", "fixture");
} catch (error) {
  console.log(error instanceof Error ? error.message : "unknown");
}
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", runner], {
      cwd: root,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, TMPDIR: runtimes, USER: "fixture-user", SUMMARY_TIMEOUT_MS: "not-a-number" },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(stdout).toContain("SUMMARY_TIMEOUT_MS must be a finite positive number");
    expect(await Bun.file(started).exists()).toBe(false);
    expect(await readdir(runtimes)).toEqual([]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
