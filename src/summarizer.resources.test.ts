import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

// Contract: an over-producing provider is bounded, terminated, and reaped.
// Regression: a provider that ignores SIGTERM previously left summarize waiting forever.
test("reaps a provider that ignores SIGTERM after bounded concurrent drains", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-summary-resource-"));
  const bin = join(root, "bin");
  const workerPid = join(root, "worker.pid");
  try {
    await mkdir(bin);
    await writeFile(join(bin, "claude"), `#!/bin/sh
echo $$ > ${workerPid}
trap '' TERM
while :; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; done
`);
    await chmod(join(bin, "claude"), 0o755);
    const script = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};
try {
  await summarize({ text: "evidence", evidenceEventIds: ["event-a"] }, "claude", "fixture");
} catch (error) {
  console.log(error instanceof Error ? error.message : "unknown");
}
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: root,
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, USER: "fixture-user" },
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
      expect(child.exitCode, stderr).toBe(0);
      expect(stdout).toContain("output exceeded its limit");
    } finally {
      clearTimeout(guard);
      await stopFixture();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 4_000);
