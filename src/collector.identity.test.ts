import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("collector keeps opaque exclusions and limited terminal fallback visible", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-identity-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  try {
    await mkdir(bin);
    await mkdir(data);
    await writeFile(join(root, "mode"), "first");
    await writeFile(join(root, "terminal.txt"), "large terminal line ".repeat(35_000));
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [
      { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a" },
      { agent: "codex", pane_id: "fixture:pane-a", terminal_id: "terminal-b" },
    ] } } }));
    await writeFile(
      join(bin, "herdr"),
      `#!/bin/sh
mode=$(cat ${join(root, "mode")})
if [ "$mode" = fail ]; then exit 7; fi
if [ "$1" = api ]; then /bin/cat ${join(root, "snapshot.json")}; exit 0; fi
/bin/cat ${join(root, "terminal.txt")}
`,
    );
    await chmod(join(bin, "herdr"), 0o755);
    const script = `
import { appendFile, writeFile } from "node:fs/promises";
import { initialState, runOnce } from "./src/collector.ts";
const root = ${JSON.stringify(root)};
const config = { sourceNamespace: "fixture", dataDir: ${JSON.stringify(data)}, excludePaneIds: ["raw:fixture:pane-a"] };
const state = initialState();
await runOnce(config, state);
const unchanged = await runOnce(config, state);
await appendFile(root + "/terminal.txt", "\\nnew terminal update");
await writeFile(root + "/mode", "second");
const fallback = await runOnce(config, state);
await writeFile(root + "/mode", "fail");
await runOnce(config, state);
const stale = state.topology?.source.stale;
const sourceError = state.sourceStatuses.some((status) => status.participantId === "source" && status.kind === "error");
await writeFile(root + "/mode", "second");
await runOnce(config, state);
console.log(JSON.stringify({
  participants: state.topology?.participants.map((participant) => participant.rawPaneId),
  unchanged: unchanged.activities.length,
  fallback: fallback.activities.map((activity) => ({ text: activity.text, status: activity.status, captureMode: activity.captureMode })),
  limited: state.sourceStatuses.some((status) => status.kind === "limited"),
  stale,
  sourceError,
  recovered: state.topology?.source.stale === false,
}));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: process.cwd(),
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "codex" },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual({
      participants: ["pane-a"],
      unchanged: 0,
      fallback: [{ text: "new terminal update", status: "limited", captureMode: "terminal" }],
      limited: true,
      stale: true,
      sourceError: true,
      recovered: true,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 10_000);
