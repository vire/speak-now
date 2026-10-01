import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("collector binds selected capture to an exact file incarnation and candidate", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-ownership-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const source = join(root, "source.jsonl");
  try {
    await mkdir(bin);
    await mkdir(data);
    const meta = (id: string) => `${JSON.stringify({ type: "session_meta", payload: { id } })}\n`;
    const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
    await writeFile(source, meta("session-a") + message("baseline", "baseline"));
    await writeFile(join(root, "paths"), `${source}\n`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/cat ${join(root, "paths")}\n`);
    for (const name of ["herdr", "rg"]) await chmod(join(bin, name), 0o755);
    const script = `
import { appendFile, readFile, rename, writeFile } from "node:fs/promises";
import { initialState, restore, runOnce } from "./src/collector.ts";
const root = ${JSON.stringify(root)};
const source = ${JSON.stringify(source)};
const data = ${JSON.stringify(data)};
const meta = (id) => JSON.stringify({ type: "session_meta", payload: { id } }) + "\\n";
const message = (id, text) => JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } }) + "\\n";
const config = { sourceNamespace: "fixture", dataDir: data, excludePaneIds: [] };
const state = initialState();
await runOnce(config, state);
const offset = state.cursors[Object.keys(state.cursors)[0]].offset;
const replacement = source + ".replacement";
await writeFile(replacement, JSON.stringify({ type: "event_msg", payload: { type: "token_count" } }) + "\\n" + meta("session-b") + " ".repeat(offset) + "\\n" + message("wrong", "unrelated output"));
await rename(replacement, source);
const replaced = await runOnce(config, state);
await writeFile(source, meta("session-a") + message("baseline", "baseline"));
const fresh = initialState();
await runOnce(config, fresh);
await appendFile(source, meta("session-b") + message("wrong", "unrelated output"));
await runOnce(config, fresh);
const restored = await restore(data);
await appendFile(source, message("after-mismatch", "still unrelated"));
const afterMismatch = await runOnce(config, restored);
const healthy = source + ".healthy";
await writeFile(healthy, meta("session-a") + message("baseline", "baseline"));
const unrelated = source + ".unrelated";
await writeFile(unrelated, meta("session-b") + message("decoy", "session-a content decoy"));
await writeFile(root + "/paths", unrelated + "\\n" + healthy + "\\n");
const candidate = initialState();
await runOnce(config, candidate);
await appendFile(healthy, message("new", "healthy selected update"));
const selected = await runOnce(config, candidate);
console.log(JSON.stringify({
  replacementGap: replaced.activities.some((activity) => activity.status === "gap"),
  replacementUnrelated: replaced.activities.some((activity) => activity.text === "unrelated output"),
  mismatchRevoked: restored.cursors[Object.keys(restored.cursors)[0]]?.exactSessionVerified === false,
  afterMismatchGap: afterMismatch.activities.some((activity) => activity.status === "gap"),
  afterMismatchUnrelated: afterMismatch.activities.some((activity) => activity.text === "still unrelated"),
  candidateHealthy: selected.activities.some((activity) => activity.text === "healthy selected update"),
}));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "codex" } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual({ replacementGap: true, replacementUnrelated: false, mismatchRevoked: true, afterMismatchGap: true, afterMismatchUnrelated: false, candidateHealthy: true });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 12_000);
