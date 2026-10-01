import { expect, test } from "bun:test";
import { appendFile, chmod, mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("owned collector process watches new records and persists retry state", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-watch-"));
  const bin = join(root, "bin"); const data = join(root, "data"); const transcript = join(root, "transcript.jsonl");
  const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
  try {
    await mkdir(bin, { recursive: true });
    await Bun.write(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await Bun.write(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("old", "old café output")}`);
    await Bun.write(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await Bun.write(join(bin, "rg"), `#!/bin/sh\nprintf '%s\\n' ${transcript}\n`);
    await Bun.write(join(bin, "claude"), `#!/bin/sh\nif [ ! -f ${join(root, "once")} ]; then : > ${join(root, "once")}; exit 7; fi\nprintf '%s\\n' '{"type":"system","tools":[],"mcp_servers":[]}' '{"type":"result","subtype":"success","is_error":false,"result":"{\\"speak\\":false,\\"kind\\":\\"progress\\",\\"text\\":\\"stored\\",\\"evidenceEventIds\\":[]}"}'\n`);
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);
    const child = Bun.spawn([Bun.which("bun")!, "src/collector.ts"], { cwd: process.cwd(), stdin: "ignore", stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SOURCE_NAMESPACE: "fixture", SPEAK_NOW_DATA_DIR: data, SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture", POLL_MS: "20" } });
    for (let attempt = 0; attempt < 50; attempt++) {
      try { if (Object.keys(JSON.parse(await readFile(join(data, "collector-state.json"), "utf8")).cursors).length) break; } catch { /* collector has not persisted baseline */ }
      await Bun.sleep(20);
    }
    await appendFile(transcript, message("new", "new ASCII output"));
    for (let attempt = 0; attempt < 100; attempt++) {
      try { const jobs = JSON.parse(await readFile(join(data, "collector-state.json"), "utf8")).jobs; if (jobs.some((job: { done: boolean }) => job.done)) break; } catch { /* wait for atomic state */ }
      await Bun.sleep(20);
    }
    child.kill(); await child.exited;
    const saved = JSON.parse(await readFile(join(data, "collector-state.json"), "utf8"));
    expect(saved.jobs.some((job: { done: boolean }) => job.done), JSON.stringify(saved.jobs)).toBe(true);
    expect(saved.jobs.some((job: { activity: { text: string } }) => job.activity.text === "old café output")).toBe(false);
  } finally { await rm(root, { recursive: true, force: true }); }
});
