import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("collector persists Claude revisions and sends bounded latest evidence to the worker", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-evidence-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");

  try {
    await mkdir(bin);
    await mkdir(data);

    const record = (uuid: string, messageId: string, text: string) => `${JSON.stringify({
      type: "assistant",
      sessionId: "session-a",
      uuid,
      message: { id: messageId, role: "assistant", content: [{ type: "text", text }] },
    })}\n`;
    await writeFile(transcript, record("baseline-record", "baseline-message", "baseline"));
    await writeFile(
      join(root, "snapshot.json"),
      JSON.stringify({
        result: {
          snapshot: {
            agents: [{
              agent: "claude",
              pane_id: "pane-a",
              terminal_id: "terminal-a",
              agent_session: { kind: "id", source: "fixture", value: "session-a" },
            }],
          },
        },
      }),
    );
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    await writeFile(
      join(bin, "claude"),
      `#!${Bun.which("bun")!}
import { appendFile, readFile } from "node:fs/promises";
const prompt = await readFile(0, "utf8");
await appendFile(${JSON.stringify(join(root, "prompts"))}, prompt + "\\n---\\n");
console.log(JSON.stringify({ type: "system", tools: [], mcp_servers: [] }));
console.log(JSON.stringify({
  type: "result",
  subtype: "success",
  is_error: false,
  result: JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: [] }),
}));
`,
    );
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);

    const script = `
import { appendFile, readFile } from "node:fs/promises";
import { initialState, restore, runOnce } from "./src/collector.ts";

const data = ${JSON.stringify(data)};
const transcript = ${JSON.stringify(transcript)};
const config = { sourceNamespace: "fixture", dataDir: data, excludePaneIds: [] };
const record = (uuid, messageId, text) => JSON.stringify({
  type: "assistant",
  sessionId: "session-a",
  uuid,
  message: { id: messageId, role: "assistant", content: [{ type: "text", text }] },
}) + "\\n";
const state = initialState();
await runOnce(config, state);

await appendFile(transcript, record("revision-one", "message-a", "revision alpha"));
const first = await runOnce(config, state);
const restarted = await restore(data);

await appendFile(transcript, record("revision-copy", "message-a", "revision alpha"));
const equivalent = await runOnce(config, restarted);
await appendFile(transcript, record("revision-two", "message-a", "revision beta"));
const changed = await runOnce(config, restarted);
await appendFile(transcript, record("separate-message", "message-b", "revision beta"));
const distinct = await runOnce(config, restarted);

const decisive = "DECISIVE_RESULT";
await appendFile(transcript, "{bad}\\n" + record("long-record", "message-long", "prefix😀".repeat(20_000) + decisive));
const long = await runOnce(config, restarted);
const prompts = await readFile(${JSON.stringify(join(root, "prompts"))}, "utf8");
const longActivity = long.activities.find((activity) => activity.text.endsWith(decisive));
console.log(JSON.stringify({
  first: first.activities.length,
  equivalent: equivalent.activities.length,
  changed: changed.activities.length,
  distinct: distinct.activities.length,
  long: {
    truncated: longActivity?.truncated,
    excerpt: longActivity?.excerpt,
    includesDecisive: longActivity?.text.endsWith(decisive),
    bytes: Buffer.byteLength(longActivity?.text ?? ""),
    originalBytes: longActivity?.originalTextBytes,
    id: longActivity?.id,
  },
  promptHasGap: prompts.includes("capture-status=gap"),
  promptHasTail: prompts.includes("capture-status=partial; excerpt=tail"),
  promptHasDecisive: prompts.includes(decisive),
  promptHasIncludedId: Boolean(longActivity?.id && prompts.includes("Evidence IDs: " + longActivity.id)),
}));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: process.cwd(),
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: `${bin}:${process.env.PATH}`,
        HOME: root,
        SUMMARY_BACKEND: "claude",
        SUMMARY_MODEL: "fixture",
      },
    });
    const timer = setTimeout(() => child.kill(), 15_000);
    try {
      const [stdout, stderr] = await Promise.all([
        new Response(child.stdout).text(),
        new Response(child.stderr).text(),
      ]);
      await child.exited;
      expect(child.exitCode, stderr).toBe(0);
      const result = JSON.parse(stdout) as {
        first: number;
        equivalent: number;
        changed: number;
        distinct: number;
        long: { truncated: boolean; excerpt: string; includesDecisive: boolean; bytes: number; originalBytes: number; id: string };
        promptHasGap: boolean;
        promptHasTail: boolean;
        promptHasDecisive: boolean;
        promptHasIncludedId: boolean;
      };
      expect(result.first).toBe(1);
      expect(result.equivalent).toBe(0);
      expect(result.changed).toBe(1);
      expect(result.distinct).toBe(1);
      expect(result.long.truncated).toBe(true);
      expect(result.long.excerpt).toBe("tail");
      expect(result.long.includesDecisive).toBe(true);
      expect(result.long.bytes).toBeLessThanOrEqual(20_000);
      expect(result.long.originalBytes).toBeGreaterThan(20_000);
      expect(result.promptHasGap).toBe(true);
      expect(result.promptHasTail).toBe(true);
      expect(result.promptHasDecisive).toBe(true);
      expect(result.promptHasIncludedId).toBe(true);
    } finally {
      clearTimeout(timer);
      if (child.exitCode === null) child.kill();
      await child.exited;
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
