import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("overflow leaves a source unacknowledged until completed work reclaims capacity", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-overflow-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");

  try {
    await mkdir(bin);
    await mkdir(data);

    const message = (id: string, text: string) => `${JSON.stringify({
      type: "response_item",
      payload: {
        type: "message",
        role: "assistant",
        phase: "commentary",
        id,
        content: [{ type: "output_text", text }],
      },
    })}\n`;

    await writeFile(
      transcript,
      `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline")}`,
    );
    await writeFile(
      join(root, "snapshot.json"),
      JSON.stringify({
        result: {
          snapshot: {
            agents: [{
              agent: "codex",
              pane_id: "pane-a",
              terminal_id: "terminal-a",
              agent_session: { kind: "id", source: "fixture", value: "session-a" },
            }],
          },
        },
      }),
    );
    await writeFile(join(root, "worker-mode"), "fail");
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    await writeFile(
      join(bin, "claude"),
      `#!/usr/bin/env bun
const mode = (await Bun.file(${JSON.stringify(join(root, "worker-mode"))}).text()).trim();
if (mode === "fail") process.exit(7);
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
import { appendFile, readFile, writeFile } from "node:fs/promises";
import { initialState, runOnce } from "./src/collector.ts";

const root = ${JSON.stringify(root)};
const data = ${JSON.stringify(data)};
const transcript = ${JSON.stringify(transcript)};
const state = initialState();
const config = { sourceNamespace: "fixture", dataDir: data, excludePaneIds: [] };
const message = (id, text) => JSON.stringify({
  type: "response_item",
  payload: {
    type: "message",
    role: "assistant",
    phase: "commentary",
    id,
    content: [{ type: "output_text", text }],
  },
}) + "\\n";

await runOnce(config, state);
await appendFile(transcript, Array.from({ length: 101 }, (_, index) => message("fill-" + index, "retained " + index)).join(""));
    const admission = await runOnce(config, state);

const participantId = Object.keys(state.cursors)[0];
const acknowledgedOffset = state.cursors[participantId].offset;

const overflow = state.sourceStatuses.some((status) => status.kind === "overflow");
const unacknowledged = acknowledgedOffset < (await (await import("node:fs/promises")).stat(transcript)).size;
const dedupUnchanged = state.activityIds.size === 100;
await writeFile(root + "/worker-mode", "success");
await runOnce(config, state);
const recovery = await runOnce(config, state);
const repeat = await runOnce(config, state);

const saved = JSON.parse(await readFile(data + "/collector-state.json", "utf8"));
console.log(JSON.stringify({
  overflow,
  unacknowledged,
  dedupUnchanged,
  admittedPrefix: admission.activities.length === 100,
  capturedOnce: recovery.activities.length === 1 && recovery.activities[0].text === "retained 100" && repeat.activities.length === 0,
  reclaimed: saved.jobs.every((job) => job.done && job.activity.text === ""),
}));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: `${bin}:${process.env.PATH}`,
        HOME: root,
        SUMMARY_BACKEND: "claude",
        SUMMARY_MODEL: "fixture",
      },
    });
    const output = await new Response(child.stdout).text();
    const error = await new Response(child.stderr).text();
    await child.exited;

    expect(child.exitCode, error).toBe(0);
    expect(JSON.parse(output)).toEqual({
      overflow: true,
      unacknowledged: true,
      dedupUnchanged: true,
      admittedPrefix: true,
      capturedOnce: true,
      reclaimed: true,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 20_000);
