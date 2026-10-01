import { expect, test } from "bun:test";
import { appendFile, chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("owned collector exits promptly when SIGTERM interrupts a hanging summary", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-cancel-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;

  try {
    await mkdir(bin);
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline")}`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    await writeFile(join(bin, "claude"), `#!/bin/sh\ntouch ${join(root, "summary-started")}\ntrap 'touch ${join(root, "summary-stopped")}; exit 0' TERM INT\nwhile :; do sleep 1; done\n`);
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);

    const child = Bun.spawn([Bun.which("bun")!, "src/collector.ts"], {
      cwd: process.cwd(),
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: `${bin}:${process.env.PATH}`,
        HOME: root,
        SOURCE_NAMESPACE: "fixture",
        SPEAK_NOW_DATA_DIR: data,
        SUMMARY_BACKEND: "claude",
        SUMMARY_MODEL: "fixture",
        POLL_MS: "10",
        SUMMARY_TIMEOUT_MS: "30000",
      },
    });
    const deadline = Date.now() + 3_000;
    try {
      while (!await Bun.file(join(root, "summary-started")).exists() && Date.now() < deadline) {
        try {
          await appendFile(transcript, message("update", "cancel this summary"));
        } catch {
          // The fixture transcript is always owned by this test.
        }
        await Bun.sleep(20);
      }
      expect(await Bun.file(join(root, "summary-started")).exists()).toBe(true);
      child.kill("SIGTERM");
      const exited = await Promise.race([
        child.exited.then(() => true),
        Bun.sleep(1_500).then(() => false),
      ]);
      expect(exited).toBe(true);
      expect(await Bun.file(join(root, "summary-stopped")).exists()).toBe(true);
    } finally {
      if (child.exitCode === null) child.kill();
      await child.exited;
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 8_000);

test("collector reaps an uncooperative observation command after cancellation", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-observation-stop-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  try {
    await mkdir(bin);
    await writeFile(join(bin, "herdr"), `#!/bin/sh\necho $$ > ${join(root, "pid")}\ntouch ${join(root, "started")}\ntrap '' TERM\nwhile :; do sleep 1; done\n`);
    await chmod(join(bin, "herdr"), 0o755);
    const script = `
import { initialState, runOnce } from "./src/collector.ts";
const controller = new AbortController();
const pending = runOnce({ sourceNamespace: "fixture", dataDir: ${JSON.stringify(data)}, excludePaneIds: [] }, initialState(), controller.signal)
  .then(() => "completed", (error) => error instanceof Error ? error.message : "failed");
const started = ${JSON.stringify(join(root, "started"))};
const deadline = Date.now() + 1_000;
while (!(await Bun.file(started).exists()) && Date.now() < deadline) await Bun.sleep(10);
if (!(await Bun.file(started).exists())) throw new Error("observation stand-in did not start");
controller.abort();
console.log(JSON.stringify({ result: await pending }));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: { PATH: `${bin}:${process.env.PATH}`, HOME: root },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout).result).toContain("cancelled");
    const pid = Number((await readFile(join(root, "pid"), "utf8")).trim());
    expect(() => process.kill(pid, 0)).toThrow();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 5_000);

test("collector baselines a verified large existing transcript at its join-time high-water mark", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-baseline-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  try {
    await mkdir(bin);
    const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${Array.from({ length: 120 }, (_, index) => message(`old-${index}`, "old ".repeat(800))).join("")}`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    for (const name of ["herdr", "rg"]) await chmod(join(bin, name), 0o755);
    const script = `
import { appendFile } from "node:fs/promises";
import { initialState, runOnce } from "./src/collector.ts";
const transcript = ${JSON.stringify(transcript)};
const state = initialState();
const config = { sourceNamespace: "fixture", dataDir: ${JSON.stringify(data)}, excludePaneIds: [] };
const message = (id, text) => JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } }) + "\\n";
await runOnce(config, state);
const replay = await runOnce(config, state);
await appendFile(transcript, message("live", "new visible output"));
const live = await runOnce(config, state);
console.log(JSON.stringify({ replay: replay.activities.length, live: live.activities.map((activity) => activity.text) }));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "codex" } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual({ replay: 0, live: ["new visible output"] });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 8_000);

test("collector resumes bounded scans and sends a supported long-record tail to the worker", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-frame-"));
  const bin = join(root, "bin");
  const data = join(root, "data");
  const transcript = join(root, "source.jsonl");
  const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
  try {
    await mkdir(bin);
    await mkdir(data);
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline")}`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    await writeFile(join(bin, "claude"), `#!/usr/bin/env bun
import { appendFile, readFile } from "node:fs/promises";
const prompt = await readFile(0, "utf8");
await appendFile(${JSON.stringify(join(root, "worker-prompts.jsonl"))}, JSON.stringify({
  decisive: prompt.includes("DECISIVE_RESULT"),
  tailMetadata: prompt.includes("capture-status=partial; excerpt=tail"),
  evidenceId: /Evidence id=[a-f0-9]{64}/.test(prompt),
}) + "\\n");
console.log(JSON.stringify({ type: "system", tools: [], mcp_servers: [] }));
console.log(JSON.stringify({ type: "result", subtype: "success", is_error: false, result: JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: [] }) }));
`);
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);
    const script = `
import { appendFile } from "node:fs/promises";
import { initialState, runOnce } from "./src/collector.ts";
const transcript = ${JSON.stringify(transcript)};
const state = initialState();
const config = { sourceNamespace: "fixture", dataDir: ${JSON.stringify(data)}, excludePaneIds: [] };
const message = (id, text) => JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } }) + "\\n";
await runOnce(config, state);
await appendFile(transcript, message("oversized", "x".repeat(1_100_000) + " DECISIVE_RESULT") + message("later", "later visible output"));
const scanning = await runOnce(config, state);
await runOnce(config, state);
await runOnce(config, state);
const supported = await runOnce(config, state);
const later = await runOnce(config, state);
const worker = (await (await import("node:fs/promises")).readFile(${JSON.stringify(join(root, "worker-prompts.jsonl"))}, "utf8")).trim().split("\\n").map(JSON.parse);
console.log(JSON.stringify({
  scanningGap: scanning.activities.some((activity) => activity.status === "gap"),
  supported: {
    status: supported.activities[0]?.status,
    excerpt: supported.activities[0]?.excerpt,
    tail: supported.activities[0]?.text.endsWith("DECISIVE_RESULT"),
  },
  later: later.activities.map((activity) => activity.text),
  worker: worker.some((entry) => entry.decisive && entry.tailMetadata && entry.evidenceId),
}));
`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(JSON.parse(stdout)).toEqual({
      scanningGap: true,
      supported: { status: "partial", excerpt: "tail", tail: true },
      later: ["later visible output"],
      worker: true,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}, 8_000);
