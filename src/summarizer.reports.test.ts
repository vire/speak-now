import { expect, test } from "bun:test";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

async function runReportCase(mode: string) {
  const root = await mkdtemp(join(tmpdir(), "speak-now-summary-reports-"));
  try {
    const command = join(root, "claude");
    await writeFile(command, `#!${process.execPath}
import { writeFile } from "node:fs/promises";
await writeFile(${JSON.stringify(join(root, "launched"))}, "yes");
console.log(JSON.stringify({ type: "system", tools: [], mcp_servers: [] }));
console.log(JSON.stringify({ type: "result", is_error: ${mode !== "malformed"}, subtype: "success", result: "PRIVATE_EVIDENCE_MARKER" }));
if (${JSON.stringify(mode)} === "deadline") await new Promise(() => {});
process.exit(${mode === "malformed" ? 0 : 7});
`);
    await chmod(command, 0o755);
    const script = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};
import { createErrorReporter } from ${JSON.stringify(join(process.cwd(), "src/errors.ts"))};
import { readdir, mkdir } from "node:fs/promises";
import { join } from "node:path";
const root = ${JSON.stringify(root)};
const mode = ${JSON.stringify(mode)};
const reporter = createErrorReporter({dataDirectory: ${JSON.stringify(root)}});
const controller = new AbortController();
if (mode === "preabort") controller.abort();
const logger = mode === "schema" ? {log: async () => {
 const directory = (await readdir(root)).find((name) => name.startsWith("speak-now-summary-"));
 if (directory) await mkdir(join(root,directory,"summary-schema.json"));
}, flush: async () => {}, getStatus: () => ({destinationAvailable:true,pendingEntries:0,droppedEntries:0})} : undefined;
try {
 await summarize({text:"PRIVATE_EVIDENCE_MARKER",evidenceEventIds:["job-a"]},"claude","fixture",controller.signal,logger,undefined,{traceId:"a".repeat(32),spanId:"b".repeat(16),sourceId:"source-a",jobId:"job-a"},reporter);
} catch (error) { console.log(JSON.stringify({error:error.message})); }
await reporter.flush();
`;
    const child = Bun.spawn([process.execPath, "-e", script], { cwd: root, stdout: "pipe", stderr: "pipe", env: { PATH: `${root}:${process.env.PATH}`, HOME: root, TMPDIR: root, SUMMARY_TIMEOUT_MS: mode === "deadline" ? "1000" : "2000" } });
    const timer = setTimeout(() => child.kill(), 5_000);
    try {
      const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      expect(await child.exited, stderr).toBe(0);
      const file = Bun.file(join(root, "errors", "reports.jsonl"));
      const text = await file.exists() ? await file.text() : "";
      return { result: JSON.parse(stdout), text, reports: text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)), launched: await Bun.file(join(root, "launched")).exists() };
    } finally { clearTimeout(timer); if (child.exitCode === null) child.kill(); await child.exited; }
  } finally { await rm(root, { recursive: true, force: true }); }
}

test("nonzero summary output stays out of persisted error reports", async () => {
  const result = await runReportCase("nonzero");
  expect(result.reports).toHaveLength(1);
  expect(result.reports[0]).toMatchObject({ category: "subprocess", traceId: "a".repeat(32), spanId: "b".repeat(16), sourceId: "source-a", jobId: "job-a" });
  expect(result.text).not.toContain("PRIVATE_EVIDENCE_MARKER");
  expect(result.result.error).toContain("exit 7");
});

for (const mode of ["malformed", "deadline", "schema", "preabort"]) {
  test(`summary ${mode} has the expected persisted failure contract`, async () => {
    const result = await runReportCase(mode);
    if (mode === "preabort") {
      expect(result.reports).toHaveLength(0);
      expect(result.launched).toBe(false);
      expect(result.result.error).toContain("cancelled");
    } else {
      expect(result.reports).toHaveLength(1);
      expect(result.reports[0]).toMatchObject({ operation: "summary.process", category: mode === "deadline" ? "timeout" : mode === "malformed" ? "malformed_output" : "subprocess", traceId: "a".repeat(32), spanId: "b".repeat(16), sourceId: "source-a", jobId: "job-a" });
      expect(result.launched).toBe(mode !== "schema");
    }
  });
}
