import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

async function runReportCase(mode: string) {
  const root = await mkdtemp(join(tmpdir(), "speak-now-summary-reports-"));
  try {
    await Promise.all([mkdir(join(root, "errors")), mkdir(join(root,"traces"))]);
    const command = join(root, "claude");
    const summary = { speak: true, kind: "progress", text: mode === "text-bound" ? "word ".repeat(61) : "safe", evidenceEventIds: mode === "unknown-evidence" ? ["not-known"] : ["job-a"] };
    const validCarrier = ["unknown-evidence", "text-bound", "cleanup-success", "stdout-edge", "stdout-overflow", "stderr-edge", "stderr-overflow"].includes(mode);
    await writeFile(command, `#!${process.execPath}
import { chmod, writeFile } from "node:fs/promises";
await writeFile(${JSON.stringify(join(root, "launched"))}, "yes");
const output = JSON.stringify({ type: "system", tools: [], mcp_servers: [] }) + "\\n" + JSON.stringify({ type: "result", is_error: ${!(mode === "malformed" || validCarrier)}, subtype: "success", result: ${JSON.stringify(validCarrier ? JSON.stringify(summary) : "PRIVATE_EVIDENCE_MARKER")} }) + "\\n";
const mode = ${JSON.stringify(mode)};
if (mode.startsWith("stderr-")) process.stderr.write("é".repeat(50000) + (mode.endsWith("overflow") ? "x" : ""));
process.stdout.write(mode.startsWith("stdout-") ? " ".repeat(200000 - Buffer.byteLength(output) + (mode.endsWith("overflow") ? 1 : 0)) + output : output);
if (${JSON.stringify(mode)} === "deadline") await new Promise(() => {});
if (${JSON.stringify(mode)}.startsWith("cleanup-")) await chmod(${JSON.stringify(root)}, 0o555);
process.exit(${mode === "malformed" || validCarrier ? 0 : 7});
`);
    await chmod(command, 0o755);
    const script = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};
import { createErrorReporter } from ${JSON.stringify(join(process.cwd(), "src/errors.ts"))};
import { createTracer } from ${JSON.stringify(join(process.cwd(), "src/tracing.ts"))};
import { readdir, mkdir } from "node:fs/promises";
import { join } from "node:path";
const root = ${JSON.stringify(root)};
const mode = ${JSON.stringify(mode)};
const reporter = createErrorReporter({dataDirectory: ${JSON.stringify(root)}});
const tracer = createTracer({dataDirectory:root});
const controller = new AbortController();
if (mode === "preabort") controller.abort();
const logger = ["schema", "failure-late-abort", "unexpected"].includes(mode) ? {log: async (level) => {
 if (mode === "unexpected") throw new Error("PRIVATE_EVIDENCE_MARKER");
 if (mode === "failure-late-abort") { if (level === "error") controller.abort(); return; }
 const directory = (await readdir(root)).find((name) => name.startsWith("speak-now-summary-"));
 if (directory) await mkdir(join(root,directory,"summary-schema.json"));
}, flush: async () => {}, getStatus: () => ({destinationAvailable:true,pendingEntries:0,droppedEntries:0})} : undefined;
try {
 const summary = await summarize({text:"PRIVATE_EVIDENCE_MARKER",evidenceEventIds:["job-a"]},"claude","fixture",controller.signal,logger,tracer,{traceId:"a".repeat(32),spanId:"b".repeat(16),sourceId:"source-a",jobId:"job-a"},reporter);
 console.log(JSON.stringify({summary}));
} catch (error) { console.log(JSON.stringify({error:error.message})); }
await reporter.flush();
await tracer.flush();
`;
    const child = Bun.spawn([process.execPath, "-e", script], { cwd: root, stdout: "pipe", stderr: "pipe", env: { PATH: `${root}:${process.env.PATH}`, HOME: root, TMPDIR: root, SUMMARY_TIMEOUT_MS: mode === "deadline" ? "1000" : "2000" } });
    const timer = setTimeout(() => child.kill(), 5_000);
    try {
      const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
      expect(await child.exited, stderr).toBe(0);
      const file = Bun.file(join(root, "errors", "reports.jsonl"));
      await chmod(root, 0o755);
      const text = await file.exists() ? await file.text() : "";
      const traceFiles = (await import("node:fs/promises").then(async ({readdir}) => {
        const directory = join(root,"traces");
        return (await Promise.all((await readdir(directory)).filter((name) => name.startsWith("writer-")).map(async (name) => {
          const writer = join(directory,name);
          return await Promise.all((await readdir(writer)).filter((file) => file.endsWith(".jsonl")).map((file) => Bun.file(join(writer,file)).text()));
        }))).flat().flatMap((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)));
      }));
      return { traces: traceFiles, result: JSON.parse(stdout), text, reports: text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)), launched: await Bun.file(join(root, "launched")).exists() };
    } finally { clearTimeout(timer); if (child.exitCode === null) child.kill(); await child.exited; }
  } finally { await chmod(root, 0o755); await rm(root, { recursive: true, force: true }); }
}

test("nonzero summary output stays out of persisted error reports", async () => {
  const result = await runReportCase("nonzero");
  expect(result.reports).toHaveLength(1);
  expect(result.reports[0]).toMatchObject({ category: "subprocess", traceId: "a".repeat(32), sourceId: "source-a", jobId: "job-a" });
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
      expect(result.reports[0]).toMatchObject({ operation: "summary.process", category: mode === "deadline" ? "timeout" : mode === "malformed" ? "malformed_output" : "subprocess", traceId: "a".repeat(32), sourceId: "source-a", jobId: "job-a" });
      expect(result.launched).toBe(mode !== "schema");
    }
  });
}

for (const mode of ["unknown-evidence", "text-bound"]) {
  test(`invalid summary ${mode} is reported as malformed output`, async () => {
    const result = await runReportCase(mode);
    expect(result.reports).toHaveLength(1);
    expect(result.reports[0].category).toBe("malformed_output");
  });
}

for (const mode of ["cleanup-success", "cleanup-primary"]) {
  test(`summary ${mode} preserves cleanup failure precedence`, async () => {
    const result = await runReportCase(mode);
    expect(result.reports).toHaveLength(1);
    expect(result.reports[0].category).toBe("subprocess");
    expect(result.result.error).toContain(mode === "cleanup-primary" ? "exit 7" : "remove private summary runtime");
    expect(result.text).not.toContain("PRIVATE_EVIDENCE_MARKER");
  });
}

test("summary failure remains failed when diagnostics trigger a later abort", async () => {
  const result = await runReportCase("failure-late-abort");
  expect(result.reports).toHaveLength(1);
  expect(result.result.error).toContain("exit 7");
  expect(result.traces.find((span) => span.operation === "summary.process").outcome).toBe("failed");
  expect(result.traces.find((span) => span.operation === "summary.cli").outcome).toBe("failed");
});

for (const mode of ["stdout-edge", "stdout-overflow", "stderr-edge", "stderr-overflow"]) {
  test(`summary ${mode} preserves the UTF-8 byte limit`, async () => {
    const result = await runReportCase(mode);
    expect(result.reports).toHaveLength(mode.endsWith("overflow") ? 1 : 0);
    if (mode.endsWith("overflow")) {
      expect(result.result.error).toContain("output exceeded its limit");
      expect(result.reports[0].category).toBe("subprocess");
    }
  });
}

test("unexpected worker failures use a fixed safe diagnostic", async () => {
  const result = await runReportCase("unexpected");
  expect(result.reports).toHaveLength(1);
  expect(result.reports[0].category).toBe("subprocess");
  expect(result.result.error).toBe("Unexpected summary worker failure");
  expect(result.text).not.toContain("PRIVATE_EVIDENCE_MARKER");
  expect(result.launched).toBe(false);
});
