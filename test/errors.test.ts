import { mkdtemp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { expect, test } from "bun:test";
import { createErrorReporter } from "../src/errors";

test("writes a redacted correlated error report", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-errors-"));
  try {
    const reporter = createErrorReporter({ dataDirectory });
    const reportId = await reporter.report({
      service: "collector",
      operation: "summary.process",
      category: "subprocess",
      error: new Error("provider failed with Bearer synthetic-secret"),
      trace: { traceId: "a".repeat(32), spanId: "b".repeat(16), sourceId: "source-a", jobId: "job-a" },
      context: { stderr: "raw worker output", authorization: "Basic synthetic-secret", attempt: 1 },
    });
    await reporter.flush();

    const [record] = (await readFile(join(dataDirectory, "errors", "reports.jsonl"), "utf8")).trim().split("\n").map(JSON.parse);
    expect(record).toMatchObject({ reportId, service: "collector", operation: "summary.process", category: "subprocess", traceId: "a".repeat(32), spanId: "b".repeat(16), sourceId: "source-a", jobId: "job-a", metadata: { attempt: 1 } });
    expect(new Date(record.timestamp).toISOString()).toBe(record.timestamp);
    expect(record.message).toContain("Bearer [REDACTED]");
    expect(JSON.stringify(record)).not.toContain("synthetic-secret");
    expect(JSON.stringify(record)).not.toContain("raw worker output");
  } finally {
    await rm(dataDirectory, { recursive: true, force: true });
  }
});

test("allows only flat report context and expires old archive rows by timestamp", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-errors-retention-"));
  try {
    const errors = join(dataDirectory, "errors");
    await mkdir(errors);
    const old = new Date(Date.now() - 500).toISOString();
    const current = new Date().toISOString();
    await writeFile(join(errors, "app.seed.jsonl"), `${JSON.stringify({ timestamp: old, reportId: "old", message: "expired" })}\n${JSON.stringify({ timestamp: current, reportId: "current", message: "retain" })}\n`);
    const reporter = createErrorReporter({ dataDirectory, retentionDays: 250 / 86_400_000 });
    await reporter.report({ service: "app", operation: "report.context", category: "fixture", error: new Error("safe"), context: { attempt: 1, nested: { fileContents: "private bytes" }, arbitrary: "private value" } });
    await reporter.flush();
    const rows = (await Promise.all((await readdir(errors)).filter((name) => name.endsWith(".jsonl")).map((name) => readFile(join(errors, name), "utf8")))).flatMap((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)));
    expect(rows.some((row) => row.reportId === "old")).toBe(false);
    expect(rows.some((row) => row.reportId === "current")).toBe(true);
    const report = rows.find((row) => row.operation === "report.context");
    expect(report.metadata).toEqual({ attempt: 1 });
  } finally { await rm(dataDirectory, { recursive: true, force: true }); }
});

test("keeps reporting failure bounded and visible", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-errors-fallback-"));
  const blocked = join(root, "not-a-directory");
  await writeFile(blocked, "blocked");
  try {
    const reporter = createErrorReporter({ dataDirectory: blocked });
    await expect(reporter.report({ service: "app", operation: "server.client-errors", category: "request", error: new Error("password=synthetic-secret") })).resolves.toMatch(/^[a-f0-9]{32}$/);
    await reporter.flush();
    expect(reporter.getStatus()).toMatchObject({ destinationAvailable: false, pendingEntries: 0 });
    expect(reporter.getStatus().lastFailure).not.toContain("synthetic-secret");
    await rm(blocked);
    await reporter.report({ service: "app", operation: "recovered", category: "fixture", error: new Error("safe") });
    await reporter.flush();
    expect(reporter.getStatus().destinationAvailable).toBe(true);
    const reports = (await readFile(join(blocked, "errors", "reports.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line));
    expect(reports).toHaveLength(1);
    expect(reports[0].operation).toBe("recovered");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("filters expired rows during later live reporter maintenance", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-errors-live-age-"));
  try {
    const reporter = createErrorReporter({ dataDirectory, retentionDays: 120 / 86_400_000 });
    await reporter.report({ service: "app", operation: "row-a", category: "fixture", error: new Error("a") });
    await Bun.sleep(80);
    await reporter.report({ service: "app", operation: "row-b", category: "fixture", error: new Error("b") });
    await Bun.sleep(80);
    await reporter.report({ service: "app", operation: "row-c", category: "fixture", error: new Error("c") });
    await reporter.flush();
    const rows = (await readFile(join(dataDirectory, "errors", "reports.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line).operation);
    expect(rows).toEqual(["row-b", "row-c"]);
  } finally { await rm(dataDirectory, { recursive: true, force: true }); }
});

test("retains live reports beyond the startup tail budget", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-errors-tail-"));
  try {
    const reporter = createErrorReporter({ dataDirectory }); const ids: string[] = [];
    for (let index = 0; index < 100; index += 1) ids.push(await reporter.report({ service: "app", operation: "tail-budget", category: "fixture", error: new Error(`${index}:${"x".repeat(900)}`) }));
    await reporter.flush();
    const files = await readdir(join(dataDirectory, "errors"));
    expect(files.filter((name) => name.endsWith(".jsonl"))).toEqual(["reports.jsonl"]);
    const records = (await readFile(join(dataDirectory, "errors", "reports.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line).reportId);
    expect(records).toEqual(ids);
  } finally { await rm(dataDirectory, { recursive: true, force: true }); }
});
