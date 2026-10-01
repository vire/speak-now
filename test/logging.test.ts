import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createLogger } from "../src/logging";

const directories: string[] = [];

async function temporaryDirectory(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "sn-log-"));
  directories.push(directory);
  return directory;
}

async function logFiles(directory: string, service: string): Promise<string[]> {
  const logs = join(directory, "logs");
  return (await readdir(logs)).filter((name) => name.startsWith(`${service}.`) && name.endsWith(".jsonl"));
}

afterEach(async () => {
  await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

describe("file logger", () => {
  test("writes filtered, structured, redacted app records", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "app", dataDirectory, minimumLevel: "info" });

    await logger.log("debug", { operation: "ignored", message: "ignore" });
    await logger.log("error", {
      operation: "summary.invoke",
      message: `Bearer synthetic-one, Authorization: Basic synthetic-two, {"api_key":"synthetic-three"}, {"access_token":"synthetic-four${"x".repeat(2_000)}"}, https://user:synthetic-five@example.invalid`,
      sourceId: "source-1",
      participantId: "participant-1",
      jobId: "job-1",
      outcome: "failed",
      metadata: {
        apiKey: "do-not-write-this",
        nested: { authorization: "Basic do-not-write-this", url: "https://example.invalid/?token=do-not-write-this" },
        transcript: "a complete captured update that must not be logged",
      },
    });
    await logger.flush();

    const text = await readFile(join(dataDirectory, "logs", "app.jsonl"), "utf8");
    const [record] = text.trim().split("\n").map(JSON.parse);
    expect(record).toMatchObject({ level: "error", service: "app", operation: "summary.invoke", sourceId: "source-1", outcome: "failed" });
    expect(new Date(record.timestamp).toISOString()).toBe(record.timestamp);
    expect(text).not.toContain("do-not-write-this");
    expect(text).not.toContain("synthetic-one");
    expect(text).not.toContain("synthetic-two");
    expect(text).not.toContain("synthetic-three");
    expect(text).not.toContain("synthetic-four");
    expect(text).not.toContain("synthetic-five");
    expect(text).not.toContain("complete captured update");
    expect(record.metadata.transcript).toBe("[OMITTED]");

    await logger.log("error", { operation: "summary.invoke", message: "https://user:synthetic-seven@example.invalid" });
    await logger.flush();
    expect(await readFile(join(dataDirectory, "logs", "app.jsonl"), "utf8")).not.toContain("synthetic-seven");
  });

  test("serializes concurrent appends", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "collector", dataDirectory, maxFileBytes: 20_000 });
    await Promise.all(Array.from({ length: 30 }, (_, index) => logger.log("info", { operation: "capture.read", message: `record ${index}`, metadata: { index } })));
    await logger.flush();

    const text = await readFile(join(dataDirectory, "logs", "collector.jsonl"), "utf8");
    expect(text.trim().split("\n")).toHaveLength(30);
  });

  test("coordinates multiple public loggers sharing one rotating destination", async () => {
    const dataDirectory = await temporaryDirectory();
    const options = { service: "app" as const, dataDirectory, maxFileBytes: 256, maxRecordBytes: 256, maxRetainedFiles: 200 };
    const loggers = Array.from({ length: 16 }, () => createLogger(options));
    const ids = Array.from({ length: 160 }, (_, index) => `accepted-${index}`);
    await Promise.all(ids.map((id, index) => loggers[index % loggers.length].log("info", { operation: "capture.read", message: id })));
    await Promise.all(loggers.map((logger) => logger.flush()));

    const names = await logFiles(dataDirectory, "app");
    const texts = await Promise.all(names.map((name) => readFile(join(dataDirectory, "logs", name), "utf8")));
    const records = texts.flatMap((text) => text.trim().split("\n").filter(Boolean).map(JSON.parse));
    expect(records.map((record) => record.message).sort()).toEqual(ids.sort());
    await Promise.all(names.map(async (name) => expect((await stat(join(dataDirectory, "logs", name))).size).toBeLessThanOrEqual(256)));
  });

  test("contains preparation failures and metadata traversal within the public boundary", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "app", dataDirectory, maxMetadataBytes: 64 });
    const failedRead = {} as Record<string, unknown>;
    Object.defineProperty(failedRead, "broken", { enumerable: true, get: () => { throw new Error("password=synthetic-five"); } });
    await expect(logger.log("error", { operation: "capture.read", message: "safe", metadata: failedRead })).resolves.toBeUndefined();
    await logger.flush();
    const prepared = await readFile(join(dataDirectory, "logs", "app.jsonl"), "utf8");
    expect(prepared).toContain("[UNAVAILABLE]");
    expect(prepared).not.toContain("synthetic-five");

    let reads = 0;
    const wide = {} as Record<string, unknown>;
    for (let index = 0; index < 10_000; index += 1) Object.defineProperty(wide, `field-${index}`, { enumerable: true, get: () => { reads += 1; return index; } });
    await logger.log("info", { operation: "capture.read", message: "bounded", metadata: wide });
    await logger.flush();
    expect(reads).toBeLessThan(100);
    const bounded = JSON.parse((await readFile(join(dataDirectory, "logs", "app.jsonl"), "utf8")).trim().split("\n").at(-1)!);
    expect(Buffer.byteLength(JSON.stringify(bounded.metadata))).toBeLessThanOrEqual(64);
  });

  test("enforces UTF-8 encoded record and file caps", async () => {
    const dataDirectory = await temporaryDirectory();
    const logsDirectory = join(dataDirectory, "logs");
    await mkdir(logsDirectory, { recursive: true });
    const old = JSON.stringify({ timestamp: new Date().toISOString(), message: "old".repeat(300) });
    await writeFile(join(logsDirectory, "collector.jsonl"), `${old}\n`);
    await writeFile(join(logsDirectory, "collector.prior.jsonl"), `${old}\n`);
    const logger = createLogger({ service: "collector", dataDirectory, maxRecordBytes: 128, maxFileBytes: 128, maxRetainedFiles: 20 });
    await logger.log("error", { operation: "operation".repeat(40), message: "🙂".repeat(100), sourceId: "source".repeat(40), participantId: "participant".repeat(40), jobId: "job".repeat(40) });
    await logger.log("error", { operation: "operation".repeat(40), message: "🙂".repeat(100), sourceId: "source".repeat(40), participantId: "participant".repeat(40), jobId: "job".repeat(40) });
    await logger.flush();
    const names = await logFiles(dataDirectory, "collector");
    await Promise.all(names.map(async (name) => {
      const content = await readFile(join(dataDirectory, "logs", name), "utf8");
      expect(Buffer.byteLength(content)).toBeLessThanOrEqual(128);
      for (const line of content.trim().split("\n")) {
        expect(Buffer.byteLength(line) + 1).toBeLessThanOrEqual(128);
        expect(() => JSON.parse(line)).not.toThrow();
      }
    }));
  });

  test("repairs oversized existing rows while preserving valid rows under unequal caps", async () => {
    const dataDirectory = await temporaryDirectory();
    const logsDirectory = join(dataDirectory, "logs");
    await mkdir(logsDirectory, { recursive: true });
    const timestamp = new Date().toISOString();
    const oversized = JSON.stringify({ timestamp, message: "oversized-existing-record".repeat(40) });
    const activeValid = JSON.stringify({ timestamp, message: "keep-active" });
    const archiveValid = JSON.stringify({ timestamp, message: "keep-archive" });
    await writeFile(join(logsDirectory, "app.jsonl"), `${oversized}\n${activeValid}\n`);
    await writeFile(join(logsDirectory, "app.previous.jsonl"), `${oversized}\n${archiveValid}\n`);
    const expiredArchive = join(logsDirectory, "app.expired.jsonl");
    await writeFile(expiredArchive, `${oversized}\n${JSON.stringify({ timestamp: new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString(), message: "expired-archive" })}\n`);
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000);
    await utimes(expiredArchive, eightDaysAgo, eightDaysAgo);
    const logger = createLogger({ service: "app", dataDirectory, maxRecordBytes: 128, maxFileBytes: 2_048 });
    await logger.log("info", { operation: "capture.read", message: "new-valid" });
    await logger.flush();

    const files = await logFiles(dataDirectory, "app");
    const records = (await Promise.all(files.map((name) => readFile(join(logsDirectory, name), "utf8")))).flatMap((text) => text.trim().split("\n").filter(Boolean));
    expect(records.map((line) => JSON.parse(line).message).sort()).toEqual(["keep-active", "keep-archive", "new-valid"]);
    expect(records.every((line) => Buffer.byteLength(`${line}\n`) <= 128)).toBeTrue();
    expect(files).not.toContain("app.expired.jsonl");
  });

  test("retains bounded rotated files and removes expired archives in a live sink", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "collector", dataDirectory, maxFileBytes: 260, maxRetainedFiles: 2, retentionDays: 7 });
    await Promise.all(Array.from({ length: 12 }, (_, index) => logger.log("info", { operation: "capture.read", message: `record ${index}`, metadata: { index } })));
    await logger.flush();

    const logsDirectory = join(dataDirectory, "logs");
    const old = join(logsDirectory, "collector.old.jsonl");
    await writeFile(old, "old\n");
    const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000);
    await utimes(old, eightDaysAgo, eightDaysAgo);

    const restarted = createLogger({ service: "collector", dataDirectory, maxFileBytes: 260, maxRetainedFiles: 2, retentionDays: 7 });
    await restarted.log("warn", { operation: "capture.restart", message: "restarted" });
    await restarted.flush();

    const names = await readdir(logsDirectory);
    expect(names).not.toContain("collector.old.jsonl");
    expect(names.filter((name) => name.startsWith("collector.") && name.endsWith(".jsonl")).length).toBeLessThanOrEqual(3);
    const contents = await Promise.all(names.filter((name) => name.endsWith(".jsonl")).map((name) => readFile(join(logsDirectory, name), "utf8")));
    expect(contents.join("")).toContain("restarted");
  });

  test("recovers a partial tail and expires an aged active file before appending", async () => {
    const dataDirectory = await temporaryDirectory();
    const logsDirectory = join(dataDirectory, "logs");
    await mkdir(logsDirectory, { recursive: true });
    await writeFile(join(logsDirectory, "app.jsonl"), `${JSON.stringify({ timestamp: new Date().toISOString(), message: "complete" })}\n{\"message\":\"interrupted`);
    const logger = createLogger({ service: "app", dataDirectory });
    await logger.log("info", { operation: "capture.restart", message: "new-record" });
    await logger.flush();
    const recovered = await readFile(join(logsDirectory, "app.jsonl"), "utf8");
    const recoveredRecords = recovered.trim().split("\n").map(JSON.parse);
    expect(recoveredRecords.map((record) => record.message)).toEqual(["complete", "new-record"]);

    const oldDirectory = await temporaryDirectory();
    const oldLogs = join(oldDirectory, "logs");
    await mkdir(oldLogs, { recursive: true });
    const oldTimestamp = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
    await writeFile(join(oldLogs, "collector.jsonl"), `${JSON.stringify({ timestamp: oldTimestamp, message: "expired-evidence" })}\n`);
    const restarted = createLogger({ service: "collector", dataDirectory: oldDirectory, retentionDays: 7 });
    await restarted.log("info", { operation: "capture.restart", message: "current" });
    await restarted.flush();
    const current = await readFile(join(oldLogs, "collector.jsonl"), "utf8");
    expect(current).not.toContain("expired-evidence");
    expect(current).toContain("current");
  });

  test("expires active and archived evidence while a sink remains live", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "app", dataDirectory, retentionDays: 5 / 86_400_000 });
    await logger.log("info", { operation: "capture.read", message: "expired-live-record" });
    await logger.flush();
    const archive = join(dataDirectory, "logs", "app.old.jsonl");
    await writeFile(archive, `${JSON.stringify({ message: "expired-archive-record" })}\n`);
    const old = new Date(Date.now() - 60_000);
    await utimes(archive, old, old);
    await Bun.sleep(30);
    await logger.log("info", { operation: "capture.read", message: "current-live-record" });
    await logger.flush();
    const active = await readFile(join(dataDirectory, "logs", "app.jsonl"), "utf8");
    expect(active).not.toContain("expired-live-record");
    expect(active).toContain("current-live-record");
    expect(await readdir(join(dataDirectory, "logs"))).not.toContain("app.old.jsonl");
  });

  test("resets active age after rotation while expiring older archives", async () => {
    const dataDirectory = await temporaryDirectory();
    const logger = createLogger({ service: "collector", dataDirectory, maxRecordBytes: 128, maxFileBytes: 128, retentionDays: 200 / 86_400_000 });
    await logger.log("info", { operation: "x", message: "first" });
    await logger.flush();
    await Bun.sleep(120);
    await logger.log("info", { operation: "x", message: "second" });
    await logger.flush();
    await Bun.sleep(110);
    await logger.log("info", { operation: "x", message: "third" });
    await logger.flush();

    const files = await logFiles(dataDirectory, "collector");
    const messages = (await Promise.all(files.map((name) => readFile(join(dataDirectory, "logs", name), "utf8")))).flatMap((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line).message));
    expect(messages).not.toContain("first");
    expect(messages).toContain("second");
    expect(messages).toContain("third");
  });

  test("keeps callers alive and exposes diagnostics when the destination cannot be opened", async () => {
    const dataDirectory = await temporaryDirectory();
    const blocked = join(dataDirectory, "password=synthetic-six");
    await writeFile(blocked, "file");
    const logger = createLogger({ service: "app", dataDirectory: blocked });

    await expect(logger.log("error", { operation: "media.write", message: "failed" })).resolves.toBeUndefined();
    expect(logger.getStatus()).toMatchObject({ destinationAvailable: false, pendingEntries: 0 });
    expect(logger.getStatus().lastFailure).toBeDefined();
    expect(logger.getStatus().lastFailure).not.toContain("synthetic-six");
  });

  test("survives a closed stderr pipe while reporting a destination failure", async () => {
    const child = Bun.spawn([process.execPath, "test/fixtures/closed-stderr-child.ts"], { cwd: process.cwd(), stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    await child.stderr.cancel();
    child.stdin.write("go");
    child.stdin.end();
    expect(await child.exited).toBe(0);
    expect(await new Response(child.stdout).text()).toContain("survived");
  });

  test("redacts truncated diagnostic credentials in status and stderr", async () => {
    const child = Bun.spawn([process.execPath, "test/fixtures/redaction-child.ts"], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe" });
    expect(await child.exited).toBe(0);
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    expect(stdout).not.toContain("synthetic-six");
    expect(stderr).not.toContain("synthetic-six");
  });
});
