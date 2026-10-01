import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTracer } from "../src/tracing";

const traceText = async (traces: string) => {
  const writers = (await readdir(traces, { withFileTypes: true })).filter((entry) => entry.isDirectory() && entry.name.startsWith("writer-"));
  const texts = await Promise.all(writers.map(async (entry) => Promise.all((await readdir(join(traces, entry.name))).filter((name) => name.endsWith(".jsonl")).map((name) => readFile(join(traces, entry.name, name), "utf8")))));
  return texts.flat().join("");
};
const traceFiles = async (traces: string) => {
  const writers = (await readdir(traces, { withFileTypes: true })).filter((entry) => entry.isDirectory() && entry.name.startsWith("writer-"));
  return (await Promise.all(writers.map(async (entry) => Promise.all((await readdir(join(traces, entry.name))).filter((name) => name.endsWith(".jsonl")).map(async (name) => ({ name, text: await readFile(join(traces, entry.name, name), "utf8") })))))).flat();
};

// Contract: local calls form a bounded, redactable parent-child trace that can be followed without captured evidence.
// Regression: a recorder could emit an unclosed child span or leak request secrets into calls.jsonl.
test("records closed correlated spans without evidence or credentials", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir });
    const root = tracer.start("capture.update", { sourceId: "source-a", participantId: "participant-a" });
    const child = tracer.start("summary.process", { ...root.context, jobId: "job-a" });
    await child.end("succeeded", { metadata: { requestId: "request-a", authorization: "Bearer synthetic-secret", evidence: "captured output" } });
    await root.end("succeeded");
    await tracer.flush();

    const text = await traceText(join(dataDir, "traces"));
    const records = text.trim().split("\n").map(JSON.parse);
    expect(records).toHaveLength(2);
    const parent = records.find((record) => record.operation === "capture.update");
    const childRecord = records.find((record) => record.operation === "summary.process");
    expect(childRecord).toMatchObject({ traceId: parent.traceId, parentSpanId: parent.spanId, outcome: "succeeded", sourceId: "source-a", participantId: "participant-a", jobId: "job-a" });
    expect(new Date(parent.startedAt).toISOString()).toBe(parent.startedAt);
    expect(new Date(childRecord.endedAt).toISOString()).toBe(childRecord.endedAt);
    expect(records.every((record: { durationMs: number }) => Number.isFinite(record.durationMs) && record.durationMs >= 0)).toBe(true);
    expect(text).not.toContain("synthetic-secret");
    expect(text).not.toContain("captured output");
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

// Contract: a restarted trace sink repairs its tail, retains admitted rotations, and never exposes credential-shaped values.
// Regression: same-millisecond rotations overwrote records, while restart left partial JSON and expired archives readable.
test("repairs and retains bounded redacted trace files", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-retention-"));
  try {
    const traces = join(dataDir, "traces");
    await mkdir(traces);
    await writeFile(join(traces, "calls.jsonl"), '{"partial":');
    for (let index = 0; index < 4; index += 1) {
      const archive = join(traces, `calls.old-${index}.jsonl`);
      await writeFile(archive, "{}\n");
      const old = new Date(Date.now() - 8 * 86_400_000);
      await utimes(archive, old, old);
    }
    const tracer = createTracer({ dataDirectory: dataDir, maxFileBytes: 384, maxRetainedFiles: 6 });
    for (let index = 0; index < 6; index += 1) await tracer.start(`trace.${index}`, { metadata: { requestId: `Basic synthetic-${index}`, detail: `password=synthetic-${index}` } }).end("succeeded");
    await tracer.flush();
    const lines = (await traceText(traces)).trim().split("\n").filter(Boolean);
    expect(lines).toHaveLength(6);
    expect(lines.every((line) => { try { JSON.parse(line); return true; } catch { return false; } })).toBe(true);
    expect(lines.join("\n")).not.toContain("synthetic-");
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

// Contract: continuous writes do not retain trace rows older than the configured retention window.
// Regression: file mtime renewal kept expired rows indefinitely while new calls arrived.
test("expires aged active rows while later rows continue", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-age-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir, retentionDays: 5 / 86_400_000 });
    await tracer.start("expired").end("succeeded");
    await Bun.sleep(30);
    await tracer.start("current").end("succeeded");
    await tracer.flush();
    const operations = (await traceText(join(dataDir, "traces")).then((text) => text.trim())).split("\n").map((line) => JSON.parse(line).operation);
    expect(operations).toEqual(["current"]);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a live sink removes expired rows even while later calls keep its file fresh.
// Regression: pruning file mtime retained old rows through continuous traffic.
test("expires old rows during continuous writes", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-continuous-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir, retentionDays: 100 / 86_400_000 });
    for (let index = 0; index < 7; index += 1) { await tracer.start(`row-${index}`).end("succeeded"); await Bun.sleep(20); }
    await tracer.flush();
    const rows = (await traceText(join(dataDir, "traces")).then((text) => text.trim())).split("\n").map((line) => JSON.parse(line));
    expect(rows.every((row) => Date.now() - Date.parse(row.endedAt) <= 120)).toBe(true);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: archive retention uses each trace row's completion time, not the archive mtime.
// Regression: a recent archive preserved an expired row beside a newer row.
test("expires old rows from rotated archives", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-archive-age-"));
  try {
    const traces = join(dataDir, "traces");
    await mkdir(traces);
    const now = Date.now();
    await writeFile(join(traces, "calls.recent.jsonl"), `${JSON.stringify({ endedAt: new Date(now - 200).toISOString(), operation: "expired" })}\n${JSON.stringify({ endedAt: new Date(now).toISOString(), operation: "current" })}\n`);
    const tracer = createTracer({ dataDirectory: dataDir, retentionDays: 100 / 86_400_000 });
    await tracer.start("append").end("succeeded");
    await tracer.flush();
    const archive = await traceText(traces);
    expect(archive).not.toContain("expired");
    expect(archive).toContain("current");
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: malformed UTF-8 complete rows are discarded without corrupting later valid Unicode rows.
// Regression: replacement decoding turned invalid bytes into a JSON-valid row during startup repair.
test("rejects malformed UTF-8 rows while retaining later Unicode rows", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-utf8-"));
  try {
    const traces = join(dataDir, "traces");
    await mkdir(traces);
    const now = new Date().toISOString();
    const malformed = Buffer.from(`{"endedAt":"${now}","operation":"bad-`);
    const valid = Buffer.from(`${JSON.stringify({ endedAt: now, operation: "café 😀 日本語" })}\n`);
    await writeFile(join(traces, "calls.utf8.jsonl"), Buffer.concat([malformed, Buffer.from([0xc3, 0x28]), Buffer.from("\"}\n"), valid]));
    const tracer = createTracer({ dataDirectory: dataDir });
    await tracer.start("append").end("succeeded");
    await tracer.flush();
    const archive = await traceText(traces);
    expect(archive).not.toContain("bad-");
    expect(archive).toContain("café 😀 日本語");
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: retained archives apply the same record-time expiry while supported writes keep rotating them.
// Regression: one-time startup repair left an archive's expired rows alive whenever later rows refreshed its mtime.
test("expires aged rows from archives during continued rotation", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-rotated-age-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir, maxFileBytes: 512, maxRetainedFiles: 30, retentionDays: 350 / 86_400_000 });
    for (let index = 0; index < 5; index += 1) { await tracer.start(`rotated-${index}`).end("succeeded"); if (index < 4) await Bun.sleep(100); }
    await tracer.flush();
    const traces = join(dataDir, "traces");
    const files = await traceFiles(traces);
    const rows = files.flatMap((file) => file.text.trim().split("\n").filter(Boolean).map(JSON.parse));
    expect(files.some((file) => file.name !== "calls.jsonl")).toBe(true);
    expect(rows.some((row) => row.operation === "rotated-4")).toBe(true);
    expect(rows.every((row: { endedAt: string }) => Date.now() - Date.parse(row.endedAt) < 350)).toBe(true);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a successful rotation enforces the final per-writer file budget, including the newly created active file.
// Regression: pruning only before rotation retained one archive plus the replacement active file at a one-file budget.
test("keeps the final per-writer budget after rotation", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-final-budget-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir, maxFileBytes: 256, maxRetainedFiles: 1 });
    for (let index = 0; index < 4; index += 1) await tracer.start(`budget-${index}`).end("succeeded");
    await tracer.flush();
    const files = await traceFiles(join(dataDir, "traces"));
    expect(files).toHaveLength(1);
    expect(files[0].text.length).toBeLessThanOrEqual(256);
    expect(tracer.getStatus().droppedEntries).toBe(0);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: independent live recorders own separate generation directories and never contend for one mutable file.
// Regression: the shared canonical destination required a lock and could drop a live writer's span.
test("isolates independent live writers in never-reused generations", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-writers-"));
  const fixture = `import { createTracer } from ${JSON.stringify(join(process.cwd(), "src/tracing.ts"))}; const t=createTracer({dataDirectory:process.argv[1]}); await t.start("child-writer").end("succeeded"); await t.flush();`;
  try {
    const child = Bun.spawn([process.execPath, "-e", fixture, dataDir], { stdout: "ignore", stderr: "pipe" });
    const parent = createTracer({ dataDirectory: dataDir }); await parent.start("parent-writer").end("succeeded"); await parent.flush();
    expect(await child.exited).toBe(0);
    const writers = (await readdir(join(dataDir, "traces"), { withFileTypes: true })).filter((entry) => entry.isDirectory() && entry.name.startsWith("writer-"));
    expect(writers).toHaveLength(2);
    const text = await traceText(join(dataDir, "traces"));
    expect(text).toContain("parent-writer"); expect(text).toContain("child-writer");
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a later append claims a generation from an actually exited owned process by move before bounded repair, preserving complete rows once.
// Regression: one-time startup recovery left a generation that died after startup outside retention forever.
test("recovers a dead generation during later maintenance", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-reclaim-"));
  try {
    const tracer = createTracer({ dataDirectory: dataDir }); await tracer.start("before-death").end("succeeded"); await tracer.flush();
    const traces = join(dataDir, "traces"); const control = join(dataDir, "dead-control"); await mkdir(control);
    const child = Bun.spawn([process.execPath, join(process.cwd(), "test/fixtures/tracing-generation.ts"), dataDir, control, "claimed-once", "exit-with-partial"], { stdout: "ignore", stderr: "pipe" });
    expect(await child.exited).toBe(0);
    await tracer.start("after-death").end("succeeded"); await tracer.flush();
    const text = await traceText(traces);
    expect(text.match(/claimed-once/g)?.length).toBe(1); expect(text).toContain("after-death");
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: competing reclaimers may each observe death, but atomic source moves retain an eligible row at most once.
// Regression: stale shared-owner cleanup could delete a successor or duplicate reclaimed trace data.
test("lets competing reclaimers claim a dead generation once", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-competing-"));
  const fixture = join(process.cwd(), "test/fixtures/tracing-generation.ts");
  try {
    const traces = join(dataDir, "traces"); const exited = Bun.spawn([process.execPath, "-e", ""], { stdout: "ignore", stderr: "ignore" }); await exited.exited;
    const dead = join(traces, `writer-${exited.pid}-seed-dead`); await mkdir(dead, { recursive: true });
    await writeFile(join(dead, "calls.jsonl"), `${JSON.stringify({ endedAt: new Date().toISOString(), operation: "claimed-once" })}\n`);
    const controlA = join(dataDir, "control-a"); const controlB = join(dataDir, "control-b"); await mkdir(controlA); await mkdir(controlB);
    const first = Bun.spawn([process.execPath, fixture, dataDir, controlA, "reclaimer-a", "root-gate"], { stdout: "ignore", stderr: "pipe" });
    while (!(await Bun.file(join(controlA, "ready")).exists())) await Bun.sleep(2);
    const second = Bun.spawn([process.execPath, fixture, dataDir, controlB, "reclaimer-b"], { stdout: "ignore", stderr: "pipe" });
    expect(await second.exited).toBe(0);
    await writeFile(join(controlA, "go"), "go");
    expect(await first.exited).toBe(0);
    const statuses = await Promise.all([controlA, controlB].map(async (control) => JSON.parse(await readFile(join(control, "result.json"), "utf8"))));
    const text = await traceText(traces);
    expect(text.match(/claimed-once/g)?.length).toBe(1);
    expect(text).toContain("reclaimer-a"); expect(text).toContain("reclaimer-b");
    expect(statuses.every((status) => status.droppedEntries === 0)).toBe(true);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a stopped legacy root file is moved once into the first private generation and bounded repair makes later starts idempotent.
// Regression: legacy calls could remain outside retention or be copied repeatedly on restart.
test("migrates a quiescent legacy trace once", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-migration-"));
  const fixture = `import { createTracer } from ${JSON.stringify(join(process.cwd(), "src/tracing.ts"))}; const t = createTracer({ dataDirectory: process.argv[1] }); await t.start("restarted").end("succeeded"); await t.flush();`;
  try {
    const traces = join(dataDir, "traces"); await mkdir(traces, { recursive: true });
    await writeFile(join(traces, "calls.jsonl"), `${JSON.stringify({ endedAt: new Date().toISOString(), operation: "legacy-row" })}\n{partial`);
    const first = createTracer({ dataDirectory: dataDir }); await first.start("first").end("succeeded"); await first.flush();
    const second = Bun.spawn([process.execPath, "-e", fixture, dataDir], { stdout: "ignore", stderr: "pipe" }); expect(await second.exited).toBe(0);
    const text = await traceText(traces);
    expect(text.match(/legacy-row/g)?.length).toBe(1); expect(await Bun.file(join(traces, "calls.jsonl")).exists()).toBe(false);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});
