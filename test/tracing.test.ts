import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readdir, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createTracer } from "../src/tracing";

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

    const text = await readFile(join(dataDir, "traces", "calls.jsonl"), "utf8");
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
    const files = await readdir(traces);
    const lines = (await Promise.all(files.filter((name) => name.endsWith(".jsonl")).map((name) => readFile(join(traces, name), "utf8")))).flatMap((text) => text.trim().split("\n").filter(Boolean));
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
    const operations = (await readFile(join(dataDir, "traces", "calls.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line).operation);
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
    const rows = (await readFile(join(dataDir, "traces", "calls.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line));
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
    const archive = await readFile(join(traces, "calls.recent.jsonl"), "utf8");
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
    const archive = await readFile(join(traces, "calls.utf8.jsonl"), "utf8");
    expect(archive).not.toContain("bad-");
    expect(archive).toContain("café 😀 日本語");
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a live writer's permanent kernel guard excludes another recorder before it can repair or append.
// Regression: an age-only or pathname-only lock allowed a contender to mutate calls.jsonl while an owner was paused.
test("does not mutate traces while a live fixture holds the permanent guard", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-live-guard-"));
  const traces = join(dataDir, "traces");
  const guard = join(traces, ".calls.guard");
  const addon = join(process.cwd(), "native", "flock_guard.node");
  const fixture = `import { open } from "node:fs/promises"; import { createRequire } from "node:module"; const handle = await open(process.argv[1], "a+"); const flock = createRequire(import.meta.url)(process.argv[2]); if (flock.lock(handle.fd) !== 0) throw new Error("fixture guard unavailable"); console.log("ready"); await new Response(Bun.stdin.stream()).text(); await handle.close();`;
  try {
    await mkdir(traces, { recursive: true });
    const holder = Bun.spawn([process.execPath, "-e", fixture, guard, addon], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    const reader = holder.stdout.getReader();
    const ready = await reader.read();
    reader.releaseLock();
    expect(new TextDecoder().decode(ready.value).includes("ready")).toBe(true);
    const tracer = createTracer({ dataDirectory: dataDir });
    const pending = tracer.start("excluded").end("succeeded");
    await Bun.sleep(30);
    expect(await Bun.file(join(traces, "calls.jsonl")).exists()).toBe(false);
    holder.stdin.end();
    await holder.exited;
    await pending;
    await tracer.flush();
    expect(await Bun.file(join(traces, "calls.jsonl")).exists()).toBe(true);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a complete atomically published owner from a dead fixture is recoverable, while canonical publication never overwrites it.
// Regression: stale recovery either stranded recording forever or could delete a successor after observing an earlier owner.
test("recovers a complete owner published by a dead fixture without replacing artifacts", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-dead-owner-"));
  const traces = join(dataDir, "traces");
  const canonical = join(traces, ".calls.lock");
  const fixture = `import { link, mkdir, writeFile } from "node:fs/promises"; import { join } from "node:path"; const directory = process.argv[1]; await mkdir(directory, { recursive: true }); const token = "fixture-token-0123456789"; const candidate = join(directory, ".calls.owner.fixture"); await writeFile(candidate, JSON.stringify({ version: 1, pid: process.pid, token })); await link(candidate, join(directory, ".calls.lock")); console.log("published");`;
  try {
    const owner = Bun.spawn([process.execPath, "-e", fixture, traces], { stdout: "pipe", stderr: "pipe" });
    expect((await new Response(owner.stdout).text()).includes("published")).toBe(true);
    expect(await owner.exited).toBe(0);
    expect((await stat(canonical)).isFile()).toBe(true);
    const tracer = createTracer({ dataDirectory: dataDir });
    await tracer.start("after-dead-owner").end("succeeded");
    await tracer.flush();
    expect((await readFile(join(traces, "calls.jsonl"), "utf8"))).toContain("after-dead-owner");
    expect(await Bun.file(canonical).exists()).toBe(false);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a canonical artifact without complete ownership metadata is unavailable and remains untouched.
// Regression: an age-based cleanup could delete an external or interrupted owner and silently steal its trace destination.
test("keeps an ambiguous canonical owner artifact conservative", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-ambiguous-owner-"));
  const traces = join(dataDir, "traces");
  const canonical = join(traces, ".calls.lock");
  try {
    await mkdir(traces, { recursive: true });
    await writeFile(canonical, "");
    const tracer = createTracer({ dataDirectory: dataDir });
    await tracer.start("must-not-steal").end("succeeded");
    await tracer.flush();
    expect(await readFile(canonical, "utf8")).toBe("");
    expect(await Bun.file(join(traces, "calls.jsonl")).exists()).toBe(false);
    expect(tracer.getStatus().droppedEntries).toBe(1);
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
    const rows = (await Promise.all((await readdir(traces)).filter((name) => name.endsWith(".jsonl")).map(async (name) => (await readFile(join(traces, name), "utf8")).trim().split("\n").filter(Boolean).map(JSON.parse)))).flat();
    expect(rows.every((row: { endedAt: string }) => Date.now() - Date.parse(row.endedAt) < 350)).toBe(true);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

// Contract: a real busy guard uses monotonic elapsed time, so an adjusted wall clock cannot postpone the bounded diagnostic.
// Regression: Date.now-based retry deadlines waited until a live holder released after a simulated backward clock step.
test("keeps the busy guard deadline monotonic across a wall-clock rollback", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-trace-clock-"));
  const guard = join(dataDir, "traces", ".calls.guard");
  const addon = join(process.cwd(), "native", "flock_guard.node");
  const fixture = `import { mkdir, open } from "node:fs/promises"; import { createRequire } from "node:module"; await mkdir(process.argv[1], { recursive: true }); const handle = await open(process.argv[2], "a+"); const flock = createRequire(import.meta.url)(process.argv[3]); if (flock.lock(handle.fd) !== 0) throw new Error("fixture guard unavailable"); console.log("ready"); await new Response(Bun.stdin.stream()).text(); await handle.close();`;
  const originalNow = Date.now;
  let holder: ReturnType<typeof Bun.spawn> | undefined;
  try {
    holder = Bun.spawn([process.execPath, "-e", fixture, join(dataDir, "traces"), guard, addon], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
    const reader = holder.stdout.getReader(); const ready = await reader.read(); reader.releaseLock();
    expect(new TextDecoder().decode(ready.value).includes("ready")).toBe(true);
    let reads = 0;
    Date.now = () => ++reads === 1 ? originalNow() : originalNow() - 60_000;
    const tracer = createTracer({ dataDirectory: dataDir });
    const started = performance.now();
    let settled = false;
    const pending = tracer.start("clock-rollback").end("succeeded").finally(() => { settled = true; });
    await Bun.sleep(2_150);
    expect(settled).toBe(true);
    expect(performance.now() - started).toBeLessThan(2_250);
    expect(tracer.getStatus().droppedEntries).toBe(1);
    await pending;
  } finally {
    Date.now = originalNow;
    holder?.stdin.end();
    if (holder) await holder.exited;
    await rm(dataDir, { recursive: true, force: true });
  }
});
