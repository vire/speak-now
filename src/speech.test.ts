import { expect, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { synthesize } from "./speech";
import { createTracer } from "./tracing";
import { createErrorReporter } from "./errors";

test("writes and reuses the deterministic ElevenLabs provider-contract fixture", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-test-"));
  let calls = 0;
  const fixtureFetch = (async () => {
    calls += 1;
    return new Response(new Uint8Array([73, 68, 51]), { status: 200, headers: { "Content-Type": "audio/mpeg" } });
  }) as unknown as typeof fetch;
  try {
    const first = await synthesize("A concise summary.", "fixture", { apiKey: "test-key", voiceId: "test-voice", dataDir }, fixtureFetch);
    const second = await synthesize("A concise summary.", "fixture", { apiKey: "test-key", voiceId: "test-voice", dataDir }, fixtureFetch);
    expect(await Bun.file(first.path).bytes()).toEqual(new Uint8Array([73, 68, 51]));
    expect(second.cached).toBe(true);
    expect(calls).toBe(1);
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});

test("aborts a provider body and removes its temporary clip", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-abort-"));
  const controller = new AbortController();
  let cancelled = false;
  const fixtureFetch = (async () => new Response(new ReadableStream<Uint8Array>({
    start(stream) {
      stream.enqueue(new Uint8Array([73, 68, 51]));
    },
    cancel() {
      cancelled = true;
    },
  }), { status: 200 })) as unknown as typeof fetch;
  try {
    const work = synthesize("A concise summary.", "abort", { apiKey: "test-key", voiceId: "test-voice", dataDir }, fixtureFetch, controller.signal);
    await Bun.sleep(10);
    controller.abort();
    await expect(work).rejects.toThrow("cancelled");
    expect(cancelled).toBe(true);
    expect(await readdir(join(dataDir, "audio"))).toEqual([]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

// Contract: a cancelled provider call closes its own child span and the media write is absent.
// Regression: cancellation previously left a provider operation without an observable terminal outcome.
test("traces a cancelled provider request", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-trace-"));
  const controller = new AbortController();
  const tracer = createTracer({ dataDirectory: dataDir });
  const parent = tracer.start("collector.job", { traceId: "trace-a", jobId: "job-a", participantId: "participant-a" });
  const fixtureFetch = (async () => new Response(new ReadableStream<Uint8Array>({ start(stream) { stream.enqueue(new Uint8Array([73, 68, 51])); } }), { status: 200 })) as unknown as typeof fetch;
  try {
    const work = synthesize("A concise summary.", "abort-trace", { apiKey: "test-key", voiceId: "test-voice", dataDir, tracer, trace: parent.context, jobId: "job-a" }, fixtureFetch, controller.signal);
    await Bun.sleep(10);
    controller.abort();
    await expect(work).rejects.toThrow("cancelled");
    await parent.end("cancelled");
    await tracer.flush();
    const traces = join(dataDir, "traces");
    const records = (await Promise.all((await readdir(traces, { withFileTypes: true })).filter((entry) => entry.isDirectory() && entry.name.startsWith("writer-")).map(async (entry) => Promise.all((await readdir(join(traces, entry.name))).filter((name) => name.endsWith(".jsonl")).map(async (name) => (await Bun.file(join(traces, entry.name, name)).text()).trim().split("\n").filter(Boolean).map((line) => JSON.parse(line))))))).flat(2);
    expect(records.find((record) => record.operation === "speech.provider")).toMatchObject({ traceId: "trace-a", parentSpanId: parent.context.spanId, jobId: "job-a", outcome: "cancelled" });
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});

test("reports a synthetic provider failure without a network call", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-error-"));
  const reporter = createErrorReporter({ dataDirectory: dataDir });
  const fixtureFetch = (async () => new Response("unavailable", { status: 503 })) as unknown as typeof fetch;
  try {
    await expect(synthesize("A concise summary.", "provider-error", { apiKey: "test-key", voiceId: "test-voice", dataDir, reporter, trace: { traceId: "a".repeat(32), spanId: "b".repeat(16), jobId: "job-a" } }, fixtureFetch)).rejects.toThrow("503");
    await reporter.flush();
    const [report] = (await readFile(join(dataDir, "errors", "reports.jsonl"), "utf8")).trim().split("\n").map((line) => JSON.parse(line));
    expect(report).toMatchObject({ service: "collector", operation: "speech.synthesize", category: "provider", traceId: "a".repeat(32), jobId: "job-a" });
  } finally { await rm(dataDir, { recursive: true, force: true }); }
});
