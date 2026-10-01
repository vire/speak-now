import { expect, test } from "bun:test";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { synthesize } from "./speech";

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
