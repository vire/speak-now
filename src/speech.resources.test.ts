import { expect, test } from "bun:test";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { synthesize } from "./speech";

// Contract: cancelling a live response aborts its body and leaves no publishable temporary clip.
// Regression: an indefinitely open provider stream could retain its reader and temporary output.
test("aborts a provider body and removes its temporary clip", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-resource-"));
  const controller = new AbortController();
  let cancelled = false;
  const fixtureFetch = (async () => new Response(new ReadableStream<Uint8Array>({
    start(stream) { stream.enqueue(new Uint8Array([73, 68, 51])); },
    cancel() { cancelled = true; },
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
