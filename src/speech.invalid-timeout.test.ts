import { expect, test } from "bun:test";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { synthesize } from "./speech";

// Contract: an invalid local deadline has no provider, listener, or output side effect.
test("rejects an invalid speech timeout before acquiring request resources", async () => {
  const dataDir = await mkdtemp(join(tmpdir(), "speak-now-speech-invalid-timeout-"));
  const controller = new AbortController();
  let fetchCalls = 0;
  let listenerAdds = 0;
  const addEventListener = controller.signal.addEventListener.bind(controller.signal);
  controller.signal.addEventListener = ((...args: Parameters<AbortSignal["addEventListener"]>) => {
    listenerAdds += 1;
    return addEventListener(...args);
  }) as AbortSignal["addEventListener"];
  const fixtureFetch = (async () => {
    fetchCalls += 1;
    return new Response(new Uint8Array([73, 68, 51]));
  }) as unknown as typeof fetch;
  try {
    await expect(synthesize("A concise summary.", "invalid", {
      apiKey: "test-key",
      voiceId: "test-voice",
      dataDir,
      timeoutMs: 0,
    }, fixtureFetch, controller.signal)).rejects.toThrow("speech timeout must be a finite positive number");
    expect(fetchCalls).toBe(0);
    expect(listenerAdds).toBe(0);
    expect(await readdir(dataDir)).toEqual([]);
  } finally {
    await rm(dataDir, { recursive: true, force: true });
  }
});
