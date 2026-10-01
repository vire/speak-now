import { afterEach, describe, expect, test } from "bun:test";
import { createPrototypePlaybackOwner } from "./prototype-playback";

type Deferred<T> = {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  return { promise: new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  }), resolve, reject };
}

class FakeAudio {
  static clips: FakeAudio[] = [];
  onended: ((this: GlobalEventHandlers, event: Event) => unknown) | null = null;
  onerror: OnErrorEventHandler | null = null;
  playCalls = 0;
  pauseCalls = 0;

  constructor(readonly source: string) {
    FakeAudio.clips.push(this);
  }

  play = () => {
    this.playCalls += 1;
    return Promise.resolve();
  };
  pause = () => {
    this.pauseCalls += 1;
  };
  end = () => this.onended?.call(this as unknown as GlobalEventHandlers, new Event("ended"));
  fail = () => this.onerror?.call(this as unknown as GlobalEventHandlers, new Event("error"));
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

const originalFetch = globalThis.fetch;
const originalAudio = globalThis.Audio;

type FetchStub = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

function usePlatform(fetchImplementation: FetchStub) {
  FakeAudio.clips = [];
  globalThis.fetch = fetchImplementation as typeof fetch;
  globalThis.Audio = FakeAudio as unknown as typeof Audio;
}

afterEach(() => {
  globalThis.fetch = originalFetch;
  globalThis.Audio = originalAudio;
});

describe("prototype playback owner", () => {
  test("claims one owner before a slow fetch settles", async () => {
    const request = deferred<Response>();
    let requests = 0;
    usePlatform(() => {
      requests += 1;
      return request.promise;
    });
    const owner = createPrototypePlaybackOwner({ onError: () => {} });

    owner.check();
    owner.check();
    expect(requests).toBe(1);
    request.resolve(Response.json({ key: "first" }));
    await settle();
    owner.check();

    expect(FakeAudio.clips).toHaveLength(1);
    expect(FakeAudio.clips[0].playCalls).toBe(1);
    expect(requests).toBe(1);
  });

  test("does not create a clip after teardown of a pending request", async () => {
    const request = deferred<Response>();
    let aborted = false;
    usePlatform((_, init) => {
        const signal = init?.signal as AbortSignal;
        signal.addEventListener("abort", () => { aborted = true; });
        return request.promise;
    });
    const owner = createPrototypePlaybackOwner({ onError: () => {} });

    owner.check();
    owner.dispose();
    request.resolve(Response.json({ key: "late" }));
    await settle();

    expect(aborted).toBe(true);
    expect(FakeAudio.clips).toHaveLength(0);
  });

  test("stops active playback during teardown", async () => {
    usePlatform(async () => Response.json({ key: "active" }));
    const owner = createPrototypePlaybackOwner({ onError: () => {} });

    owner.check();
    await settle();
    owner.dispose();
    owner.dispose();

    expect(FakeAudio.clips[0].playCalls).toBe(1);
    expect(FakeAudio.clips[0].pauseCalls).toBe(1);
  });

  test("allows an explicit retry after playback failure", async () => {
    let requests = 0;
    const errorPauseCounts: number[] = [];
    usePlatform(async () => {
      requests += 1;
      return Response.json({ key: `retry-${requests}` });
    });
    const owner = createPrototypePlaybackOwner({
      onError: (message) => {
        if (message) errorPauseCounts.push(FakeAudio.clips[0].pauseCalls);
      },
    });

    owner.check();
    await settle();
    const failedClip = FakeAudio.clips[0];
    failedClip.fail();
    owner.check();
    await settle();
    const replacement = FakeAudio.clips[1];
    failedClip.end();
    failedClip.fail();
    owner.check();
    await settle();
    owner.dispose();

    expect(errorPauseCounts).toEqual([1]);
    expect(requests).toBe(2);
    expect(replacement.playCalls).toBe(1);
    expect(failedClip.pauseCalls).toBe(1);
    expect(replacement.pauseCalls).toBe(1);
  });

  test("does not replay a completed clip key, but plays a later key", async () => {
    const keys = ["completed", "completed", "new"];
    let requests = 0;
    usePlatform(async () => {
      const key = keys[requests];
      requests += 1;
      return Response.json({ key });
    });
    const owner = createPrototypePlaybackOwner({ onError: () => {} });

    owner.check();
    await settle();
    owner.check();
    expect(requests).toBe(1);
    expect(FakeAudio.clips).toHaveLength(1);
    expect(FakeAudio.clips[0].playCalls).toBe(1);
    FakeAudio.clips[0].end();
    owner.check();
    await settle();

    expect(requests).toBe(2);
    expect(FakeAudio.clips).toHaveLength(1);
    expect(FakeAudio.clips[0].playCalls).toBe(1);
    owner.check();
    await settle();

    expect(requests).toBe(3);
    expect(FakeAudio.clips).toHaveLength(2);
    expect(FakeAudio.clips[1].playCalls).toBe(1);
  });

  test("retries a failed clip key", async () => {
    let requests = 0;
    usePlatform(async () => {
      requests += 1;
      return Response.json({ key: "retryable" });
    });
    const owner = createPrototypePlaybackOwner({ onError: () => {} });

    owner.check();
    await settle();
    FakeAudio.clips[0].fail();
    owner.check();
    await settle();

    expect(requests).toBe(2);
    expect(FakeAudio.clips).toHaveLength(2);
    expect(FakeAudio.clips[1].playCalls).toBe(1);
  });
});
