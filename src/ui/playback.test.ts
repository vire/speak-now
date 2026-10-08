import { afterEach, expect, test } from "bun:test";
import { createScopedPlaybackOwner, type PlaybackCandidate } from "./playback";

type Deferred<T> = {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
};

const deferred = <T>(): Deferred<T> => {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => { resolve = resolvePromise; reject = rejectPromise; });
  return { promise, resolve, reject };
};

class FakeAudio {
  static instances: FakeAudio[] = [];
  onended: (() => void) | null = null;
  onerror: (() => void) | null = null;
  playResult: Promise<void> = Promise.resolve();
  pauseCalls = 0;
  volume = 1;
  playbackRate = 1;

  constructor(readonly src: string) { FakeAudio.instances.push(this); }
  play = () => this.playResult;
  pause = () => { this.pauseCalls += 1; };
  end = () => this.onended?.();
}

const scope = { sourceId: "source-a", workspaceId: "workspace-a", generation: 4 } as const;
const candidate = (overrides: Partial<PlaybackCandidate> = {}): PlaybackCandidate => ({
  id: "announcement-a",
  kind: "announcement",
  audioUrl: "/api/playback/audio/announcement-a",
  sourceId: scope.sourceId,
  workspaceId: scope.workspaceId,
  participantId: "participant-a",
  originGeneration: scope.generation,
  ...overrides,
});

afterEach(() => { FakeAudio.instances = []; });

test("only the confirmed joined scope and exact participant can auto-play", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 0.8, rate: 1.25 }, participants: { "participant-a": { muted: false, volume: 0.5 } } } });
  await owner.play(candidate({ participantId: "replacement-a" }));
  await owner.play(candidate({ workspaceId: "workspace-other" }));
  await owner.play(candidate());

  expect(FakeAudio.instances).toHaveLength(1);
  expect(FakeAudio.instances[0]?.volume).toBe(0.4);
  expect(FakeAudio.instances[0]?.playbackRate).toBe(1.25);
  expect(outcomes).toEqual(["blocked", "blocked", "started"]);
});

test("mute stops the active matching clip and suppresses replay until it is unmuted", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate());
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: true, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate(), { replay: true });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate(), { replay: true });

  expect(FakeAudio.instances[0]?.pauseCalls).toBe(1);
  expect(FakeAudio.instances).toHaveLength(2);
  expect(outcomes).toEqual(["started", "stopped", "skipped", "started"]);
});

test("explicit replay permits an older origin generation in the current joined scope", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });
  const retained = candidate({ originGeneration: 2 });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(retained, { replay: true });

  expect(FakeAudio.instances).toHaveLength(1);
  expect(outcomes).toEqual(["started"]);
});

test("a replay receipt from an older authorization generation cannot start after a same-scope rejoin", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });
  const staleReceipt = candidate({ attemptId: "replay-attempt", originGeneration: 2, authorizationGeneration: 4 } as Partial<PlaybackCandidate>);

  owner.sync({ scope: { ...scope, generation: 5 }, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(staleReceipt, { replay: true });

  expect(FakeAudio.instances).toHaveLength(0);
  expect(outcomes).toEqual(["blocked"]);
});

test("a participant mute is exact and does not mute a replacement identity", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "replacement-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: { "participant-a": { muted: false, volume: 1 } } } });
  await owner.play(candidate());
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "replacement-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: { "participant-a": { muted: true, volume: 1 } } } });
  await owner.play(candidate({ id: "replacement-item", participantId: "replacement-a" }));

  expect(FakeAudio.instances[0]?.pauseCalls).toBe(1);
  expect(FakeAudio.instances).toHaveLength(2);
  expect(outcomes).toEqual(["started", "stopped", "started"]);
});

test("automatic ready items queue behind one owner instead of interrupting each other", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(`${event.item.id}:${event.outcome}`); } });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "participant-b"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });

  await owner.enqueue(candidate({ id: "announcement-a" }));
  await owner.enqueue(candidate({ id: "announcement-b", participantId: "participant-b" }));
  expect(FakeAudio.instances).toHaveLength(1);
  FakeAudio.instances[0]?.end();
  await Bun.sleep(1);

  expect(FakeAudio.instances).toHaveLength(2);
  expect(outcomes).toEqual(["announcement-a:started", "announcement-a:heard", "announcement-b:started"]);
});

test("scope uncertainty retires queued items before a delayed stopped acknowledgement can drain them", async () => {
  const stopped = deferred<void>();
  let retryQueuedStop = true;
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => {
    outcomes.push(`${event.item.id}:${event.outcome}`);
    if (event.item.id === "announcement-b" && event.outcome === "stopped" && retryQueuedStop) return Promise.reject(new TypeError("offline"));
    return event.item.id === "announcement-a" && event.outcome === "stopped" ? stopped.promise : undefined;
  } });
  const settings = { master: { muted: false, volume: 1, rate: 1 }, participants: {} };

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "participant-b"], settings });
  await owner.enqueue(candidate({ id: "announcement-a" }));
  await owner.enqueue(candidate({ id: "announcement-b", participantId: "participant-b" }));
  expect(FakeAudio.instances).toHaveLength(1);
  owner.sync({ scope: null, scopeStatus: "pending", participants: [], settings });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "participant-b"], settings });
  stopped.resolve();
  await Bun.sleep(1);

  expect(FakeAudio.instances).toHaveLength(1);
  expect(outcomes).toEqual(["announcement-a:started", "announcement-b:stopped", "announcement-a:stopped"]);
  retryQueuedStop = false;
  await owner.retryPendingAcknowledgements();
  expect(outcomes.filter((outcome) => outcome === "announcement-b:stopped")).toHaveLength(2);
});

test("disposing the owner terminalizes queued prepared items", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(`${event.item.id}:${event.outcome}`); } });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a", "participant-b"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });

  await owner.enqueue(candidate({ id: "announcement-a" }));
  await owner.enqueue(candidate({ id: "announcement-b", participantId: "participant-b" }));
  owner.dispose();

  expect(outcomes).toEqual(["announcement-a:started", "announcement-b:stopped", "announcement-a:stopped"]);
});

test("browser autoplay denial is blocked rather than failed", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => {
    const audio = new FakeAudio(url);
    audio.playResult = Promise.reject(new DOMException("permission", "NotAllowedError"));
    return audio;
  }, report: (event) => { outcomes.push(event.outcome); } });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });

  await owner.play(candidate());

  expect(outcomes).toEqual(["blocked"]);
});

test("a leaving or generation change cancels a late play and cannot report Heard", async () => {
  const started = deferred<void>();
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => {
    const audio = new FakeAudio(url);
    audio.playResult = started.promise;
    return audio;
  }, report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  const playing = owner.play(candidate());
  owner.sync({ scope: null, scopeStatus: "pending", participants: [], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  started.resolve();
  await playing;
  FakeAudio.instances[0]?.end();

  expect(FakeAudio.instances[0]?.pauseCalls).toBe(2);
  expect(outcomes).toEqual(["stopped"]);
});

test("a new listening generation stops the old clip and fences its late completion", async () => {
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate());
  owner.sync({ scope: { ...scope, generation: 5 }, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  FakeAudio.instances[0]?.end();

  expect(FakeAudio.instances[0]?.pauseCalls).toBe(1);
  expect(outcomes).toEqual(["started", "stopped"]);
});

test("completion, not audio fetch or play acceptance, records Heard for announcements and recaps", async () => {
  const outcomes: Array<{ id: string; outcome: string }> = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => { outcomes.push({ id: event.item.id, outcome: event.outcome }); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate());
  expect(outcomes).toEqual([{ id: "announcement-a", outcome: "started" }]);
  FakeAudio.instances[0]?.end();
  await owner.play(candidate({ id: "recap-a", kind: "recap" }));
  FakeAudio.instances[1]?.end();

  expect(outcomes).toEqual([
    { id: "announcement-a", outcome: "started" },
    { id: "announcement-a", outcome: "heard" },
    { id: "recap-a", outcome: "started" },
    { id: "recap-a", outcome: "heard" },
  ]);
});

test("a short clip ending before play resolves queues Heard behind the durable started acknowledgement", async () => {
  const accepted = deferred<void>();
  const outcomes: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => {
    const audio = new FakeAudio(url);
    audio.playResult = accepted.promise;
    return audio;
  }, report: (event) => { outcomes.push(event.outcome); } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  const playing = owner.play(candidate());
  FakeAudio.instances[0]?.end();
  accepted.resolve();
  await playing;

  expect(outcomes).toEqual(["started", "heard"]);
});

test("an ended clip remains owed Heard when another clip takes the active slot before its started receipt", async () => {
  const firstStarted = deferred<void>();
  const reports: Array<{ id: string; outcome: string }> = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => {
    reports.push({ id: event.item.id, outcome: event.outcome });
    return event.item.id === "announcement-a" && event.outcome === "started" ? firstStarted.promise : undefined;
  } });
  const first = { ...candidate(), attemptId: "attempt-a" } as PlaybackCandidate;
  const second = { ...candidate({ id: "announcement-b" }), attemptId: "attempt-b" } as PlaybackCandidate;

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(first);
  FakeAudio.instances[0]?.end();
  await owner.play(second);
  firstStarted.resolve();
  await owner.retryPendingAcknowledgements();

  expect(reports.filter((report) => report.id === "announcement-a").map((report) => report.outcome)).toEqual(["started", "heard"]);
  expect(reports).toContainEqual({ id: "announcement-b", outcome: "started" });
});

test("an ended clip reports its same-attempt started then Heard after native play resolves behind another clip", async () => {
  const nativePlay = deferred<void>();
  const reports: Array<{ id: string; outcome: string }> = [];
  let audioCount = 0;
  const owner = createScopedPlaybackOwner({ createAudio: (url) => {
    const audio = new FakeAudio(url);
    if (audioCount++ === 0) audio.playResult = nativePlay.promise;
    return audio;
  }, report: (event) => { reports.push({ id: event.item.id, outcome: event.outcome }); } });
  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  const first = owner.play({ ...candidate(), attemptId: "attempt-a" });
  FakeAudio.instances[0]?.end();
  await owner.play({ ...candidate({ id: "announcement-b" }), attemptId: "attempt-b" });
  nativePlay.resolve();
  await first;
  await Bun.sleep(1);

  expect(reports.filter((report) => report.id === "announcement-a").map((report) => report.outcome)).toEqual(["started", "heard"]);
  expect(reports).toContainEqual({ id: "announcement-b", outcome: "started" });
});

test("local ended retries the same durable Heard acknowledgement without replaying audio", async () => {
  let failHeard = true;
  const reports: Array<{ attemptId?: string; outcome: string }> = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: async (event) => {
    reports.push({ attemptId: event.item.attemptId, outcome: event.outcome });
    if (event.outcome === "heard" && failHeard) throw new TypeError("offline");
  } });
  const attempt = { ...candidate(), attemptId: "attempt-a" } as PlaybackCandidate & { attemptId: string };

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(attempt);
  FakeAudio.instances[0]?.end();
  await Promise.resolve();
  failHeard = false;
  await owner.retryPendingAcknowledgements();
  await owner.play(attempt);

  expect(reports.filter((event) => event.outcome === "heard")).toEqual([{ attemptId: "attempt-a", outcome: "heard" }, { attemptId: "attempt-a", outcome: "heard" }]);
  expect(FakeAudio.instances).toHaveLength(1);
});

test("all failed reports are retained for retry instead of rejecting in the background", async () => {
  let rejectReports = true;
  const reports: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: async (event) => {
    reports.push(event.outcome);
    if (rejectReports) throw new TypeError("offline");
  } });

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(candidate());
  owner.sync({ scope: null, scopeStatus: "pending", participants: [], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  rejectReports = false;
  await owner.retryPendingAcknowledgements();

  expect(reports).toEqual(["started", "stopped", "started", "stopped"]);
});

test("ended waits for the durable started receipt and never marks Heard after a failed start", async () => {
  const startedReceipt = deferred<void>();
  let rejectStarted = false;
  const reports: string[] = [];
  const owner = createScopedPlaybackOwner({ createAudio: (url) => new FakeAudio(url), report: (event) => {
    reports.push(event.outcome);
    if (event.outcome !== "started") return;
    return rejectStarted ? Promise.reject(new TypeError("offline")) : startedReceipt.promise;
  } });
  const attempt = { ...candidate(), attemptId: "attempt-start-order" } as PlaybackCandidate & { attemptId: string };

  owner.sync({ scope, scopeStatus: "confirmed", participants: ["participant-a"], settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} } });
  await owner.play(attempt);
  FakeAudio.instances[0]?.end();
  expect(reports).toEqual(["started"]);
  startedReceipt.resolve();
  await owner.retryPendingAcknowledgements();
  expect(reports).toEqual(["started", "heard"]);

  rejectStarted = true;
  const failed = { ...candidate({ id: "failed-start" }), attemptId: "attempt-start-failed" } as PlaybackCandidate & { attemptId: string };
  await owner.play(failed);
  FakeAudio.instances[1]?.end();
  await owner.retryPendingAcknowledgements();
  expect(reports.filter((outcome) => outcome === "heard")).toEqual(["heard"]);
});
