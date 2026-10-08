import { afterEach, expect, test } from "bun:test";
import { BrowserApiError, type BrowserApi as BrowserApiContract, type EventNotification, type HistoryPage, type StateResponse } from "./api";
import { createBrowserSession } from "./session";

const sourceA = "source-a";
const sourceB = "source-b";
const workspaceB = "workspace-b";
const tabB = "tab-b";
type BrowserApi = Pick<BrowserApiContract, "readState" | "readHistory" | "setListening" | "openEvents"> & Partial<Omit<BrowserApiContract, "readState" | "readHistory" | "setListening" | "openEvents">>;

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => { resolve = resolvePromise; reject = rejectPromise; });
  return { promise, resolve, reject };
};

const eventually = async (assertion: () => void, attempts = 30) => {
  let failure: unknown;
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try { assertion(); return; } catch (error) { failure = error; }
    await Bun.sleep(1);
  }
  throw failure;
};

const state = (sourceId: string, eventSequence: number, scope: StateResponse["scope"] = null): StateResponse => ({
  sources: [sourceA, sourceB].map((id) => ({ sourceId: id, stale: false, observedAt: "2026-10-07T12:00:00.000Z", topologySequence: eventSequence, listeningGeneration: 0 })),
  topology: { source: { id: sourceId, namespace: sourceId, stale: false, observedAt: "2026-10-07T12:00:00.000Z" }, workspaces: [], tabs: [], panes: [], participants: [] },
  scope,
  captureByParticipant: [],
  jobs: { pending: 0, leased: 0, completed: 0, expired: 0 },
  announcements: { count: 0 },
  eventSequence,
} as unknown as StateResponse);

const history = (): HistoryPage => ({ announcements: [], results: [] });

const idleEvents = async function*(): AsyncIterable<EventNotification> {
  await new Promise<void>(() => undefined);
};

const sessions = new Set<ReturnType<typeof createBrowserSession>>();
const sessionFor = (api: BrowserApi) => {
  const session = createBrowserSession(api as BrowserApiContract);
  sessions.add(session);
  return session;
};
afterEach(() => {
  for (const session of sessions) session.stop();
  sessions.clear();
});

test("playback state follows the confirmed joined scope rather than the browsed source", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined),
    readHistory: async () => history(),
    setListening: async () => ({ scope: joined, generation: 7 }),
    openEvents: async () => idleEvents(),
    readPlaybackSettings: async (sourceId) => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [{ sourceId, participantId: "participant-a", muted: false, volume: 1 }] }),
    readPlaybackCandidates: async () => ({ scope: joined, candidates: [{ itemId: "announcement:job-a", kind: "announcement", sourceId: sourceA, workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 7, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "pending" } }] }),
  };
  const session = sessionFor(api);

  await session.start(sourceB);
  await session.refreshPlayback();

  expect(session.snapshot().selectedSourceId).toBe(sourceB);
  expect(session.snapshot().playback).toMatchObject({ scope: joined, candidates: [{ sourceId: sourceA, media: { state: "pending" } }] });
});

test("a same-generation completion refreshes an empty joined candidate feed after SSE", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const completion = deferred<EventNotification>();
  let completed = false;
  let streams = 0;
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, completed ? 2 : 1, joined),
    readHistory: async () => history(),
    setListening: async () => ({ scope: joined, generation: 7 }),
    openEvents: async () => {
      streams += 1;
      if (streams === 1) return (async function*() { yield await completion.promise; })();
      return idleEvents();
    },
    readPlaybackSettings: async () => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [] }),
    readPlaybackCandidates: async () => ({ scope: joined, candidates: completed ? [{ itemId: "recap:job-a", kind: "recap", sourceId: sourceA, workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 7, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "pending" } }] : [] }),
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  await session.refreshPlayback();
  expect(session.snapshot().playback?.candidates).toEqual([]);
  completed = true;
  completion.resolve({ id: 2, event: "completed" });

  await eventually(() => expect(session.snapshot().playback?.candidates).toMatchObject([{ itemId: "recap:job-a", media: { state: "pending" } }]));
});

test("playback refresh traverses scoped candidate pages beyond the initial fifty", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const candidate = (index: number) => ({ itemId: `announcement:job-${index}`, kind: "announcement" as const, sourceId: sourceA, workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 7, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "ready" as const, id: "a".repeat(64) }, playback: { status: "unattempted" as const } });
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined), readHistory: async () => history(), setListening: async () => ({ scope: joined, generation: 7 }), openEvents: async () => idleEvents(),
    readPlaybackSettings: async () => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [] }),
    readPlaybackCandidates: async (query) => query?.cursor ? { scope: joined, candidates: [candidate(51)] } : { scope: joined, candidates: Array.from({ length: 50 }, (_, index) => candidate(index)), nextCursor: "after-50" },
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  await session.refreshPlayback();

  expect(session.snapshot().playback?.candidates).toHaveLength(51);
  expect(session.snapshot().playback?.candidates.at(-1)?.itemId).toBe("announcement:job-51");
});

test("an SSE refreshes joined-source participants while browsing another source", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const replacement = deferred<EventNotification>();
  let currentParticipant = "participant-old";
  let streams = 0;
  const sourceState = (sourceId: string) => {
    const base = state(sourceId, streams + 1, joined);
    if (sourceId !== sourceA) return base;
    return {
      ...base,
      topology: {
        ...base.topology!,
        workspaces: [{ id: "workspace-a", sourceId: sourceA, label: "Workspace", order: 0, live: true }],
        tabs: [{ id: "tab-a", workspaceId: "workspace-a", label: "Tab", order: 0 }],
        panes: [{ id: "pane-a", tabId: "tab-a", terminalId: "terminal-a" }],
        participants: [{ id: currentParticipant, sourceId: sourceA, paneId: "pane-a", rawPaneId: "raw-a", terminalId: "terminal-a", kind: "codex", generation: 1, active: true }],
      },
    } as StateResponse;
  };
  const api: BrowserApi = {
    readState: async (sourceId) => sourceState(sourceId ?? sourceA),
    readHistory: async () => history(),
    setListening: async () => ({ scope: joined, generation: 7 }),
    openEvents: async () => {
      streams += 1;
      if (streams === 2) return (async function*() { yield await replacement.promise; })();
      return idleEvents();
    },
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  await session.selectSource(sourceB);
  expect(session.snapshot().playbackAuthorization?.participantIds).toEqual(["participant-old"]);
  currentParticipant = "participant-new";
  replacement.resolve({ id: 3, event: "topology" });

  await eventually(() => expect(session.snapshot().playbackAuthorization?.participantIds).toEqual(["participant-new"]));
});

test("playback session prepares pending media before it authorizes a client-owned attempt", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  let prepared = false;
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined),
    readHistory: async () => history(),
    setListening: async () => ({ scope: joined, generation: 7 }),
    openEvents: async () => idleEvents(),
    readPlaybackSettings: async () => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [] }),
    readPlaybackCandidates: async () => ({ scope: joined, candidates: [{ itemId: "announcement:job-a", kind: "announcement", sourceId: sourceA, workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 7, createdAt: "2026-10-08T12:00:00.000Z", media: prepared ? { state: "ready", id: "a".repeat(64) } : { state: "pending" } }] }),
    preparePlaybackItem: async () => { prepared = true; return { media: { state: "ready", id: "a".repeat(64) } }; },
    createPlaybackAttempt: async (attemptId) => ({ attemptId, state: "prepared", originGeneration: 7, authorizationGeneration: 7, mediaUrl: "/api/playback/attempts/attempt-a/media" }),
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  await session.refreshPlayback();
  await session.preparePlaybackItem("announcement:job-a");
  const attempt = await session.authorizePlayback("announcement:job-a", "automatic", "attempt-a");

  expect(session.snapshot().playback?.candidates[0]?.media.state).toBe("ready");
  expect(attempt).toMatchObject({ attemptId: "attempt-a", mediaUrl: "/api/playback/attempts/attempt-a/media" });
});

test("a delayed playback settings read cannot undo an optimistic accepted mute", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const delayedRead = deferred<{ master: { muted: boolean; volume: number; speed: number }; participants: [] }>();
  const delayedWrite = deferred<{ master: { muted: boolean; volume: number; speed: number }; participants: [] }>();
  let delayReads = false;
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined),
    readHistory: async () => history(),
    setListening: async () => ({ scope: joined, generation: 7 }),
    openEvents: async () => idleEvents(),
    readPlaybackSettings: async () => delayReads ? delayedRead.promise : { master: { muted: false, volume: 1, speed: 1 }, participants: [] },
    readPlaybackCandidates: async () => ({ scope: joined, candidates: [] }),
    setPlaybackSettings: async () => delayedWrite.promise,
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  delayReads = true;
  const staleRefresh = session.refreshPlayback();
  const mute = session.setPlaybackSettings({ master: { muted: true } });
  expect(session.snapshot().playback?.settings.master.muted).toBe(true);
  delayedWrite.resolve({ master: { muted: true, volume: 1, speed: 1 }, participants: [] });
  delayedRead.resolve({ master: { muted: false, volume: 1, speed: 1 }, participants: [] });
  await Promise.all([mute, staleRefresh]);

  expect(session.snapshot().playback?.settings.master.muted).toBe(true);
});

test("a failed Leave reconciliation fences a late prepared attempt even when the generation is unchanged", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const receipt = deferred<{ attemptId: string; state: "prepared"; originGeneration: number; authorizationGeneration: number; mediaUrl: string }>();
  const stopped: string[] = [];
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined), readHistory: async () => history(),
    setListening: async () => { throw new TypeError("uncertain leave"); }, openEvents: async () => idleEvents(),
    readPlaybackSettings: async () => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [] }),
    readPlaybackCandidates: async () => ({ scope: joined, candidates: [{ itemId: "announcement:job-a", kind: "announcement", sourceId: sourceA, workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 7, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "ready", id: "a".repeat(64) }, playback: { status: "unattempted" } }] }),
    createPlaybackAttempt: async () => receipt.promise,
    updatePlaybackAttempt: async (attemptId, update) => { stopped.push(`${attemptId}:${update.state}`); return { attemptId, state: update.state, authorizationGeneration: update.authorizationGeneration }; },
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  const pending = session.authorizePlayback("announcement:job-a", "automatic", "attempt-a");
  await expect(session.leave()).rejects.toThrow("uncertain leave");
  receipt.resolve({ attemptId: "attempt-a", state: "prepared", originGeneration: 7, authorizationGeneration: 7, mediaUrl: "/api/playback/attempts/attempt-a/media" });

  await expect(pending).rejects.toThrow("Playback authorization was invalidated");
  expect(stopped).toEqual(["attempt-a:stopped"]);
});

test("concurrent master and participant settings patches retain every optimistic change until accepted", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const mute = deferred<{ master: { muted: boolean; volume: number; speed: number }; participants: Array<{ sourceId: string; participantId: string; muted: boolean; volume: number }> }>();
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1, joined), readHistory: async () => history(), setListening: async () => ({ scope: joined, generation: 7 }), openEvents: async () => idleEvents(),
    readPlaybackSettings: async () => ({ master: { muted: false, volume: 1, speed: 1 }, participants: [{ sourceId: sourceA, participantId: "participant-a", muted: false, volume: 1 }] }),
    readPlaybackCandidates: async () => ({ scope: joined, candidates: [] }),
    setPlaybackSettings: async (patch) => patch.master ? mute.promise : { master: { muted: false, volume: 1, speed: 1 }, participants: [{ sourceId: sourceA, participantId: "participant-a", muted: false, volume: 0.4 }] },
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  const master = session.setPlaybackSettings({ master: { muted: true } });
  await session.setPlaybackSettings({ participant: { sourceId: sourceA, participantId: "participant-a", volume: 0.4 } });
  expect(session.snapshot().playback?.settings.master.muted).toBe(true);
  mute.resolve({ master: { muted: true, volume: 1, speed: 1 }, participants: [{ sourceId: sourceA, participantId: "participant-a", muted: false, volume: 1 }] });
  await master;

  expect(session.snapshot().playback?.settings).toMatchObject({ master: { muted: true }, participants: [{ participantId: "participant-a", volume: 0.4 }] });
});

test("a Join confirms a playback authorization and Leave fences the next generation", async () => {
  const joined = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, 1),
    readHistory: async () => history(),
    setListening: async (selection) => selection === null ? { scope: null, generation: 8 } : { scope: joined, generation: 7 },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);

  await session.selectSource(sourceA);
  await session.joinWorkspace(sourceA, "workspace-a");
  expect(session.snapshot().playbackAuthorization).toEqual({ scope: joined, generation: 7, confirmed: true, participantIds: [] });
  await session.leave();
  expect(session.snapshot().playbackAuthorization).toEqual({ scope: null, generation: 8, confirmed: true, participantIds: [] });
});

test("session playback authorization names only exact current participant identities", async () => {
  const api: BrowserApi = {
    readState: async (sourceId) => ({ ...state(sourceId ?? sourceA, 1, { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 }), topology: { ...state(sourceId ?? sourceA, 1).topology!, workspaces: [{ id: "workspace-a", sourceId: sourceA, label: "Workspace", order: 0, live: true }], tabs: [{ id: "tab-a", workspaceId: "workspace-a", label: "Tab", order: 0 }], panes: [{ id: "pane-a", tabId: "tab-a", terminalId: "terminal-a" }], participants: [{ id: "participant-current", sourceId: sourceA, paneId: "pane-a", rawPaneId: "raw-a", terminalId: "terminal-a", kind: "codex", generation: 1, active: true }] } } as unknown as StateResponse),
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);

  await session.start(sourceA);

  expect(session.snapshot().playbackAuthorization?.participantIds).toEqual(["participant-current"]);
});

test("browsing fences an obsolete source read without implicitly changing listening scope", async () => {
  const sourceARead = deferred<StateResponse>();
  const writes: Array<unknown> = [];
  const api: BrowserApi = {
    readState: (sourceId) => sourceId === sourceA ? sourceARead.promise : Promise.resolve(state(sourceId ?? sourceB, 4, { sourceId: sourceA, tabId: "tab-a", generation: 1 })),
    readHistory: async () => history(),
    setListening: async (scope) => { writes.push(scope); return { scope: null, generation: 0 }; },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);

  const initial = session.selectSource(sourceA);
  await session.selectSource(sourceB);
  sourceARead.resolve(state(sourceA, 3));
  await initial;

  expect(session.snapshot().selectedSourceId).toBe(sourceB);
  expect(session.snapshot().stateBySource[sourceA]).toBeUndefined();
  expect(session.snapshot().confirmedScope).toEqual({ sourceId: sourceA, tabId: "tab-a", generation: 1 });
  expect(session.snapshot().playbackAuthorization).toEqual({ scope: { sourceId: sourceA, tabId: "tab-a", generation: 1 }, generation: 1, confirmed: true, participantIds: [] });
  expect(writes).toEqual([]);
  session.stop();
});

test("start uses the default authoritative snapshot before opening the one global stream", async () => {
  const reads: Array<string | undefined> = [];
  const cursors: number[] = [];
  const api: BrowserApi = {
    readState: async (sourceId) => {
      reads.push(sourceId);
      return state(sourceB, 9);
    },
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async (cursor) => {
      cursors.push(cursor);
      return idleEvents();
    },
  };
  const session = sessionFor(api);

  await session.start();

  expect(reads).toEqual([undefined]);
  expect(session.snapshot().selectedSourceId).toBe(sourceB);
  expect(cursors).toEqual([9]);
  session.stop();
});

test("an ambiguous scope PUT resnapshots before another write can proceed", async () => {
  const write = deferred<{ scope: StateResponse["scope"]; generation: number }>();
  const stateReads: string[] = [];
  const api: BrowserApi = {
    readState: async (sourceId) => { stateReads.push(sourceId ?? "default"); return state(sourceId ?? sourceA, stateReads.length); },
    readHistory: async () => history(),
    setListening: async () => write.promise,
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.selectSource(sourceA);

  const pending = session.joinWorkspace(sourceA, "workspace-a");
  expect(() => session.leave()).toThrow("scope mutation is pending");
  write.reject(new TypeError("connection reset after request"));
  await expect(pending).rejects.toThrow("connection reset after request");

  expect(stateReads).toEqual([sourceA, sourceA]);
  expect(session.snapshot().scopeStatus).toBe("confirmed");
  session.stop();
});

test("one global stream ignores duplicates, resnapshots on a gap, and treats every event as invalidation", async () => {
  const events = deferred<EventNotification>();
  const stateReads: string[] = [];
  let streamCount = 0;
  const api: BrowserApi = {
    readState: async (sourceId) => { stateReads.push(sourceId ?? sourceA); return state(sourceId ?? sourceA, stateReads.length === 1 ? 20 : 23); },
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async (cursor) => {
      streamCount += 1;
      if (streamCount === 1) {
        expect(cursor).toBe(20);
        return (async function*() {
          yield { id: 20, event: "ignored" };
          yield await events.promise;
        })();
      }
      expect(cursor).toBe(23);
      return idleEvents();
    },
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  events.resolve({ id: 22, event: "untyped" });
  await eventually(() => expect(streamCount).toBe(2));

  expect(stateReads).toEqual([sourceA, sourceA]);
  expect(session.snapshot().stateBySource[sourceA]?.stale).toBe(false);
  session.stop();
});

test("an expired global stream resnapshots before reconnecting from the accepted state cursor", async () => {
  const cursors: number[] = [];
  let stateReads = 0;
  const api: BrowserApi = {
    readState: async (sourceId) => {
      stateReads += 1;
      return state(sourceId ?? sourceA, stateReads === 1 ? 20 : 31);
    },
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async (cursor) => {
      cursors.push(cursor);
      if (cursors.length === 1) throw new BrowserApiError("events_expired", 410);
      return idleEvents();
    },
  };
  const session = sessionFor(api);

  await session.start(sourceA);
  await eventually(() => expect(cursors).toEqual([20, 31]));

  expect(stateReads).toBe(2);
  session.stop();
});

test("a late history page from an old filter cannot replace the reset newest feed", async () => {
  const oldPage = deferred<HistoryPage>();
  const newPage = deferred<HistoryPage>();
  const api: BrowserApi = {
    readState: async () => state(sourceA, 1),
    readHistory: (filters) => filters.sourceId === sourceA ? oldPage.promise : newPage.promise,
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);

  const oldFilter = session.setHistoryFilters({ sourceId: sourceA, order: "desc" });
  const newFilter = session.setHistoryFilters({ sourceId: sourceB, order: "desc" });
  newPage.resolve({ announcements: [], results: [{ jobId: "new", sourceId: sourceB, participantId: "participant-b", createdAt: "2026-10-07T12:00:00.000Z", result: {}, capture: {} }] });
  await newFilter;
  oldPage.resolve({ announcements: [], results: [{ jobId: "old", sourceId: sourceA, participantId: "participant-a", createdAt: "2026-10-07T11:00:00.000Z", result: {}, capture: {} }] });
  await oldFilter;

  expect(session.snapshot().historyFilters.sourceId).toBe(sourceB);
  expect(session.snapshot().history.results.map((item) => item.jobId)).toEqual(["new"]);
  session.stop();
});

test("history exposes loading and error states while preserving the last good page", async () => {
  const delayed = deferred<HistoryPage>();
  let calls = 0;
  const api: BrowserApi = {
    readState: async () => state(sourceA, 1),
    readHistory: async () => ++calls === 1 ? history() : delayed.promise,
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  const filtering = session.setHistoryFilters({ sourceId: sourceB, order: "desc" });
  expect(session.snapshot().historyStatus).toBe("loading");
  delayed.reject(new TypeError("history offline"));
  await filtering;
  expect(session.snapshot().historyStatus).toBe("error");
  expect(session.snapshot().history.results).toEqual([]);
  session.stop();
});

test("failed reconciliation keeps scope unresolved and gates Join and Leave until explicit authoritative retry", async () => {
  let stateReads = 0;
  let writes = 0;
  const api: BrowserApi = {
    readState: async (sourceId) => {
      stateReads += 1;
      if (stateReads === 2 || stateReads === 3) throw new TypeError("reconciliation unavailable");
      return state(sourceId ?? sourceA, stateReads, stateReads === 4 ? { sourceId: sourceA, workspaceId: "workspace-a", generation: 2 } : null);
    },
    readHistory: async () => history(),
    setListening: async () => {
      writes += 1;
      if (writes === 1) throw new TypeError("connection reset after request");
      return { scope: null, generation: 3 };
    },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.selectSource(sourceA);

  await expect(session.joinWorkspace(sourceA, "workspace-a")).rejects.toThrow("connection reset after request");
  expect(session.snapshot().scopeStatus).toBe("unresolved");
  await expect(session.joinTab(sourceA, "tab-a")).rejects.toThrow("scope mutation is pending");
  await expect(session.leave()).rejects.toThrow("scope mutation is pending");

  await session.start(session.snapshot().selectedSourceId);
  expect(session.snapshot().scopeStatus).toBe("unresolved");
  await expect(session.leave()).rejects.toThrow("scope mutation is pending");
  await session.start(session.snapshot().selectedSourceId);
  expect(session.snapshot().scopeStatus).toBe("confirmed");
  expect(session.snapshot().confirmedScope).toEqual({ sourceId: sourceA, workspaceId: "workspace-a", generation: 2 });
  await expect(session.leave()).resolves.toBeUndefined();
  session.stop();
});

test("an abort-ignoring pre-PUT state response cannot replace a newer confirmed scope", async () => {
  const staleRead = deferred<StateResponse>();
  let reads = 0;
  const confirmed = { sourceId: sourceA, workspaceId: "workspace-a", generation: 2 };
  const api: BrowserApi = {
    readState: async (sourceId) => {
      reads += 1;
      return reads === 1 ? state(sourceId ?? sourceA, 1) : staleRead.promise;
    },
    readHistory: async () => history(),
    setListening: async () => ({ scope: confirmed, generation: 2 }),
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.selectSource(sourceA);

  const beforePut = session.selectSource(sourceA);
  await session.joinWorkspace(sourceA, "workspace-a");
  staleRead.resolve(state(sourceA, 2, null));
  await beforePut;

  expect(session.snapshot().confirmedScope).toEqual(confirmed);
  session.stop();
});

test("a state read begun during a pending PUT cannot replace its acknowledged scope", async () => {
  const pendingWrite = deferred<{ scope: StateResponse["scope"]; generation: number }>();
  const oldScopeRead = deferred<StateResponse>();
  let reads = 0;
  const confirmed = { sourceId: sourceA, workspaceId: "workspace-a", generation: 2 };
  const api: BrowserApi = {
    readState: async (sourceId) => ++reads === 2 ? oldScopeRead.promise : state(sourceId ?? sourceA, reads),
    readHistory: async () => history(),
    setListening: async () => pendingWrite.promise,
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.selectSource(sourceA);

  const joining = session.joinWorkspace(sourceA, "workspace-a");
  const duringPut = session.selectSource(sourceA);
  pendingWrite.resolve({ scope: confirmed, generation: 2 });
  await joining;
  oldScopeRead.resolve(state(sourceA, 2, null));
  await duringPut;

  expect(session.snapshot().confirmedScope).toEqual(confirmed);
  session.stop();
});

test("a stopped scope-write owner cannot publish after a restarted session", async () => {
  const oldWrite = deferred<{ scope: StateResponse["scope"]; generation: number }>();
  const oldScope = { sourceId: sourceA, workspaceId: "workspace-old", generation: 2 };
  const newScope = { sourceId: sourceA, workspaceId: "workspace-new", generation: 3 };
  let reads = 0;
  const api: BrowserApi = {
    readState: async (sourceId) => state(sourceId ?? sourceA, ++reads, reads < 2 ? null : newScope),
    readHistory: async () => history(),
    setListening: async () => oldWrite.promise,
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  const joining = session.joinWorkspace(sourceA, "workspace-old");
  session.stop();
  await session.start(sourceA);
  oldWrite.resolve({ scope: oldScope, generation: 2 });
  await joining;

  expect(session.snapshot().confirmedScope).toEqual(newScope);
  session.stop();
});

test("an old failed reconciliation cannot gate a restarted authoritative scope", async () => {
  const reconciliation = deferred<StateResponse>();
  let reads = 0;
  const newer = { sourceId: sourceA, workspaceId: "workspace-new", generation: 3 };
  const api: BrowserApi = {
    readState: async (sourceId) => {
      reads += 1;
      if (reads === 2) return reconciliation.promise;
      return state(sourceId ?? sourceA, reads, reads >= 3 ? newer : null);
    },
    readHistory: async () => history(),
    setListening: async () => { throw new TypeError("ambiguous write"); },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  const failed = session.joinWorkspace(sourceA, "workspace-old");
  await eventually(() => expect(reads).toBe(2));
  session.stop();
  await session.start(sourceA);
  reconciliation.resolve(state(sourceA, 2, null));
  await expect(failed).rejects.toThrow("ambiguous write");
  expect(session.snapshot().confirmedScope).toEqual(newer);
  expect(session.snapshot().scopeStatus).not.toBe("unresolved");
  session.stop();
});

test("an old explicit retry cannot publish after stop and restart", async () => {
  const retryRead = deferred<StateResponse>();
  let reads = 0;
  const newer = { sourceId: sourceA, workspaceId: "workspace-new", generation: 4 };
  const api: BrowserApi = {
    readState: async (sourceId) => {
      reads += 1;
      if (reads === 2) throw new TypeError("first reconciliation failed");
      if (reads === 3) return retryRead.promise;
      return state(sourceId ?? sourceA, reads, newer);
    },
    readHistory: async () => history(),
    setListening: async () => { throw new TypeError("ambiguous"); },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  await expect(session.joinWorkspace(sourceA, "workspace-old")).rejects.toThrow("ambiguous");
  const retry = session.start(session.snapshot().selectedSourceId);
  await eventually(() => expect(reads).toBe(3));
  session.stop();
  await session.start(sourceA);
  retryRead.reject(new TypeError("old retry failed"));
  await retry;
  expect(session.snapshot().confirmedScope).toEqual(newer);
  expect(session.snapshot().scopeStatus).not.toBe("unresolved");
  session.stop();
});

test("subscriptions render all-source invalidation while the selected source is refreshed", async () => {
  const event = deferred<EventNotification>();
  let bReads = 0;
  let stream = 0;
  const updates: string[] = [];
  const api: BrowserApi = {
    readState: async (sourceId) => {
      if (sourceId === sourceB) {
        bReads += 1;
        return state(sourceB, bReads === 1 ? 10 : 11);
      }
      return state(sourceA, 9);
    },
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 0 }),
    openEvents: async (cursor) => {
      stream += 1;
      if (stream === 2) return (async function*() { yield await event.promise; })();
      if (stream === 1) expect(cursor).toBe(9);
      return idleEvents();
    },
  };
  const session = sessionFor(api);
  const unsubscribe = session.subscribe(() => updates.push(session.snapshot().selectedSourceId ?? "none"));

  await session.selectSource(sourceA);
  await session.selectSource(sourceB);
  event.resolve({ id: 11, event: "untyped" });
  await eventually(() => expect(session.snapshot().stateBySource[sourceA]?.stale).toBe(true));

  expect(session.snapshot().stateBySource[sourceB]?.stale).toBe(false);
  expect(updates).toContain(sourceB);
  unsubscribe();
  session.stop();
});


test("a retired start cannot confirm a newer pending listening write", async () => {
  const oldHistory = deferred<HistoryPage>();
  const write = deferred<{ scope: StateResponse["scope"]; generation: number }>();
  let histories = 0;
  const api: BrowserApi = {
    readState: async () => state(sourceA, 1),
    readHistory: async () => ++histories === 1 ? oldHistory.promise : history(),
    setListening: async () => write.promise,
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  const oldStart = session.start(sourceA);
  await eventually(() => expect(histories).toBe(1));
  session.stop();
  await session.start(sourceA);
  const joining = session.joinWorkspace(sourceA, "new-workspace");
  expect(session.snapshot().scopeStatus).toBe("pending");
  oldHistory.resolve(history());
  await oldStart;
  expect(session.snapshot().scopeStatus).toBe("pending");
  const scope = { sourceId: sourceA, workspaceId: "new-workspace", generation: 2 };
  write.resolve({ scope, generation: 2 });
  await joining;
  expect(session.snapshot().confirmedScope).toEqual(scope);
});

test("a superseded retry cannot gate a newer confirmed scope", async () => {
  const oldRead = deferred<StateResponse>();
  let reads = 0;
  const scope = { sourceId: sourceA, workspaceId: "new-workspace", generation: 4 };
  const api: BrowserApi = {
    readState: async () => {
      reads += 1;
      if (reads === 2) throw new TypeError("reconciliation unavailable");
      if (reads === 3) return oldRead.promise;
      return state(sourceA, reads, reads >= 4 ? scope : null);
    },
    readHistory: async () => history(),
    setListening: async () => { throw new TypeError("ambiguous write"); },
    openEvents: async () => idleEvents(),
  };
  const session = sessionFor(api);
  await session.start(sourceA);
  await expect(session.joinWorkspace(sourceA, "old-workspace")).rejects.toThrow("ambiguous write");
  const oldRetry = session.start(session.snapshot().selectedSourceId);
  await eventually(() => expect(reads).toBe(3));
  await session.start(session.snapshot().selectedSourceId);
  expect(session.snapshot().scopeStatus).toBe("confirmed");
  oldRead.reject(new TypeError("old read failed"));
  await oldRetry;
  expect(session.snapshot().scopeStatus).toBe("confirmed");
  expect(session.snapshot().confirmedScope).toEqual(scope);
});

test("Catch up requires an explicit confirmed join, reuses its request key after transport failure, and publishes partial evidence", async () => {
  const requests: Array<{ sourceId: string; generation: number; requestId: string }> = [];
  let attempts = 0;
  const api = {
    readState: async () => state(sourceA, 1, { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 }),
    readHistory: async () => history(),
    setListening: async () => ({ scope: null, generation: 8 }),
    openEvents: async () => idleEvents(),
    requestCatchUp: async (request: { sourceId: string; generation: number; requestId: string }) => {
      requests.push(request);
      if (++attempts === 1) throw new TypeError("network unavailable");
      return { status: "partial", requestId: request.requestId, generation: request.generation, scope: { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 }, entries: [{ participantId: "participant-a", location: { sourceId: sourceA, workspaceId: "workspace-a" }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "partial", text: "The task is blocked.", reason: "Older evidence expired." }] };
    },
  } as BrowserApi & { requestCatchUp: (request: { sourceId: string; generation: number; requestId: string }) => Promise<unknown> };
  const session = sessionFor(api);
  const catchUp = session as typeof session & { catchUp: () => Promise<void> };

  await expect(catchUp.catchUp()).rejects.toThrow("confirmed joined scope");
  expect(requests).toEqual([]);
  await session.start(sourceA);
  await catchUp.catchUp();
  await catchUp.catchUp();

  expect(requests).toHaveLength(2);
  expect(requests.map(({ sourceId: requestSourceId, generation }) => ({ sourceId: requestSourceId, generation }))).toEqual([{ sourceId: sourceA, generation: 7 }, { sourceId: sourceA, generation: 7 }]);
  expect(requests[0]?.requestId).toBe(requests[1]?.requestId);
  expect(session.snapshot()).toMatchObject({ catchUp: { status: "partial", entries: [{ text: "The task is blocked.", reason: "Older evidence expired." }] } });
  await catchUp.catchUp();
  expect(requests[2]?.requestId).not.toBe(requests[1]?.requestId);
});

test("a Catch up response from a source with the same generation is discarded after scope switch", async () => {
  const response = deferred<unknown>();
  const api = {
    readState: async () => state(sourceA, 1, { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 }),
    readHistory: async () => history(),
    setListening: async () => ({ scope: { sourceId: sourceB, tabId: tabB, generation: 7 }, generation: 7 }),
    openEvents: async () => idleEvents(),
    requestCatchUp: async () => response.promise,
  } as BrowserApi & { requestCatchUp: () => Promise<unknown> };
  const session = sessionFor(api);
  const catchUp = session as typeof session & { catchUp: () => Promise<void> };

  await session.start(sourceA);
  const pending = catchUp.catchUp();
  await session.joinTab(sourceB, tabB);
  response.resolve({ status: "complete", requestId: "request-a", generation: 7, scope: { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 }, entries: [{ participantId: "participant-a", location: { sourceId: sourceA, workspaceId: "workspace-a" }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "complete", text: "Completed." }] });
  await pending;

  expect(session.snapshot()).not.toHaveProperty("catchUp");
  expect(session.snapshot().confirmedScope).toEqual({ sourceId: sourceB, tabId: tabB, generation: 7 });
});

test("a completed recap replaces pending text after the persisted completion event", async () => {
  const completion = deferred<EventNotification>();
  const scope = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const requests: string[] = [];
  let streams = 0;
  const api = {
    readState: async () => state(sourceA, streams ? 2 : 1, scope),
    readHistory: async () => history(),
    setListening: async () => ({ scope, generation: 7 }),
    openEvents: async () => ++streams === 1 ? (async function*() { yield await completion.promise; })() : idleEvents(),
    requestCatchUp: async (request: { requestId: string }) => {
      requests.push(request.requestId);
      return requests.length === 1
        ? { status: "pending", requestId: request.requestId, generation: 7, scope, entries: [] }
        : { status: "complete", requestId: request.requestId, generation: 7, scope, entries: [{ participantId: "participant-a", location: { sourceId: sourceA, workspaceId: "workspace-a" }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "complete", text: "The task finished." }] };
    },
  } as BrowserApi;
  const session = sessionFor(api);
  await session.start(sourceA);
  await session.catchUp();
  expect(session.snapshot().catchUp?.status).toBe("pending");
  completion.resolve({ id: 2, event: "completed" });
  await eventually(() => expect(session.snapshot().catchUp?.status).toBe("complete"));
  expect(requests).toEqual([requests[0], requests[0]]);
  session.stop();
});

test("completion arriving during the initial Catch up request still refreshes its frozen key", async () => {
  const completion = deferred<EventNotification>();
  const firstResponse = deferred<unknown>();
  const scope = { sourceId: sourceA, workspaceId: "workspace-a", generation: 7 };
  const requests: string[] = [];
  let streams = 0;
  const api = {
    readState: async () => state(sourceA, streams ? 2 : 1, scope),
    readHistory: async () => history(),
    setListening: async () => ({ scope, generation: 7 }),
    openEvents: async () => ++streams === 1 ? (async function*() { yield await completion.promise; })() : idleEvents(),
    requestCatchUp: async (request: { requestId: string }) => {
      requests.push(request.requestId);
      return requests.length === 1 ? firstResponse.promise : { status: "complete", requestId: request.requestId, generation: 7, scope, entries: [] };
    },
  } as BrowserApi;
  const session = sessionFor(api);
  await session.start(sourceA);
  const started = session.catchUp();
  completion.resolve({ id: 2, event: "completed" });
  await eventually(() => expect(streams).toBe(2));
  firstResponse.resolve({ status: "pending", requestId: requests[0], generation: 7, scope, entries: [] });
  await started;
  await eventually(() => expect(session.snapshot().catchUp?.status).toBe("complete"));
  expect(requests).toEqual([requests[0], requests[0]]);
  session.stop();
});
