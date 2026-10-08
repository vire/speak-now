import { expect, test } from "bun:test";
import { BrowserApiError, createBrowserApi, playbackRoutes, type BrowserFetch, type HistoryPage, type StateResponse } from "./api";

const sourceId = "source /?&=one";
const tabId = "tab /?&=two";
const participantId = "participant /?&=three";

const state = (): StateResponse => ({
  sources: [{ sourceId, stale: false, observedAt: "2026-10-07T12:00:00.000Z", topologySequence: 1, listeningGeneration: 0 }],
  topology: null,
  scope: null,
  captureByParticipant: [],
  jobs: { pending: 0, leased: 0, completed: 0, expired: 0 },
  announcements: { count: 0 },
  eventSequence: 8,
} as StateResponse);

const history = (): HistoryPage => ({ announcements: [], results: [] });

const eventFrame = ["id: 8", "event: ingested", "data: ignored", "", ""].join(String.fromCharCode(10));

const consume = async (events: AsyncIterable<unknown>) => {
  for await (const _ of events) undefined;
};

test("frozen playback routes encode opaque item, attempt, and media IDs without selecting an audio DTO", () => {
  expect(playbackRoutes.settings()).toBe("/api/playback/settings");
  expect(playbackRoutes.candidates()).toBe("/api/playback/candidates");
  expect(playbackRoutes.prepare("item /?&=one")).toBe("/api/playback/items/item%20%2F%3F%26%3Done/prepare");
  expect(playbackRoutes.attempts()).toBe("/api/playback/attempts");
  expect(playbackRoutes.attempt("attempt /?&=two")).toBe("/api/playback/attempts/attempt%20%2F%3F%26%3Dtwo");
  expect(playbackRoutes.media("a".repeat(64))).toBe(`/api/playback/media/${"a".repeat(64)}`);
});

test("browser playback transport prepares pending media before it creates and confirms a client-owned attempt", async () => {
  const source = "source-a";
  const participant = "participant-a";
  const itemId = "recap:job-a";
  const attemptId = "attempt-a";
  const requests: Array<{ url: URL; init?: RequestInit }> = [];
  const api = createBrowserApi(async (input, init) => {
    const url = new URL(String(input), "http://speak-now.test");
    requests.push({ url, init });
    if (url.pathname === "/api/playback/settings" && !init?.method) return Response.json({ master: { muted: false, volume: 1, speed: 1 }, participants: [] });
    if (url.pathname === "/api/playback/candidates") return Response.json({ scope: { sourceId: source, workspaceId: "workspace-a", generation: 4 }, candidates: [{ itemId, kind: "recap", sourceId: source, workspaceId: "workspace-a", participantId: participant, originGeneration: 2, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "pending" } }] });
    if (url.pathname.endsWith("/prepare")) return Response.json({ media: { state: "ready", id: "b".repeat(64) } });
    if (url.pathname === `/api/playback/attempts/${attemptId}`) return Response.json({ attemptId, state: "prepared", originGeneration: 2, authorizationGeneration: 4, mediaUrl: `/api/playback/attempts/${attemptId}/media` }, { status: 201 });
    if (url.pathname === `/api/playback/attempts/${attemptId}/status`) return Response.json({ attemptId, state: "started", authorizationGeneration: 4 });
    throw new Error(`unexpected route ${url.pathname}`);
  });

  await expect(api.readPlaybackSettings(source)).resolves.toMatchObject({ master: { speed: 1 } });
  await expect(api.readPlaybackCandidates({ limit: 10, order: "desc" })).resolves.toMatchObject({ candidates: [{ itemId, media: { state: "pending" } }] });
  await expect(api.preparePlaybackItem(itemId, { sourceId: source, participantId: participant, scopeGeneration: 4 })).resolves.toMatchObject({ media: { state: "ready" } });
  await expect(api.createPlaybackAttempt(attemptId, { itemId, sourceId: source, participantId: participant, scopeGeneration: 4, intent: "replay" })).resolves.toMatchObject({ state: "prepared", mediaUrl: `/api/playback/attempts/${attemptId}/media` });
  await expect(api.updatePlaybackAttempt(attemptId, { state: "started", authorizationGeneration: 4 })).resolves.toMatchObject({ state: "started" });

  expect(requests.map(({ url, init }) => ({ path: `${url.pathname}${url.search}`, method: init?.method, body: init?.body }))).toEqual([
    { path: `/api/playback/settings?sourceId=${source}`, method: undefined, body: undefined },
    { path: "/api/playback/candidates?limit=10&order=desc", method: undefined, body: undefined },
    { path: `/api/playback/items/${encodeURIComponent(itemId)}/prepare`, method: "POST", body: JSON.stringify({ sourceId: source, participantId: participant, scopeGeneration: 4 }) },
    { path: `/api/playback/attempts/${attemptId}`, method: "PUT", body: JSON.stringify({ itemId, sourceId: source, participantId: participant, scopeGeneration: 4, intent: "replay" }) },
    { path: `/api/playback/attempts/${attemptId}/status`, method: "PUT", body: JSON.stringify({ state: "started", authorizationGeneration: 4 }) },
  ]);
});

test("browser playback transport rejects mismatched attempt, state, generation, and unsafe settings receipts", async () => {
  const api = createBrowserApi(async (input) => {
    const path = new URL(String(input), "http://speak-now.test").pathname;
    if (path === "/api/playback/settings") return Response.json({ master: { muted: false, volume: 2, speed: 3 }, participants: [] });
    if (path.endsWith("/status")) return Response.json({ attemptId: "attempt-other", state: "started", authorizationGeneration: 4 });
    return Response.json({ attemptId: "attempt-other", state: "prepared", originGeneration: 4, authorizationGeneration: 4, mediaUrl: "/api/playback/attempts/attempt-other/media" });
  });

  await expect(api.readPlaybackSettings("source-a")).rejects.toMatchObject({ code: "malformed_response" });
  await expect(api.createPlaybackAttempt("attempt-a", { itemId: "announcement:job-a", sourceId: "source-a", participantId: "participant-a", scopeGeneration: 4, intent: "automatic" })).rejects.toMatchObject({ code: "malformed_response" });
  await expect(api.updatePlaybackAttempt("attempt-a", { state: "heard", authorizationGeneration: 4 })).rejects.toMatchObject({ code: "malformed_response" });
});

test("browser API encodes opaque IDs and sends each endpoint its own public DTO", async () => {
  const requests: Array<{ url: URL; init?: RequestInit }> = [];
  const fetcher: BrowserFetch = async (input, init) => {
    const url = new URL(String(input), "http://speak-now.test");
    requests.push({ url, init });
    if (url.pathname === "/api/state") return Response.json(state());
    if (url.pathname === "/api/history") return Response.json(history());
    if (url.pathname === "/api/listening") return Response.json({ scope: { sourceId, tabId, generation: 3 }, generation: 3 });
    throw new Error(`unexpected route ${url.pathname}`);
  };
  const api = createBrowserApi(fetcher);

  await api.readState(sourceId);
  await api.readHistory({ sourceId, tabId, participantId, from: "2026-10-07T10:00:00.000Z", to: "2026-10-07T11:00:00.000Z", order: "desc", limit: 20 });
  await expect(api.setListening({ sourceId, tabId })).resolves.toEqual({ scope: { sourceId, tabId, generation: 3 }, generation: 3 });
  await expect(api.setListening(null)).resolves.toEqual({ scope: { sourceId, tabId, generation: 3 }, generation: 3 });

  expect(requests[0]?.url.searchParams.get("sourceId")).toBe(sourceId);
  expect(Object.fromEntries(requests[1]!.url.searchParams)).toEqual({ sourceId, tabId, participantId, from: "2026-10-07T10:00:00.000Z", to: "2026-10-07T11:00:00.000Z", order: "desc", limit: "20" });
  expect(requests.slice(2).map(({ init }) => ({ method: init?.method, body: init?.body }))).toEqual([
    { method: "PUT", body: JSON.stringify({ sourceId, tabId }) },
    { method: "PUT", body: "null" },
  ]);
});

test("browser API sends only the joined-source Catch up fence and decodes an evidence-backed recap", async () => {
  const request = { sourceId, generation: 3, requestId: "request-a" };
  const api = createBrowserApi(async (input, init) => {
    expect(String(input)).toBe("/api/catch-up");
    expect(init).toMatchObject({ method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request) });
    return Response.json({ status: "partial", ...request, scope: { sourceId, tabId, generation: 3 }, entries: [{ participantId, location: { sourceId, tabId }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "partial", text: "A recap.", reason: "Some evidence expired." }] });
  });

  await expect((api as typeof api & { requestCatchUp: (value: typeof request) => Promise<unknown> }).requestCatchUp(request)).resolves.toMatchObject({ status: "partial", entries: [{ text: "A recap.", reason: "Some evidence expired." }] });
});

test("browser API rejects recap text on unavailable evidence", async () => {
  const api = createBrowserApi(async () => Response.json({ status: "unavailable", sourceId, generation: 3, requestId: "request-a", scope: { sourceId, tabId, generation: 3 }, entries: [{ participantId, location: { sourceId, tabId }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: [], status: "unavailable", text: "Invented recap.", reason: "Evidence expired." }] }));

  await expect(api.requestCatchUp({ sourceId, generation: 3, requestId: "request-a" })).rejects.toMatchObject({ code: "malformed_response" });
});

test("browser API decodes a header-cursor SSE stream and rejects expiry or malformed frames", async () => {
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(["id: 8", "event: ingested", "data: ignored", "", ""].join(String.fromCharCode(10))));
      controller.close();
    },
  });
  const api = createBrowserApi(async (input, init) => {
    requests.push({ url: String(input), init });
    return new Response(stream, { status: 200 });
  });
  const events = await api.openEvents(7);
  const received: unknown[] = [];
  for await (const event of events) received.push(event);
  expect(received).toEqual([{ id: 8, event: "ingested" }]);
  expect(requests[0]?.init?.headers).toEqual({ "Last-Event-ID": "7" });

  const expired = createBrowserApi(async () => new Response(null, { status: 410 }));
  await expect(expired.openEvents(7)).rejects.toMatchObject({ code: "events_expired", status: 410 });

  const malformed = createBrowserApi(async () => new Response(["event: ingested", "data: ignored", "", ""].join(String.fromCharCode(10)), { status: 200 }));
  const malformedStream = await malformed.openEvents(7);
  try {
    for await (const _ of malformedStream) undefined;
    throw new Error("expected malformed SSE rejection");
  } catch (error) {
    expect(error).toMatchObject({ code: "malformed_stream" });
  }
});

test("browser API rejects malformed state, history, and listening success DTOs before they can confirm reconciliation", async () => {
  const malformedState = createBrowserApi(async () => Response.json({ eventSequence: 1 }));
  await expect(malformedState.readState()).rejects.toMatchObject({ code: "malformed_response" });

  const malformedHistory = createBrowserApi(async () => Response.json({ announcements: [{ id: "announcement" }], results: [] }));
  await expect(malformedHistory.readHistory({})).rejects.toMatchObject({ code: "malformed_response" });

  const malformedListening = createBrowserApi(async () => Response.json({ scope: { sourceId: "", workspaceId: "", generation: 3 }, generation: 3 }));
  await expect(malformedListening.setListening(null)).rejects.toMatchObject({ code: "malformed_response" });
});

test("browser API rejects a second malformed channel key in state and listening scope successes", async () => {
  const malformedScope = { sourceId, workspaceId: "workspace", tabId: 42, generation: 3 };
  const malformedState = createBrowserApi(async () => Response.json({ ...state(), scope: malformedScope }));
  await expect(malformedState.readState()).rejects.toMatchObject({ code: "malformed_response" });

  const malformedListening = createBrowserApi(async () => Response.json({ scope: malformedScope, generation: 3 }));
  await expect(malformedListening.setListening(null)).rejects.toMatchObject({ code: "malformed_response" });
});

test("browser API releases native SSE readers after normal end, early return, abort, and malformed frames", async () => {
  const normal = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(eventFrame));
      controller.close();
    },
  });
  await consume(await createBrowserApi(async () => new Response(normal)).openEvents(7));
  expect(normal.locked).toBeFalse();

  let earlyCancelled = false;
  const early = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new TextEncoder().encode(eventFrame)); },
    cancel() { earlyCancelled = true; },
  });
  for await (const _ of await createBrowserApi(async () => new Response(early)).openEvents(7)) break;
  expect(earlyCancelled).toBeTrue();
  expect(early.locked).toBeFalse();

  let abortedController: ReadableStreamDefaultController<Uint8Array> | undefined;
  const abort = new AbortController();
  const aborted = new ReadableStream<Uint8Array>({
    start(controller) {
      abortedController = controller;
      abort.signal.addEventListener("abort", () => controller.error(new Error("aborted")), { once: true });
    },
  });
  const abortedEvents = await createBrowserApi(async () => new Response(aborted)).openEvents(7, abort.signal);
  abort.abort();
  await expect(consume(abortedEvents)).rejects.toThrow("aborted");
  expect(abortedController).toBeDefined();
  expect(aborted.locked).toBeFalse();

  let malformedCancelled = false;
  const malformed = new ReadableStream<Uint8Array>({
    start(controller) { controller.enqueue(new TextEncoder().encode(["event: ingested", "", ""].join(String.fromCharCode(10)))); },
    cancel() { malformedCancelled = true; },
  });
  await expect(consume(await createBrowserApi(async () => new Response(malformed)).openEvents(7))).rejects.toMatchObject({ code: "malformed_stream" });
  expect(malformedCancelled).toBeTrue();
  expect(malformed.locked).toBeFalse();
});
