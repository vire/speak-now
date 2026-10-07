import { expect, test } from "bun:test";
import { BrowserApiError, createBrowserApi, type BrowserFetch, type HistoryPage, type StateResponse } from "./api";

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
