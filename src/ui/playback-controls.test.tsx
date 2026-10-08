import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { PlaybackControls } from "./playback-controls";

test("controls expose durable master and exact participant settings with honest replay status", () => {
  const html = renderToStaticMarkup(<PlaybackControls
    scope={{ sourceId: "source-a", workspaceId: "workspace-a", generation: 5 }}
    scopeStatus="confirmed"
    settings={{ master: { muted: false, volume: 0.7, rate: 1.25 }, participants: { "participant-a": { muted: true, volume: 0.4 } } }}
    participants={["participant-a", "replacement-a"]}
    items={[
      { id: "announcement-a", participantId: "participant-a", kind: "announcement", status: "heard", replayable: true },
      { id: "recap-a", participantId: "replacement-a", kind: "recap", status: "pending", replayable: false },
    ]}
    onSettings={() => undefined}
    onReplay={() => undefined}
  />);

  expect(html).toContain('aria-label="Playback controls"');
  expect(html).toContain("Master mute");
  expect(html).toContain('value="0.7"');
  expect(html).toContain('value="1.25"');
  expect(html).toContain("participant-a mute");
  expect(html).toContain("replacement-a mute");
  expect(html).toContain("Heard");
  expect(html).toContain("Replay announcement");
  expect(html).toContain("Speech is pending.");
});

test("old history stays readable but cannot replay without a confirmed matching join", () => {
  const html = renderToStaticMarkup(<PlaybackControls
    scope={null}
    scopeStatus="confirmed"
    settings={{ master: { muted: false, volume: 1, rate: 1 }, participants: {} }}
    participants={[]}
    items={[{ id: "old-a", participantId: "participant-a", kind: "announcement", status: "failed", replayable: true }]}
    onSettings={() => undefined}
    onReplay={() => undefined}
  />);

  expect(html).toContain("Speech failed.");
  expect(html).toContain("Join this item’s scope to replay.");
  expect(html).toContain('disabled=""');
});

test("blocked playback has its own honest status", () => {
  const item = { id: "blocked-a", participantId: "participant-a", kind: "announcement", status: "blocked", replayable: true } as unknown as Parameters<typeof PlaybackControls>[0]["items"][number];
  const html = renderToStaticMarkup(<PlaybackControls
    scope={null}
    scopeStatus="unresolved"
    settings={{ master: { muted: false, volume: 1, rate: 1 }, participants: {} }}
    participants={[]}
    items={[item]}
    onSettings={() => undefined}
    onReplay={() => undefined}
  />);

  expect(html).toContain("Playback is blocked.");
});

test("a durable unattempted ready recap can offer explicit replay without claiming it was heard", () => {
  const item = { id: "recap-ready", participantId: "participant-a", kind: "recap", status: "unattempted", replayable: true } as unknown as Parameters<typeof PlaybackControls>[0]["items"][number];
  const html = renderToStaticMarkup(<PlaybackControls
    scope={{ sourceId: "source-a", workspaceId: "workspace-a", generation: 5 }}
    scopeStatus="confirmed"
    settings={{ master: { muted: false, volume: 1, rate: 1 }, participants: {} }}
    participants={["participant-a"]}
    items={[item]}
    onSettings={() => undefined}
    onReplay={() => undefined}
  />);

  expect(html).toContain("Not played yet.");
  expect(html).toContain("Replay recap");
  expect(html).not.toContain("Heard");
});

test("durable started does not claim a clip is currently playing", () => {
  const html = renderToStaticMarkup(<PlaybackControls scope={null} scopeStatus="confirmed" settings={{ master: { muted: false, volume: 1, rate: 1 }, participants: {} }} participants={[]} items={[{ id: "started", participantId: "participant-a", kind: "announcement", status: "started", replayable: false }]} onSettings={() => undefined} onReplay={() => undefined} />);
  expect(html).toContain("Playback started.");
  expect(html).not.toContain("Playing.");
});

test("empty or out-of-range speed input does not publish an invalid native rate", () => {
  const calls: unknown[] = [];
  const element = PlaybackControls({ scope: null, scopeStatus: "confirmed", settings: { master: { muted: false, volume: 1, rate: 1 }, participants: {} }, participants: [], items: [], onSettings: (settings) => calls.push(settings), onReplay: () => undefined });
  const seen = new Set<object>();
  const locate = (node: any): any => {
    if (!node || typeof node !== "object" || seen.has(node)) return;
    seen.add(node);
    if (node?.props?.["aria-label"] === "Speech speed") return node;
    const children = node?.props?.children;
    for (const child of Array.isArray(children) ? children : [children]) {
      const found = locate(child);
      if (found) return found;
    }
  };
  const speed = locate(element);
  speed.props.onChange({ target: { value: "" } });
  speed.props.onChange({ target: { value: "3" } });
  expect(calls).toEqual([]);
});
