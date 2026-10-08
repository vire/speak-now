import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import { PlaybackPanel } from "./playback-panel";

test("scoped candidate status controls replay independently of filtered reading history", () => {
  const html = renderToStaticMarkup(<PlaybackPanel
    scope={{ sourceId: "source-a", workspaceId: "workspace-a", generation: 5 }}
    scopeStatus="confirmed"
    settings={{ master: { muted: false, volume: 1, speed: 1 }, participants: [] }}
    participants={["participant-a"]}
    candidates={[
      { itemId: "recap:job-a", kind: "recap", sourceId: "source-a", workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 2, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "ready", id: "a".repeat(64) }, playback: { status: "unattempted" } },
      { itemId: "announcement:job-b", kind: "announcement", sourceId: "source-a", workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 5, createdAt: "2026-10-08T12:01:00.000Z", media: { state: "pending" }, playback: { status: "unattempted" } },
    ]}
    onSettings={() => undefined}
    onReplay={() => undefined}
  />);

  expect(html).toContain("Not played yet.");
  expect(html).toContain("Replay recap");
  expect(html).toContain("Speech is pending.");
  expect(html).not.toContain("Replay announcement");
});

test("media preparation progress and failure stay distinct from durable playback status", () => {
  const html = renderToStaticMarkup(<PlaybackPanel
    scope={{ sourceId: "source-a", workspaceId: "workspace-a", generation: 5 }} scopeStatus="confirmed"
    settings={{ master: { muted: false, volume: 1, speed: 1 }, participants: [] }} participants={["participant-a"]}
    candidates={[
      { itemId: "announcement:preparing", kind: "announcement", sourceId: "source-a", workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 5, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "preparing" }, playback: { status: "unattempted" } },
      { itemId: "announcement:failed", kind: "announcement", sourceId: "source-a", workspaceId: "workspace-a", participantId: "participant-a", originGeneration: 5, createdAt: "2026-10-08T12:00:00.000Z", media: { state: "failed" }, playback: { status: "unattempted" } },
    ]}
    onSettings={() => undefined} onReplay={() => undefined}
  />);

  expect(html).toContain("Preparing speech.");
  expect(html).toContain("Speech generation failed.");
  expect(html).not.toContain("Not played yet.");
});
