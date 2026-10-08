import { expect, test } from "bun:test";
import { renderToStaticMarkup } from "react-dom/server";
import type { BrowserSessionSnapshot } from "./session";
import { CatchUpView } from "./catch-up";
import { HierarchyView } from "./tree";

test("Catch up is a native keyboard-operable button with a live partial-evidence announcement", () => {
  const snapshot = {
    ready: true,
    selectedSourceId: "source-a",
    stateBySource: {},
    confirmedScope: { sourceId: "source-a", workspaceId: "workspace-a", generation: 7 },
    scopeStatus: "confirmed",
    history: { announcements: [], results: [] },
    historyStatus: "ready",
    historyFilters: { order: "desc", limit: 50 },
    catchUpPending: false,
    catchUp: { status: "partial", requestId: "request-a", generation: 7, scope: { sourceId: "source-a", workspaceId: "workspace-a", generation: 7 }, entries: [{ participantId: "participant-a", location: { sourceId: "source-a", workspaceId: "workspace-a", tabId: "tab-b" }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "partial", text: "A recap.", reason: "Some evidence expired." }] },
  } as BrowserSessionSnapshot;
  const html = renderToStaticMarkup(<CatchUpView snapshot={snapshot} onCatchUp={() => undefined} />);

  expect(html).toContain('<button type="button">Catch up</button>');
  expect(html).toContain('role="status"');
  expect(html).toContain("A recap.");
  expect(html).toContain("Some evidence expired.");
  expect(html).toContain("tab-b");
  expect(renderToStaticMarkup(<CatchUpView snapshot={{ ...snapshot, catchUp: undefined }} onCatchUp={() => undefined} />)).toContain('aria-live="polite"');
  expect(renderToStaticMarkup(<CatchUpView snapshot={{ ...snapshot, catchUp: { ...snapshot.catchUp!, status: "unavailable", entries: [] } }} onCatchUp={() => undefined} />)).toContain("No retained activity is available for this scope.");
});

test("text-only recap data does not render replay or invent a playback status", () => {
  const snapshot = {
    ready: true,
    stateBySource: {},
    confirmedScope: { sourceId: "source-a", workspaceId: "workspace-a", generation: 7 },
    scopeStatus: "confirmed",
    history: { announcements: [], results: [] },
    historyStatus: "ready",
    historyFilters: { order: "desc", limit: 50 },
    catchUpPending: false,
    catchUp: { status: "complete", requestId: "request-a", generation: 7, scope: { sourceId: "source-a", workspaceId: "workspace-a", generation: 7 }, entries: [{ participantId: "participant-a", location: { sourceId: "source-a", workspaceId: "workspace-a" }, timestamp: "2026-10-07T12:00:00.000Z", evidenceRefs: ["evidence-a"], status: "complete", text: "A recap.", reason: "Complete." }] },
  } as BrowserSessionSnapshot;
  const props = { snapshot, onCatchUp: () => undefined } as React.ComponentProps<typeof CatchUpView>;
  const html = renderToStaticMarkup(<CatchUpView {...props} />);

  expect(html).not.toContain("Replay recap");
  expect(html).not.toContain("Not heard yet.");
});

test("history keeps a completed Catch up recap's partial evidence reason visible", () => {
  const snapshot = { ready: true, stateBySource: {}, confirmedScope: null, scopeStatus: "confirmed", historyStatus: "ready", historyFilters: {}, catchUpPending: false, history: { announcements: [], results: [{ jobId: "catchup:one", sourceId: "source-a", participantId: "participant-a", createdAt: "2026-10-07T12:00:00.000Z", result: { speak: false, kind: "progress", text: "The agent finished a task.", evidenceEventIds: ["event-a"] }, capture: { participant: { id: "participant-a", kind: "codex" } }, catchUp: { status: "partial", reason: "incomplete_evidence" } }] } } as BrowserSessionSnapshot;
  const html = renderToStaticMarkup(<HierarchyView snapshot={snapshot} onBrowse={() => undefined} onRetry={() => undefined} onJoinWorkspace={() => undefined} onJoinTab={() => undefined} onLeave={() => undefined} onHistoryFilters={() => undefined} onLoadMore={() => undefined} />);
  expect(html).toContain("Catch up: partial. Reason: incomplete_evidence.");
});

test("rendered history controls expose durable playback status filtering", () => {
  const snapshot = { ready: true, stateBySource: {}, confirmedScope: null, scopeStatus: "confirmed", historyStatus: "ready", historyFilters: { order: "desc", limit: 50 }, catchUpPending: false, history: { announcements: [], results: [] } } as BrowserSessionSnapshot;
  const html = renderToStaticMarkup(<HierarchyView snapshot={snapshot} onBrowse={() => undefined} onRetry={() => undefined} onJoinWorkspace={() => undefined} onJoinTab={() => undefined} onLeave={() => undefined} onHistoryFilters={() => undefined} onLoadMore={() => undefined} />);

  expect(html).toContain("Playback status");
  expect(html).toContain('value="heard"');
  expect(html).toContain('value="unattempted"');
});
