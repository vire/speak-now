import { expect, test } from "bun:test";
import { buildHierarchy, captureDisplay, historyLocation, sourceFreshness } from "./hierarchy";
import type { StateResponse } from "./api";

const now = Date.parse("2026-10-07T12:00:00.000Z");
const sourceA = "source-a";
const sourceB = "source-b";

const selectedState = (): StateResponse => ({
  sources: [
    { sourceId: sourceA, stale: false, observedAt: "2026-10-07T11:59:30.000Z", topologySequence: 3, listeningGeneration: 0 },
    { sourceId: sourceB, stale: true, observedAt: "2026-10-07T11:40:00.000Z", topologySequence: 8, listeningGeneration: 2 },
  ],
  topology: {
    source: { id: sourceA, namespace: sourceA, stale: false, observedAt: "2026-10-07T11:59:30.000Z" },
    workspaces: [
      { id: "workspace-a", sourceId: sourceA, label: "Same", order: 0, live: true },
      { id: "workspace-b", sourceId: sourceA, label: "Same", order: 1, live: true },
    ],
    tabs: [
      { id: "tab-empty", workspaceId: "workspace-a", label: "Same", order: 0 },
      { id: "tab-live", workspaceId: "workspace-a", label: "Same", order: 1 },
      { id: "tab-shell", workspaceId: "workspace-b", label: "Shell", order: 0 },
    ],
    panes: [
      { id: "pane-mixed", tabId: "tab-live", terminalId: "term-1", cwd: "/repo-a", label: "Pane" },
      { id: "pane-unsupported", tabId: "tab-live", terminalId: "term-2", cwd: "/repo-b", label: "Pane" },
      { id: "pane-shell", tabId: "tab-shell", terminalId: "term-3", label: "Shell" },
    ],
    participants: [
      { id: "participant-old", sourceId: sourceA, paneId: "pane-mixed", rawPaneId: "raw-1", terminalId: "term-1", kind: "codex", generation: 0, active: false },
      { id: "participant-new", sourceId: sourceA, paneId: "pane-mixed", rawPaneId: "raw-1", terminalId: "term-1", kind: "codex", generation: 1, active: true },
      { id: "participant-unsupported", sourceId: sourceA, paneId: "pane-unsupported", rawPaneId: "raw-2", terminalId: "term-2", kind: "unsupported", generation: 0, active: true },
    ],
  },
  scope: { sourceId: sourceB, tabId: "tab-other", generation: 2 },
  captureByParticipant: [
    { participantId: "participant-old", capture: null },
    { participantId: "participant-new", capture: { captureMode: "structured", status: "complete", truncated: false, observedAt: "2026-10-07T11:59:00.000Z", expiresAt: "2026-10-07T12:01:00.000Z" } },
    { participantId: "participant-unsupported", capture: { captureMode: "terminal", status: "complete", truncated: true, observedAt: "2026-10-07T11:58:00.000Z", expiresAt: "2026-10-07T11:59:00.000Z" } },
  ],
  jobs: { pending: 0, leased: 0, completed: 1, expired: 0 },
  announcements: { count: 1 },
  eventSequence: 12,
} as unknown as StateResponse);

test("projects complete opaque hierarchy without merging duplicate labels or hiding empty, shell, or unsupported nodes", () => {
  const tree = buildHierarchy(selectedState(), now);

  expect(tree.sourceId).toBe(sourceA);
  expect(tree.workspaces.map((workspace) => [workspace.id, workspace.label, workspace.tabs.map((tab) => [tab.id, tab.panes.length])])).toEqual([
    ["workspace-a", "Same", [["tab-empty", 0], ["tab-live", 2]]],
    ["workspace-b", "Same", [["tab-shell", 1]]],
  ]);
  expect(tree.workspaces[0]?.tabs[1]?.panes.map((pane) => [pane.id, pane.cwd, pane.participants.map((participant) => participant.id)])).toEqual([
    ["pane-mixed", "/repo-a", ["participant-old", "participant-new"]],
    ["pane-unsupported", "/repo-b", ["participant-unsupported"]],
  ]);
  expect(tree.workspaces[1]?.tabs[0]?.panes[0]?.participants).toEqual([]);
  expect(tree.workspaces[0]?.tabs[1]?.panes[0]?.participants.map((participant) => [participant.lifecycle, participant.verifiedBadges, participant.capture.label])).toEqual([
    ["Unknown", "Unknown", "Unknown"],
    ["Unknown", "Unknown", "Last observed capture"],
  ]);
});

test("keeps freshness, capture expiry, and history context independent from current labels", () => {
  const state = selectedState();
  expect(sourceFreshness(state.topology!.source, now, 60_000)).toBe("Fresh");
  expect(sourceFreshness({ ...state.topology!.source, observedAt: "2026-10-07T11:58:59.000Z" }, now, 60_000)).toBe("Stale");
  expect(captureDisplay(state.captureByParticipant[1]?.capture ?? null, now)).toEqual({ label: "Last observed capture", value: "structured - complete - full - observed 2026-10-07T11:59:00.000Z" });
  expect(captureDisplay(state.captureByParticipant[2]?.capture ?? null, now)).toEqual({ label: "Unknown", value: "Unknown" });
  expect(historyLocation({ workspace: { id: "workspace-old", label: "   " }, tab: { id: "tab-old", label: "Former tab" }, pane: { id: "pane-old" }, participant: { id: "participant-old", kind: "codex" } })).toEqual({ workspace: "workspace-old", tab: "Former tab", pane: "pane-old", participant: "participant-old" });
});
