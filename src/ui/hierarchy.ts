import type { CaptureFact, StateResponse } from "./api";

export type CaptureDisplay = { label: "Last observed capture" | "Unknown"; value: string };
export type HierarchyParticipant = { id: string; lifecycle: "Unknown"; verifiedBadges: "Unknown"; capture: CaptureDisplay; kind: string; active: boolean };
export type HierarchyPane = { id: string; label?: string; cwd?: string; participants: HierarchyParticipant[] };
export type HierarchyTab = { id: string; label: string; panes: HierarchyPane[] };
export type HierarchyWorkspace = { id: string; label: string; tabs: HierarchyTab[] };
export type Hierarchy = { sourceId: string; freshness: "Fresh" | "Stale"; workspaces: HierarchyWorkspace[] };

export const sourceFreshness = (source: { stale: boolean; observedAt: string }, now: number, thresholdMs: number): "Fresh" | "Stale" => {
  const observedAt = Date.parse(source.observedAt);
  return source.stale || !Number.isFinite(observedAt) || now - observedAt >= thresholdMs ? "Stale" : "Fresh";
};

export const captureDisplay = (capture: CaptureFact | null, now: number): CaptureDisplay => {
  if (!capture || !Number.isFinite(Date.parse(capture.expiresAt)) || Date.parse(capture.expiresAt) <= now) return { label: "Unknown", value: "Unknown" };
  return { label: "Last observed capture", value: `${capture.captureMode} - ${capture.status} - ${capture.truncated ? "truncated" : "full"} - observed ${capture.observedAt}` };
};

const capturedLabel = (label: string | undefined, id: string | undefined) => label?.trim() || id || "Unknown";

export const historyLocation = (capture: { workspace?: { id: string; label: string }; tab?: { id: string; label: string }; pane?: { id: string; label?: string }; participant: { id: string; kind?: string } }) => ({
  workspace: capturedLabel(capture.workspace?.label, capture.workspace?.id),
  tab: capturedLabel(capture.tab?.label, capture.tab?.id),
  pane: capturedLabel(capture.pane?.label, capture.pane?.id),
  participant: capture.participant.id,
});

export const buildHierarchy = (state: StateResponse, now = Date.now(), freshnessThresholdMs = 60_000): Hierarchy => {
  if (!state.topology) return { sourceId: "", freshness: "Stale", workspaces: [] };
  const { topology } = state;
  const captures = new Map(state.captureByParticipant.map((item) => [item.participantId, item.capture]));
  const participantsFor = (paneId: string): HierarchyParticipant[] => topology.participants
    .filter((participant) => participant.paneId === paneId)
    .map((participant) => ({ id: participant.id, kind: participant.kind, active: participant.active, lifecycle: "Unknown", verifiedBadges: "Unknown", capture: captureDisplay(captures.get(participant.id) ?? null, now) }));
  const panesFor = (tabId: string): HierarchyPane[] => topology.panes
    .filter((pane) => pane.tabId === tabId)
    .map((pane) => ({ id: pane.id, ...(pane.label ? { label: pane.label } : {}), ...(pane.cwd ? { cwd: pane.cwd } : {}), participants: participantsFor(pane.id) }));
  const tabsFor = (workspaceId: string): HierarchyTab[] => topology.tabs
    .filter((tab) => tab.workspaceId === workspaceId)
    .sort((left, right) => left.order - right.order)
    .map((tab) => ({ id: tab.id, label: tab.label, panes: panesFor(tab.id) }));
  return {
    sourceId: topology.source.id,
    freshness: sourceFreshness(topology.source, now, freshnessThresholdMs),
    workspaces: topology.workspaces.slice().sort((left, right) => left.order - right.order).map((workspace) => ({ id: workspace.id, label: workspace.label, tabs: tabsFor(workspace.id) })),
  };
};
