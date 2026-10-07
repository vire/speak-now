export type AgentKind = "claude" | "codex";

export type SourceId = string & { readonly sourceId: unique symbol };
export type WorkspaceId = string & { readonly workspaceId: unique symbol };
export type TabId = string & { readonly tabId: unique symbol };
export type PaneId = string & { readonly paneId: unique symbol };
export type ParticipantId = string & { readonly participantId: unique symbol };

export type CaptureMode = "structured" | "terminal";
export type CaptureStatus = "complete" | "partial" | "gap" | "limited";

export interface TraceContext { traceId: string; spanId: string; parentSpanId?: string; sourceId?: string; participantId?: string; jobId?: string; announcementId?: string; clipId?: string; }

export interface Source {
  id: SourceId;
  namespace: string;
  stale: boolean;
  observedAt: string;
}

export interface Workspace {
  id: WorkspaceId;
  sourceId: SourceId;
  label: string;
  order: number;
  live: boolean;
}

export interface Tab {
  id: TabId;
  workspaceId: WorkspaceId;
  label: string;
  order: number;
}

export interface Pane {
  id: PaneId;
  tabId: TabId;
  terminalId: string;
  cwd?: string;
  label?: string;
}

export interface Participant {
  id: ParticipantId;
  sourceId: SourceId;
  paneId: PaneId;
  rawPaneId: string;
  terminalId: string;
  kind: AgentKind | "terminal" | "unsupported";
  sessionId?: string;
  sessionReferenceKind?: string;
  sessionReferenceSource?: string;
  generation: number;
  active: boolean;
}

export interface Cursor {
  participantId: ParticipantId;
  sourceCursor: string;
  offset: number;
  initialized: boolean;
  exactSessionVerified?: boolean;
  discardOffset?: number;
  discardScanned?: number;
  terminalLineHashes?: string[];
}

export interface Activity {
  id: string;
  stableMessageId?: string;
  revisionId?: string;
  revisionOf?: string;
  participantId: ParticipantId;
  sourceCursor: string;
  observedAt: string;
  kind: "assistant" | "tool" | "lifecycle" | "unknown";
  text: string;
  captureMode: CaptureMode;
  status: CaptureStatus;
  truncated: boolean;
  excerpt: "full" | "tail";
  originalTextBytes: number;
  trace?: TraceContext;
}

export interface ListeningScope {
  sourceId: SourceId;
  workspaceId?: WorkspaceId;
  tabId?: TabId;
  generation: number;
}

export interface Topology {
  source: Source;
  workspaces: Workspace[];
  tabs: Tab[];
  panes: Pane[];
  participants: Participant[];
}

export const opaqueId = <T extends string>(namespace: string, id: string) =>
  `${namespace}:${id}` as T;

export const COLLECTOR_WIRE_LIMITS = {
  activityTextBytes: 20_000,
  cursorTokenBytes: 20_000,
  requestBytes: 524_288,
  responseBytes: 524_288,
  topologyBytes: 65_536,
  maxArrayItems: 1_000,
} as const;

const sortCollectorJson = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(sortCollectorJson);
  if (value && typeof value === "object") {
    const object = value as Record<string, unknown>;
    return Object.fromEntries(Object.keys(object).sort().flatMap((key) => object[key] === undefined ? [] : [[key, sortCollectorJson(object[key])]]));
  }
  return value;
};

export const encodeCollectorJson = (value: unknown): string => JSON.stringify(sortCollectorJson(value)) ?? "null";
export const collectorUtf8Bytes = (value: string): number => new TextEncoder().encode(value).byteLength;
