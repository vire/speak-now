import type { Topology } from "../shared";

export type CaptureFact = {
  captureMode: string;
  status: string;
  truncated: boolean;
  observedAt: string;
  expiresAt: string;
};

export type ListeningScopeResponse = { sourceId: string; workspaceId?: string; tabId?: string; generation: number };
export type CatchUpRequest = { sourceId: string; generation: number; requestId: string };
export type CatchUpStatus = "pending" | "partial" | "complete" | "unavailable" | "failed";
export type CatchUpResponse = { status: CatchUpStatus; requestId: string; generation: number; scope: ListeningScopeResponse; entries: Array<{ participantId: string; location: { sourceId: string; workspaceId?: string; tabId?: string; paneId?: string }; timestamp: string; evidenceRefs: string[]; status: CatchUpStatus; text?: string; reason?: string }> };

export type StateResponse = {
  sources: Array<{ sourceId: string; stale: boolean; observedAt: string; topologySequence: number; listeningGeneration: number }>;
  topology: Topology | null;
  scope: ListeningScopeResponse | null;
  captureByParticipant: Array<{ participantId: string; capture: CaptureFact | null }>;
  jobs: { pending: number; leased: number; completed: number; expired: number };
  announcements: { count: number };
  eventSequence: number;
};

export type HistoryFilters = {
  sourceId?: string;
  workspaceId?: string;
  tabId?: string;
  participantId?: string;
  from?: string;
  to?: string;
  cursor?: string;
  limit?: number;
  order?: "asc" | "desc";
  playbackStatus?: PlaybackStatus;
};

export type PlaybackStatus = "unattempted" | "prepared" | "started" | "heard" | "stopped" | "skipped" | "blocked" | "failed";
export type PlaybackSettings = { master: { muted: boolean; volume: number; speed: number }; participants: Array<{ sourceId: string; participantId: string; muted: boolean; volume: number }> };
export type PlaybackSettingsPatch = { master?: Partial<PlaybackSettings["master"]>; participant?: { sourceId: string; participantId: string; muted?: boolean; volume?: number } };
export type PlaybackCandidate = { itemId: string; kind: "announcement" | "recap"; sourceId: string; workspaceId?: string; tabId?: string; participantId: string; originGeneration: number; createdAt: string; media: { state: "pending" | "preparing" | "ready" | "failed"; id?: string }; playback?: PlaybackRecord };
export type PlaybackCandidates = { scope: ListeningScopeResponse | null; candidates: PlaybackCandidate[]; nextCursor?: string };
export type PlaybackPrepareRequest = { sourceId: string; participantId: string; scopeGeneration: number };
export type PlaybackAttemptRequest = { itemId: string; sourceId: string; participantId: string; scopeGeneration: number; intent: "automatic" | "replay" };
export type PlaybackAttemptReceipt = { attemptId: string; state: "prepared" | "started" | "heard" | "stopped" | "skipped" | "blocked" | "failed"; originGeneration?: number; authorizationGeneration: number; mediaUrl?: string };

export type HistoryPage = {
  announcements: Array<{ id: string; jobId: string; sourceId: string; participantId: string; createdAt: string; summary: unknown; playback?: PlaybackRecord }>;
  results: Array<{ jobId: string; sourceId: string; participantId: string; createdAt: string; result: unknown; capture: unknown; catchUp?: { status: "partial" | "complete"; reason: string }; playback?: PlaybackRecord }>;
  nextCursor?: string;
};

export type PlaybackRecord = { status: PlaybackStatus; attemptId?: string; originGeneration?: number; authorizationGeneration?: number };

export type ListeningSelection = { sourceId: string; workspaceId: string } | { sourceId: string; tabId: string } | null;
export type EventNotification = { id: number; event: string };
export type BrowserFetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

const playbackPath = (segment: string) => encodeURIComponent(segment);
export const playbackRoutes = {
  settings: () => "/api/playback/settings",
  candidates: () => "/api/playback/candidates",
  prepare: (itemId: string) => `/api/playback/items/${playbackPath(itemId)}/prepare`,
  attempts: () => "/api/playback/attempts",
  attempt: (attemptId: string) => `/api/playback/attempts/${playbackPath(attemptId)}`,
  attemptStatus: (attemptId: string) => `/api/playback/attempts/${playbackPath(attemptId)}/status`,
  media: (mediaId: string) => `/api/playback/media/${playbackPath(mediaId)}`,
};

export interface BrowserApi {
  readState(sourceId?: string, signal?: AbortSignal): Promise<StateResponse>;
  readHistory(filters: HistoryFilters, signal?: AbortSignal): Promise<HistoryPage>;
  setListening(selection: ListeningSelection, signal?: AbortSignal): Promise<{ scope: ListeningScopeResponse | null; generation: number }>;
  requestCatchUp(request: CatchUpRequest, signal?: AbortSignal): Promise<CatchUpResponse>;
  readPlaybackSettings(sourceId: string, signal?: AbortSignal): Promise<PlaybackSettings>;
  setPlaybackSettings(patch: PlaybackSettingsPatch, signal?: AbortSignal): Promise<PlaybackSettings>;
  readPlaybackCandidates(query?: { limit?: number; cursor?: string; order?: "asc" | "desc" }, signal?: AbortSignal): Promise<PlaybackCandidates>;
  preparePlaybackItem(itemId: string, request: PlaybackPrepareRequest, signal?: AbortSignal): Promise<{ media: PlaybackCandidate["media"] }>;
  createPlaybackAttempt(attemptId: string, request: PlaybackAttemptRequest, signal?: AbortSignal): Promise<PlaybackAttemptReceipt>;
  updatePlaybackAttempt(attemptId: string, update: { state: Exclude<PlaybackStatus, "unattempted" | "prepared">; authorizationGeneration: number }, signal?: AbortSignal): Promise<PlaybackAttemptReceipt>;
  openEvents(cursor: number, signal?: AbortSignal): Promise<AsyncIterable<EventNotification>>;
}

export class BrowserApiError extends Error {
  constructor(readonly code: "http" | "events_expired" | "malformed_response" | "malformed_stream" | "no_joined_scope" | "stale_generation", readonly status?: number) {
    super(code);
  }
}

const json = async (response: Response): Promise<unknown> => {
  if (!response.ok) throw new BrowserApiError(response.status === 410 ? "events_expired" : "http", response.status);
  try {
    return await response.json();
  } catch {
    throw new BrowserApiError("malformed_response");
  }
};

const record = (value: unknown): Record<string, unknown> | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : undefined;

const has = (value: Record<string, unknown>, key: string) => Object.hasOwn(value, key);
const string = (value: unknown): value is string => typeof value === "string";
const identifier = (value: unknown): value is string => string(value) && value.length > 0;
const optionalString = (value: unknown) => value === undefined || string(value);
const boolean = (value: unknown): value is boolean => typeof value === "boolean";
const nonnegativeInteger = (value: unknown): value is number => typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
const finiteNumber = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const array = (value: unknown): value is unknown[] => Array.isArray(value);
const playbackStatus = (value: unknown): value is PlaybackStatus => ["unattempted", "prepared", "started", "heard", "stopped", "skipped", "blocked", "failed"].includes(value as string);

const playbackRecord = (value: unknown): value is PlaybackRecord => {
  const body = record(value);
  return Boolean(body && playbackStatus(body.status) && (!has(body, "attemptId") || identifier(body.attemptId)) && (!has(body, "originGeneration") || nonnegativeInteger(body.originGeneration)) && (!has(body, "authorizationGeneration") || nonnegativeInteger(body.authorizationGeneration)));
};

const scope = (value: unknown): value is ListeningScopeResponse => {
  const body = record(value);
  if (!body || !identifier(body.sourceId) || !nonnegativeInteger(body.generation)) return false;
  const workspace = has(body, "workspaceId");
  const tab = has(body, "tabId");
  return workspace !== tab && (!workspace || identifier(body.workspaceId)) && (!tab || identifier(body.tabId));
};

const nullableScope = (value: unknown): value is ListeningScopeResponse | null => value === null || scope(value);

const source = (value: unknown) => {
  const body = record(value);
  return Boolean(body && string(body.sourceId) && boolean(body.stale) && string(body.observedAt) && nonnegativeInteger(body.topologySequence) && nonnegativeInteger(body.listeningGeneration));
};

const topology = (value: unknown): value is Topology => {
  const body = record(value);
  const sourceValue = body && record(body.source);
  const validSource = sourceValue && string(sourceValue.id) && string(sourceValue.namespace) && boolean(sourceValue.stale) && string(sourceValue.observedAt);
  const workspaces = body?.workspaces;
  const tabs = body?.tabs;
  const panes = body?.panes;
  const participants = body?.participants;
  const validWorkspace = (item: unknown) => {
    const workspace = record(item);
    return Boolean(workspace && string(workspace.id) && string(workspace.sourceId) && string(workspace.label) && finiteNumber(workspace.order) && boolean(workspace.live));
  };
  const validTab = (item: unknown) => {
    const tab = record(item);
    return Boolean(tab && string(tab.id) && string(tab.workspaceId) && string(tab.label) && finiteNumber(tab.order));
  };
  const validPane = (item: unknown) => {
    const pane = record(item);
    return Boolean(pane && string(pane.id) && string(pane.tabId) && string(pane.terminalId) && optionalString(pane.cwd) && optionalString(pane.label));
  };
  const validParticipant = (item: unknown) => {
    const participant = record(item);
    return Boolean(participant && string(participant.id) && string(participant.sourceId) && string(participant.paneId) && string(participant.rawPaneId) && string(participant.terminalId)
      && ["claude", "codex", "terminal", "unsupported"].includes(participant.kind as string) && nonnegativeInteger(participant.generation) && boolean(participant.active)
      && optionalString(participant.sessionId) && optionalString(participant.sessionReferenceKind) && optionalString(participant.sessionReferenceSource));
  };
  return Boolean(validSource && array(workspaces) && workspaces.every(validWorkspace) && array(tabs) && tabs.every(validTab) && array(panes) && panes.every(validPane) && array(participants) && participants.every(validParticipant));
};

const capture = (value: unknown): value is CaptureFact => {
  const body = record(value);
  return Boolean(body && string(body.captureMode) && string(body.status) && boolean(body.truncated) && string(body.observedAt) && string(body.expiresAt));
};

const stateResponse = async (response: Response): Promise<StateResponse> => {
  const value = record(await json(response));
  const jobs = value && record(value.jobs);
  const announcements = value && record(value.announcements);
  if (!value || !array(value.sources) || !value.sources.every(source) || !(value.topology === null || topology(value.topology)) || !nullableScope(value.scope)
    || !array(value.captureByParticipant) || !value.captureByParticipant.every((item) => {
      const participant = record(item);
      return Boolean(participant && string(participant.participantId) && (participant.capture === null || capture(participant.capture)));
    })
    || !jobs || !["pending", "leased", "completed", "expired"].every((key) => nonnegativeInteger(jobs[key]))
    || !announcements || !nonnegativeInteger(announcements.count) || !nonnegativeInteger(value.eventSequence)) throw new BrowserApiError("malformed_response");
  return value as StateResponse;
};

const historyResponse = async (response: Response): Promise<HistoryPage> => {
  const value = record(await json(response));
  const announcements = value?.announcements;
  const results = value?.results;
  const announcement = (item: unknown) => {
    const row = record(item);
    return Boolean(row && string(row.id) && string(row.jobId) && string(row.sourceId) && string(row.participantId) && string(row.createdAt) && has(row, "summary") && (!has(row, "playback") || playbackRecord(row.playback)));
  };
  const result = (item: unknown) => {
    const row = record(item);
    const recap = row && record(row.catchUp);
    return Boolean(row && string(row.jobId) && string(row.sourceId) && string(row.participantId) && string(row.createdAt) && has(row, "result") && has(row, "capture") && (!has(row, "catchUp") || recap && (recap.status === "partial" || recap.status === "complete") && string(recap.reason)) && (!has(row, "playback") || playbackRecord(row.playback)));
  };
  if (!value || !array(announcements) || !announcements.every(announcement) || !array(results) || !results.every(result) || !optionalString(value.nextCursor)) throw new BrowserApiError("malformed_response");
  return value as HistoryPage;
};

const listeningResponse = async (response: Response) => {
  const value = await json(response);
  const body = record(value);
  if (!body || !nullableScope(body.scope) || !nonnegativeInteger(body.generation) || (body.scope !== null && body.scope.generation !== body.generation)) throw new BrowserApiError("malformed_response");
  return { scope: body.scope as ListeningScopeResponse | null, generation: body.generation };
};

const catchUpResponse = async (response: Response): Promise<CatchUpResponse> => {
  if (!response.ok) {
    const body = record(await response.json().catch(() => undefined));
    if (body?.code === "no_joined_scope" || body?.code === "stale_generation") throw new BrowserApiError(body.code, response.status);
    throw new BrowserApiError("http", response.status);
  }
  const body = record(await json(response));
  const entries = body?.entries;
  const entry = (value: unknown) => {
    const item = record(value);
    const location = item && record(item.location);
    const recap = item && ["partial", "complete"].includes(item.status as string);
    return Boolean(item && identifier(item.participantId) && location && identifier(location.sourceId) && (!has(location, "workspaceId") || identifier(location.workspaceId)) && (!has(location, "tabId") || identifier(location.tabId)) && (!has(location, "paneId") || identifier(location.paneId)) && string(item.timestamp) && array(item.evidenceRefs) && item.evidenceRefs.every(identifier) && ["pending", "partial", "complete", "unavailable", "failed"].includes(item.status as string) && (recap ? string(item.text) : !has(item, "text")) && string(item.reason));
  };
  if (!body || !["pending", "partial", "complete", "unavailable", "failed"].includes(body.status as string) || !identifier(body.requestId) || !nonnegativeInteger(body.generation) || !scope(body.scope) || body.scope.generation !== body.generation || !array(entries) || !entries.every(entry)) throw new BrowserApiError("malformed_response");
  return body as CatchUpResponse;
};

const playbackSettingsResponse = async (response: Response): Promise<PlaybackSettings> => {
  const body = record(await json(response));
  const master = body && record(body.master);
  const participants = body?.participants;
  const validSettings = (value: Record<string, unknown> | undefined) => Boolean(value && boolean(value.muted) && finiteNumber(value.volume) && value.volume >= 0 && value.volume <= 1 && finiteNumber(value.speed) && value.speed >= 0.5 && value.speed <= 2);
  const validParticipant = (value: unknown) => {
    const participant = record(value);
    return Boolean(participant && identifier(participant.sourceId) && identifier(participant.participantId) && boolean(participant.muted) && finiteNumber(participant.volume) && participant.volume >= 0 && participant.volume <= 1);
  };
  if (!body || !validSettings(master) || !array(participants) || !participants.every(validParticipant)) throw new BrowserApiError("malformed_response");
  return body as PlaybackSettings;
};

const playbackMedia = (value: unknown): value is PlaybackCandidate["media"] => {
  const media = record(value);
  if (!media || !["pending", "preparing", "ready", "failed"].includes(media.state as string) || (!has(media, "id") && media.state === "ready") || (has(media, "id") && !identifier(media.id))) return false;
  return true;
};

const playbackCandidatesResponse = async (response: Response): Promise<PlaybackCandidates> => {
  const body = record(await json(response));
  const candidates = body?.candidates;
  const candidate = (value: unknown) => {
    const item = record(value);
    return Boolean(item && identifier(item.itemId) && (item.kind === "announcement" || item.kind === "recap") && identifier(item.sourceId) && (!has(item, "workspaceId") || identifier(item.workspaceId)) && (!has(item, "tabId") || identifier(item.tabId)) && identifier(item.participantId) && nonnegativeInteger(item.originGeneration) && string(item.createdAt) && playbackMedia(item.media) && (!has(item, "playback") || playbackRecord(item.playback)));
  };
  if (!body || !nullableScope(body.scope) || !array(candidates) || !candidates.every(candidate) || !optionalString(body.nextCursor)) throw new BrowserApiError("malformed_response");
  return body as PlaybackCandidates;
};

const playbackPrepareResponse = async (response: Response): Promise<{ media: PlaybackCandidate["media"] }> => {
  const body = record(await json(response));
  if (!body || !playbackMedia(body.media)) throw new BrowserApiError("malformed_response");
  return body as { media: PlaybackCandidate["media"] };
};

const playbackAttemptResponse = async (response: Response): Promise<PlaybackAttemptReceipt> => {
  const body = record(await json(response));
  if (!body || !identifier(body.attemptId) || !playbackStatus(body.state) || !nonnegativeInteger(body.authorizationGeneration) || (!has(body, "originGeneration") && body.state === "prepared") || (has(body, "originGeneration") && !nonnegativeInteger(body.originGeneration)) || (has(body, "mediaUrl") && !identifier(body.mediaUrl))) throw new BrowserApiError("malformed_response");
  return body as PlaybackAttemptReceipt;
};

const append = (parameters: URLSearchParams, name: string, value: string | number | undefined) => {
  if (value !== undefined) parameters.append(name, String(value));
};

const parseSse = async function*(response: Response): AsyncIterable<EventNotification> {
  if (!response.body) throw new BrowserApiError("malformed_stream");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const frames = () => {
    const complete = buffer.split("\n\n");
    buffer = complete.pop() ?? "";
    return complete;
  };
  const notification = (frame: string): EventNotification => {
    const lines = frame.replaceAll("\r", "").split("\n");
    const id = lines.find((line) => line.startsWith("id: "))?.slice(4);
    const event = lines.find((line) => line.startsWith("event: "))?.slice(7);
    if (!id || !event || !/^(0|[1-9][0-9]*)$/.test(id)) throw new BrowserApiError("malformed_stream");
    return { id: Number(id), event };
  };
  try {
    while (true) {
      const { done, value } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });
      for (const frame of frames()) {
        if (frame) yield notification(frame);
      }
      if (done) break;
    }
    if (buffer.trim()) throw new BrowserApiError("malformed_stream");
  } finally {
    try {
      await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
};

export const createBrowserApi = (fetcher: BrowserFetch = fetch): BrowserApi => ({
  async readState(sourceId, signal) {
    const parameters = new URLSearchParams();
    if (sourceId !== undefined) parameters.set("sourceId", sourceId);
    return stateResponse(await fetcher(`/api/state${parameters.size ? `?${parameters}` : ""}`, { signal }));
  },
  async readHistory(filters, signal) {
    const parameters = new URLSearchParams();
    append(parameters, "sourceId", filters.sourceId);
    append(parameters, "workspaceId", filters.workspaceId);
    append(parameters, "tabId", filters.tabId);
    append(parameters, "participantId", filters.participantId);
    append(parameters, "from", filters.from);
    append(parameters, "to", filters.to);
    append(parameters, "order", filters.order);
    append(parameters, "cursor", filters.cursor);
    append(parameters, "limit", filters.limit);
    append(parameters, "playbackStatus", filters.playbackStatus);
    return historyResponse(await fetcher(`/api/history${parameters.size ? `?${parameters}` : ""}`, { signal }));
  },
  async setListening(selection, signal) {
    return listeningResponse(await fetcher("/api/listening", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(selection),
      signal,
    }));
  },
  async requestCatchUp(request, signal) {
    return catchUpResponse(await fetcher("/api/catch-up", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request), signal }));
  },
  async readPlaybackSettings(sourceId, signal) {
    const parameters = new URLSearchParams({ sourceId });
    return playbackSettingsResponse(await fetcher(`${playbackRoutes.settings()}?${parameters}`, { signal }));
  },
  async setPlaybackSettings(patch, signal) {
    return playbackSettingsResponse(await fetcher(playbackRoutes.settings(), { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(patch), signal }));
  },
  async readPlaybackCandidates(query = {}, signal) {
    const parameters = new URLSearchParams();
    append(parameters, "limit", query.limit);
    append(parameters, "cursor", query.cursor);
    append(parameters, "order", query.order);
    return playbackCandidatesResponse(await fetcher(`${playbackRoutes.candidates()}${parameters.size ? `?${parameters}` : ""}`, { signal }));
  },
  async preparePlaybackItem(itemId, request, signal) {
    return playbackPrepareResponse(await fetcher(playbackRoutes.prepare(itemId), { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request), signal }));
  },
  async createPlaybackAttempt(attemptId, request, signal) {
    const receipt = await playbackAttemptResponse(await fetcher(playbackRoutes.attempt(attemptId), { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request), signal }));
    if (receipt.attemptId !== attemptId || receipt.authorizationGeneration !== request.scopeGeneration || receipt.state !== "prepared" || receipt.originGeneration === undefined || !receipt.mediaUrl) throw new BrowserApiError("malformed_response");
    return receipt;
  },
  async updatePlaybackAttempt(attemptId, update, signal) {
    const receipt = await playbackAttemptResponse(await fetcher(playbackRoutes.attemptStatus(attemptId), { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(update), signal }));
    if (receipt.attemptId !== attemptId || receipt.authorizationGeneration !== update.authorizationGeneration || receipt.state !== update.state) throw new BrowserApiError("malformed_response");
    return receipt;
  },
  async openEvents(cursor, signal) {
    const response = await fetcher("/api/events", { headers: { "Last-Event-ID": String(cursor) }, signal });
    if (!response.ok) throw new BrowserApiError(response.status === 410 ? "events_expired" : "http", response.status);
    return parseSse(response);
  },
});
