import { BrowserApiError, type BrowserApi, type CatchUpResponse, type HistoryFilters, type HistoryPage, type ListeningScopeResponse, type ListeningSelection, type PlaybackAttemptReceipt, type PlaybackCandidates, type PlaybackSettings, type PlaybackSettingsPatch, type StateResponse } from "./api";

type CachedState = { state: StateResponse; stale: boolean };
type ScopeStatus = "idle" | "pending" | "confirmed" | "failed" | "unresolved";
export type PlaybackAuthorization = { scope: ListeningScopeResponse | null; generation: number; confirmed: boolean; participantIds: string[] };

export type BrowserSessionSnapshot = {
  ready: boolean;
  selectedSourceId?: string;
  stateBySource: Record<string, CachedState>;
  confirmedScope: StateResponse["scope"];
  scopeStatus: ScopeStatus;
  history: HistoryPage;
  historyStatus: "loading" | "ready" | "error";
  historyFilters: Omit<HistoryFilters, "cursor">;
  catchUp?: CatchUpResponse;
  catchUpPending: boolean;
  playbackAuthorization?: PlaybackAuthorization;
  playback?: { scope: ListeningScopeResponse; settings: PlaybackSettings; candidates: PlaybackCandidates["candidates"]; nextCursor?: string };
  transportError?: string;
};

const emptyHistory = (): HistoryPage => ({ announcements: [], results: [] });
const withoutCursor = ({ cursor: _cursor, ...filters }: HistoryFilters): Omit<HistoryFilters, "cursor"> => filters;
const sameScope = (left: ListeningScopeResponse | null, right: ListeningScopeResponse | null) => left?.sourceId === right?.sourceId && left?.workspaceId === right?.workspaceId && left?.tabId === right?.tabId && left?.generation === right?.generation;
const applyPlaybackSettingsPatch = (current: PlaybackSettings, patch: PlaybackSettingsPatch, accepted?: PlaybackSettings): PlaybackSettings => {
  const masterPatch = patch.master;
  const master = !masterPatch ? current.master : {
    ...current.master,
    ...(masterPatch.muted === undefined ? {} : { muted: accepted?.master.muted ?? masterPatch.muted }),
    ...(masterPatch.volume === undefined ? {} : { volume: accepted?.master.volume ?? masterPatch.volume }),
    ...(masterPatch.speed === undefined ? {} : { speed: accepted?.master.speed ?? masterPatch.speed }),
  };
  const participantPatch = patch.participant;
  if (!participantPatch) return { master, participants: current.participants };
  const acceptedParticipant = accepted?.participants.find((participant) => participant.sourceId === participantPatch.sourceId && participant.participantId === participantPatch.participantId);
  const existing = current.participants.find((participant) => participant.sourceId === participantPatch.sourceId && participant.participantId === participantPatch.participantId);
  const participant = {
    sourceId: participantPatch.sourceId,
    participantId: participantPatch.participantId,
    muted: participantPatch.muted === undefined ? existing?.muted ?? false : acceptedParticipant?.muted ?? participantPatch.muted,
    volume: participantPatch.volume === undefined ? existing?.volume ?? 1 : acceptedParticipant?.volume ?? participantPatch.volume,
  };
  return {
    master,
    participants: existing
      ? current.participants.map((entry) => entry === existing ? participant : entry)
      : [...current.participants, participant],
  };
};

export const createBrowserSession = (api: BrowserApi) => {
  let stopped = false;
  let selectedSourceId: string | undefined;
  let stateBySource: Record<string, CachedState> = {};
  let confirmedScope: StateResponse["scope"] = null;
  let playbackGeneration = 0;
  let playback: BrowserSessionSnapshot["playback"];
  let playbackRead = 0;
  let playbackSettingsVersion = 0;
  let pendingPlaybackSettingsWrites = 0;
  let scopeStatus: ScopeStatus = "idle";
  let historyFilters: Omit<HistoryFilters, "cursor"> = { order: "desc", limit: 50 };
  let history = emptyHistory();
  let historyStatus: "loading" | "ready" | "error" = "loading";
  let catchUp: CatchUpResponse | undefined;
  let catchUpPending = false;
  let catchUpRequest: { scope: ListeningScopeResponse; requestId: string } | undefined;
  let retryCatchUp = false;
  let catchUpInvalidated = false;
  let catchUpRead = 0;
  let transportError: string | undefined;
  let ready = false;
  let stateRead = 0;
  let joinedStateRead = 0;
  let historyRead = 0;
  let streamOwner = 0;
  let eventCursor: number | undefined;
  let scopeEpoch = 0;
  let lifecycle = 0;
  let scopeUnresolved = false;
  let stateController: AbortController | undefined;
  let historyController: AbortController | undefined;
  let streamController: AbortController | undefined;
  let scopeWrite: Promise<void> | undefined;
  const listeners = new Set<() => void>();

  const playbackParticipantIds = () => {
    if (!confirmedScope) return [];
    const topology = stateBySource[confirmedScope.sourceId]?.state.topology;
    if (!topology) return [];
    const panes = new Map(topology.panes.map((pane) => [pane.id, pane]));
    const tabs = new Map(topology.tabs.map((tab) => [tab.id, tab]));
    return topology.participants
      .filter((participant) => {
        if (!participant.active || participant.sourceId !== confirmedScope?.sourceId) return false;
        const pane = panes.get(participant.paneId);
        const tab = pane ? tabs.get(pane.tabId) : undefined;
        return confirmedScope.workspaceId ? tab?.workspaceId === confirmedScope.workspaceId : tab?.id === confirmedScope.tabId;
      })
      .map((participant) => participant.id)
      .sort();
  };
  const view = (): BrowserSessionSnapshot => ({ ready, selectedSourceId, stateBySource, confirmedScope, scopeStatus, history, historyStatus, historyFilters, catchUpPending, playbackAuthorization: { scope: confirmedScope, generation: playbackGeneration, confirmed: scopeStatus === "confirmed" && !scopeUnresolved && !scopeWrite, participantIds: playbackParticipantIds() }, ...(playback ? { playback } : {}), ...(catchUp ? { catchUp } : {}), ...(transportError ? { transportError } : {}) });
  let current = view();
  const publish = () => {
    current = view();
    for (const listener of listeners) listener();
  };
  const snapshot = () => current;
  const clearCatchUp = () => { catchUpRead += 1; catchUp = undefined; catchUpPending = false; catchUpRequest = undefined; retryCatchUp = false; catchUpInvalidated = false; };

  const markAllStale = () => {
    stateBySource = Object.fromEntries(Object.entries(stateBySource).map(([sourceId, cached]) => [sourceId, { ...cached, stale: true }]));
    publish();
  };

  const stopStream = () => {
    streamOwner += 1;
    streamController?.abort();
    streamController = undefined;
  };

  const refreshHistory = async (nextCursor?: string) => {
    const read = ++historyRead;
    historyController?.abort();
    const controller = new AbortController();
    historyController = controller;
    historyStatus = "loading";
    publish();
    try {
      const page = await api.readHistory({ ...historyFilters, ...(nextCursor ? { cursor: nextCursor } : {}) }, controller.signal);
      if (stopped || read !== historyRead) return;
      if (!nextCursor) {
        history = page;
        historyStatus = "ready";
        publish();
        return;
      }
      const resultIds = new Set(history.results.map((item) => item.jobId));
      const announcementIds = new Set(history.announcements.map((item) => item.id));
      history = {
        results: [...history.results, ...page.results.filter((item) => !resultIds.has(item.jobId))],
        announcements: [...history.announcements, ...page.announcements.filter((item) => !announcementIds.has(item.id))],
        ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
      };
      historyStatus = "ready";
      publish();
    } catch (error) {
      if (!controller.signal.aborted && read === historyRead) {
        transportError = error instanceof Error ? error.message : "history_failed";
        historyStatus = "error";
        publish();
      }
    }
  };

  const refreshPlayback = async () => {
    const joined = confirmedScope;
    if (!joined || scopeStatus !== "confirmed" || scopeWrite || scopeUnresolved) {
      playback = undefined;
      publish();
      return;
    }
    const read = ++playbackRead;
    const settingsVersion = playbackSettingsVersion;
    const [settings, firstPage] = await Promise.all([api.readPlaybackSettings(joined.sourceId), api.readPlaybackCandidates({ limit: 50, order: "desc" })]);
    const candidates = [...firstPage.candidates];
    let cursor = firstPage.nextCursor;
    const cursors = new Set<string>();
    while (cursor && !cursors.has(cursor)) {
      cursors.add(cursor);
      const page = await api.readPlaybackCandidates({ limit: 50, cursor, order: "desc" });
      if (!sameScope(page.scope, joined)) return;
      candidates.push(...page.candidates);
      cursor = page.nextCursor;
    }
    if (stopped || read !== playbackRead || !sameScope(confirmedScope, joined) || scopeStatus !== "confirmed" || scopeWrite || scopeUnresolved || !sameScope(firstPage.scope, joined)) return;
    const retainedSettings = playback && sameScope(playback.scope, joined) ? playback.settings : undefined;
    playback = { scope: joined, settings: pendingPlaybackSettingsWrites || playbackSettingsVersion !== settingsVersion ? retainedSettings ?? settings : settings, candidates };
    publish();
  };

  const playbackCandidate = (itemId: string) => {
    const joined = confirmedScope;
    const currentPlayback = playback;
    const candidate = currentPlayback?.candidates.find((item) => item.itemId === itemId);
    if (!joined || !currentPlayback || !candidate || scopeStatus !== "confirmed" || scopeWrite || scopeUnresolved || !sameScope(currentPlayback.scope, joined)) throw new Error("Playback requires a confirmed joined scope");
    return { joined, candidate };
  };

  const preparePlaybackItem = async (itemId: string) => {
    const { joined, candidate } = playbackCandidate(itemId);
    if (candidate.media.state === "ready" || candidate.media.state === "failed") return;
    await api.preparePlaybackItem(itemId, { sourceId: joined.sourceId, participantId: candidate.participantId, scopeGeneration: joined.generation });
    await refreshPlayback();
  };

  const authorizePlayback = async (itemId: string, intent: "automatic" | "replay", attemptId: string): Promise<PlaybackAttemptReceipt> => {
    const { joined, candidate } = playbackCandidate(itemId);
    if (candidate.media.state !== "ready") throw new Error("Playback media is not ready");
    const authorizationScopeEpoch = scopeEpoch;
    const authorizationLifecycle = lifecycle;
    const receipt = await api.createPlaybackAttempt(attemptId, { itemId, sourceId: joined.sourceId, participantId: candidate.participantId, scopeGeneration: joined.generation, intent });
    const stillAuthorized = !stopped
      && authorizationLifecycle === lifecycle
      && authorizationScopeEpoch === scopeEpoch
      && scopeStatus === "confirmed"
      && !scopeWrite
      && !scopeUnresolved
      && sameScope(confirmedScope, joined);
    if (stillAuthorized) return receipt;
    if (receipt.state === "prepared") {
      try {
        await api.updatePlaybackAttempt(receipt.attemptId, { state: "stopped", authorizationGeneration: receipt.authorizationGeneration });
      } catch (error) {
        transportError = error instanceof Error ? error.message : "playback_stop_failed";
        publish();
      }
    }
    throw new Error("Playback authorization was invalidated");
  };

  const setPlaybackSettings = async (patch: PlaybackSettingsPatch) => {
    const joined = confirmedScope;
    if (!joined || scopeStatus !== "confirmed") throw new Error("Playback settings require a confirmed joined scope");
    playbackSettingsVersion += 1;
    pendingPlaybackSettingsWrites += 1;
    if (playback && sameScope(playback.scope, joined)) {
      playback = { ...playback, settings: applyPlaybackSettingsPatch(playback.settings, patch) };
      publish();
    }
    try {
      const settings = await api.setPlaybackSettings(patch);
      if (!stopped && sameScope(confirmedScope, joined) && playback) {
        playback = { ...playback, settings: applyPlaybackSettingsPatch(playback.settings, patch, settings) };
        publish();
        playbackSettingsVersion += 1;
      }
    } finally {
      pendingPlaybackSettingsWrites -= 1;
    }
  };

  const acknowledgePlaybackAttempt = async (attemptId: string, update: { state: Exclude<import("./api").PlaybackStatus, "unattempted" | "prepared">; authorizationGeneration: number }) => {
    const acknowledgedScope = confirmedScope;
    const acknowledgementLifecycle = lifecycle;
    const receipt = await api.updatePlaybackAttempt(attemptId, update);
    if (stopped || acknowledgementLifecycle !== lifecycle || !sameScope(confirmedScope, acknowledgedScope)) return receipt;
    void Promise.all([refreshPlayback(), refreshHistory()]).catch((error) => {
      if (stopped || acknowledgementLifecycle !== lifecycle || !sameScope(confirmedScope, acknowledgedScope)) return;
      transportError = error instanceof Error ? error.message : "playback_refresh_failed";
      publish();
    });
    return receipt;
  };

  const startStream = (cursor: number) => {
    stopStream();
    const owner = ++streamOwner;
    const controller = new AbortController();
    streamController = controller;
    void (async () => {
      try {
        const events = await api.openEvents(cursor, controller.signal);
        for await (const event of events) {
          if (stopped || owner !== streamOwner || controller.signal.aborted) return;
          if (event.id <= (eventCursor ?? cursor)) continue;
          await resnapshot();
          return;
        }
        if (!stopped && owner === streamOwner && !controller.signal.aborted) await resnapshot();
      } catch (error) {
        if (!stopped && owner === streamOwner && !controller.signal.aborted) {
          transportError = error instanceof Error ? error.message : "events_failed";
          await resnapshot();
        }
      }
    })();
  };

  const refreshState = async (): Promise<boolean> => {
    const requestedSourceId = selectedSourceId;
    const readScopeEpoch = scopeEpoch;
    const read = ++stateRead;
    stateController?.abort();
    const controller = new AbortController();
    stateController = controller;
    try {
      const state = await api.readState(requestedSourceId, controller.signal);
      const sourceId = state.topology?.source.id ?? requestedSourceId;
      if (stopped || read !== stateRead || (requestedSourceId !== undefined && selectedSourceId !== requestedSourceId)) return false;
      if (sourceId) {
        selectedSourceId = sourceId;
        stateBySource = { ...stateBySource, [sourceId]: { state, stale: false } };
      }
        if (readScopeEpoch === scopeEpoch) {
          if (!sameScope(confirmedScope, state.scope)) clearCatchUp();
          confirmedScope = state.scope;
          playbackGeneration = state.scope?.generation ?? state.sources.find((source) => source.sourceId === sourceId)?.listeningGeneration ?? playbackGeneration;
          if (!scopeWrite && !scopeUnresolved) scopeStatus = "confirmed";
        }
      ready = true;
      eventCursor = state.eventSequence;
      transportError = undefined;
      publish();
      startStream(eventCursor);
      return true;
    } catch (error) {
      if (!controller.signal.aborted && read === stateRead) {
        transportError = error instanceof Error ? error.message : "state_failed";
        publish();
      }
      return false;
    }
  };

  const refreshJoinedSource = async () => {
    const joined = confirmedScope;
    if (!joined || joined.sourceId === selectedSourceId) return;
    const read = ++joinedStateRead;
    try {
      const state = await api.readState(joined.sourceId);
      if (stopped || read !== joinedStateRead || !sameScope(confirmedScope, joined) || scopeWrite || scopeUnresolved) return;
      const sourceId = state.topology?.source.id ?? joined.sourceId;
      stateBySource = { ...stateBySource, [sourceId]: { state, stale: false } };
      if (!sameScope(confirmedScope, state.scope)) {
        clearCatchUp();
        confirmedScope = state.scope;
        playbackGeneration = state.scope?.generation ?? state.sources.find((source) => source.sourceId === sourceId)?.listeningGeneration ?? playbackGeneration;
      }
      publish();
    } catch (error) {
      if (!stopped && read === joinedStateRead) {
        transportError = error instanceof Error ? error.message : "joined_state_failed";
        publish();
      }
    }
  };

  const resnapshot = async () => {
    markAllStale();
    const [stateRead] = await Promise.all([refreshState(), refreshHistory()]);
    if (stateRead) {
      await refreshJoinedSource();
      if (typeof api.readPlaybackSettings === "function" && typeof api.readPlaybackCandidates === "function") {
        try {
          await refreshPlayback();
        } catch (error) {
          transportError = error instanceof Error ? error.message : "playback_failed";
          publish();
        }
      }
    }
    if (stateRead && catchUp?.status === "pending" && catchUpRequest) {
      if (catchUpPending) catchUpInvalidated = true;
      else await requestCatchUp();
    }
    return stateRead;
  };

  const selectSource = async (sourceId: string) => {
    selectedSourceId = sourceId;
    publish();
    await refreshState();
  };

  const setListening = async (selection: ListeningSelection) => {
    if (scopeWrite || scopeUnresolved) throw new Error("scope mutation is pending");
    const writeScopeEpoch = ++scopeEpoch;
    const writeLifecycle = lifecycle;
    stateRead += 1;
    stateController?.abort();
    clearCatchUp();
    scopeStatus = "pending";
    publish();
    const write = (async () => {
      try {
        const response = await api.setListening(selection);
        if (!stopped && writeLifecycle === lifecycle && writeScopeEpoch === scopeEpoch) {
            confirmedScope = response.scope;
            playbackGeneration = response.generation;
          scopeStatus = "confirmed";
          scopeEpoch += 1;
          publish();
        }
      } catch (error) {
        if (stopped || writeLifecycle !== lifecycle) throw error;
        scopeStatus = "failed";
        publish();
        const reconciled = await resnapshot();
        if (stopped || writeLifecycle !== lifecycle) throw error;
        scopeUnresolved = !reconciled;
        scopeStatus = reconciled ? "confirmed" : "unresolved";
        publish();
        throw error;
      }
    })();
    scopeWrite = write;
      try {
        await write;
      } finally {
        if (scopeWrite === write) {
          scopeWrite = undefined;
          publish();
        }
      }
  };

  const requestCatchUp = async () => {
    const scope = confirmedScope;
    if (!scope || scopeStatus !== "confirmed" || scopeWrite || scopeUnresolved) throw new Error("Catch up requires a confirmed joined scope");
    const request = retryCatchUp && catchUpRequest && sameScope(catchUpRequest.scope, scope) ? catchUpRequest : { scope, requestId: crypto.randomUUID() };
    const read = ++catchUpRead;
    catchUpRequest = request;
    catchUpPending = true;
    catchUpInvalidated = false;
    catchUp = { status: "pending", requestId: request.requestId, generation: scope.generation, scope, entries: [] };
    publish();
    try {
      const response = await api.requestCatchUp({ sourceId: scope.sourceId, generation: scope.generation, requestId: request.requestId });
      if (stopped || read !== catchUpRead || !sameScope(confirmedScope, scope) || scopeStatus !== "confirmed") return;
      if (!sameScope(response.scope, scope)) {
        clearCatchUp();
        publish();
        return;
      }
      catchUpPending = false;
      catchUp = response;
      retryCatchUp = response.status === "pending";
      if (retryCatchUp && catchUpInvalidated) return await requestCatchUp();
    } catch (error) {
      if (stopped || read !== catchUpRead || !sameScope(confirmedScope, scope) || scopeStatus !== "confirmed") return;
      if (error instanceof BrowserApiError && (error.code === "no_joined_scope" || error.code === "stale_generation")) clearCatchUp();
      else {
        catchUpPending = false;
        retryCatchUp = true;
        catchUp = { status: "failed", requestId: request.requestId, generation: scope.generation, scope, entries: [{ participantId: "unavailable", location: { sourceId: scope.sourceId }, timestamp: new Date().toISOString(), evidenceRefs: [], status: "failed", reason: "Catch up could not be completed. Retry this request." }] };
      }
    }
    publish();
  };

  return {
    snapshot,
    async start(sourceId?: string) {
      stopped = false;
      if (sourceId) selectedSourceId = sourceId;
      const startLifecycle = lifecycle;
      const startScopeEpoch = scopeEpoch;
      const startRead = stateRead + 1;
      const started = await resnapshot();
      if (started && !stopped && startLifecycle === lifecycle && startScopeEpoch === scopeEpoch && startRead === stateRead && !scopeWrite) {
        scopeUnresolved = false;
        scopeStatus = "confirmed";
        publish();
      }
    },
    selectSource,
    joinWorkspace: (sourceId: string, workspaceId: string) => setListening({ sourceId, workspaceId }),
    joinTab: (sourceId: string, tabId: string) => setListening({ sourceId, tabId }),
    leave: () => setListening(null),
    setListening,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    async setHistoryFilters(filters: HistoryFilters) {
      historyFilters = withoutCursor(filters);
      history = emptyHistory();
      historyStatus = "loading";
      publish();
      await refreshHistory();
    },
    loadMoreHistory: async () => {
      if (history.nextCursor) await refreshHistory(history.nextCursor);
    },
      catchUp: requestCatchUp,
      refreshPlayback,
      acknowledgePlaybackAttempt,
      preparePlaybackItem,
      authorizePlayback,
      setPlaybackSettings,
    stop() {
      stopped = true;
      lifecycle += 1;
      scopeEpoch += 1;
      scopeWrite = undefined;
      clearCatchUp();
      stateRead += 1;
      joinedStateRead += 1;
      historyRead += 1;
      stateController?.abort();
      historyController?.abort();
      stopStream();
    },
  };
};
