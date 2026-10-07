import type { BrowserApi, HistoryFilters, HistoryPage, ListeningSelection, StateResponse } from "./api";

type CachedState = { state: StateResponse; stale: boolean };
type ScopeStatus = "idle" | "pending" | "confirmed" | "failed" | "unresolved";

export type BrowserSessionSnapshot = {
  ready: boolean;
  selectedSourceId?: string;
  stateBySource: Record<string, CachedState>;
  confirmedScope: StateResponse["scope"];
  scopeStatus: ScopeStatus;
  history: HistoryPage;
  historyStatus: "loading" | "ready" | "error";
  historyFilters: Omit<HistoryFilters, "cursor">;
  transportError?: string;
};

const emptyHistory = (): HistoryPage => ({ announcements: [], results: [] });
const withoutCursor = ({ cursor: _cursor, ...filters }: HistoryFilters): Omit<HistoryFilters, "cursor"> => filters;

export const createBrowserSession = (api: BrowserApi) => {
  let stopped = false;
  let selectedSourceId: string | undefined;
  let stateBySource: Record<string, CachedState> = {};
  let confirmedScope: StateResponse["scope"] = null;
  let scopeStatus: ScopeStatus = "idle";
  let historyFilters: Omit<HistoryFilters, "cursor"> = { order: "desc", limit: 50 };
  let history = emptyHistory();
  let historyStatus: "loading" | "ready" | "error" = "loading";
  let transportError: string | undefined;
  let ready = false;
  let stateRead = 0;
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

  const view = (): BrowserSessionSnapshot => ({ ready, selectedSourceId, stateBySource, confirmedScope, scopeStatus, history, historyStatus, historyFilters, ...(transportError ? { transportError } : {}) });
  let current = view();
  const publish = () => {
    current = view();
    for (const listener of listeners) listener();
  };
  const snapshot = () => current;

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
      if (readScopeEpoch === scopeEpoch) confirmedScope = state.scope;
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

  const resnapshot = async () => {
    markAllStale();
    const [stateRead] = await Promise.all([refreshState(), refreshHistory()]);
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
    scopeStatus = "pending";
    publish();
    const write = (async () => {
      try {
        const response = await api.setListening(selection);
        if (!stopped && writeLifecycle === lifecycle && writeScopeEpoch === scopeEpoch) {
          confirmedScope = response.scope;
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
      if (scopeWrite === write) scopeWrite = undefined;
    }
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
    stop() {
      stopped = true;
      lifecycle += 1;
      scopeEpoch += 1;
      scopeWrite = undefined;
      stateRead += 1;
      historyRead += 1;
      stateController?.abort();
      historyController?.abort();
      stopStream();
    },
  };
};
