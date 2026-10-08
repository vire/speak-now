import type { ListeningScopeResponse } from "./api";

export type PlaybackCandidate = {
  id: string;
  attemptId?: string;
  authorizationGeneration?: number;
  kind: "announcement" | "recap";
  audioUrl: string;
  sourceId: string;
  workspaceId?: string;
  tabId?: string;
  participantId: string;
  originGeneration: number;
};

export type PlaybackSettings = {
  master: { muted: boolean; volume: number; rate: number };
  participants: Record<string, { muted: boolean; volume: number }>;
};

export type PlaybackOutcome = "started" | "heard" | "stopped" | "skipped" | "blocked" | "failed";
export type PlaybackEvent = { item: PlaybackCandidate; outcome: PlaybackOutcome; replay: boolean };
export type PlaybackAudio = Pick<HTMLAudioElement, "onended" | "onerror" | "pause" | "play" | "playbackRate" | "volume">;

export type ScopedPlaybackOwner = {
  sync(input: { scope: ListeningScopeResponse | null; scopeStatus: "idle" | "pending" | "confirmed" | "failed" | "unresolved"; participants: readonly string[]; settings: PlaybackSettings }): void;
  play(candidate: PlaybackCandidate, options?: { replay?: boolean }): Promise<void>;
  enqueue(candidate: PlaybackCandidate): Promise<void>;
  retryPendingAcknowledgements(): Promise<void>;
  dispose(): void;
};

type ActivePlayback = { item: PlaybackCandidate; audio: PlaybackAudio; replay: boolean; authorizationGeneration: number; reportedStart: boolean; ended: boolean; completionSent: boolean };
type PendingReport = { event: PlaybackEvent; onAccepted?: () => void };

const matchesScope = (candidate: PlaybackCandidate, scope: ListeningScopeResponse | null) => {
  if (!scope || candidate.sourceId !== scope.sourceId) return false;
  if (scope.workspaceId) return candidate.workspaceId === scope.workspaceId;
  return candidate.tabId === scope.tabId;
};

const muted = (candidate: PlaybackCandidate, settings: PlaybackSettings) => settings.master.muted || settings.participants[candidate.participantId]?.muted === true;
const key = (candidate: PlaybackCandidate) => candidate.attemptId ?? `${candidate.kind}:${candidate.id}:${candidate.participantId}:${candidate.originGeneration}`;

export const createScopedPlaybackOwner = ({ createAudio, report }: { createAudio(url: string): PlaybackAudio; report(event: PlaybackEvent): void | Promise<void> }) => {
  let disposed = false;
  let scope: ListeningScopeResponse | null = null;
  let scopeStatus: "idle" | "pending" | "confirmed" | "failed" | "unresolved" = "idle";
  let participants = new Set<string>();
  let settings: PlaybackSettings = { master: { muted: false, volume: 1, rate: 1 }, participants: {} };
  let active: ActivePlayback | undefined;
  const automaticallyCompleted = new Set<string>();
  const queued: PlaybackCandidate[] = [];
  let drainingQueue = false;
  const pendingReports = new Map<string, PendingReport>();
  const reportWrites = new Set<Promise<void>>();
  const reportKey = (event: PlaybackEvent) => `${key(event.item)}:${event.outcome}`;

  const deliver = (event: PlaybackEvent, onAccepted?: () => void) => {
    let result: void | Promise<void>;
    try { result = report(event); } catch { result = Promise.reject(); }
    const write = Promise.resolve(result).then(() => {
      pendingReports.delete(reportKey(event));
      onAccepted?.();
    }).catch(() => { pendingReports.set(reportKey(event), { event, onAccepted }); });
    reportWrites.add(write);
    void write.finally(() => reportWrites.delete(write));
    return write;
  };

  const send = (item: PlaybackCandidate, outcome: PlaybackOutcome, replay: boolean, onAccepted?: () => void) => deliver({ item, outcome, replay }, onAccepted);

  let drainQueue: () => Promise<void>;
  const completeEnded = (current: ActivePlayback) => {
    if (!current.ended || !current.reportedStart || current.completionSent) return;
    current.completionSent = true;
    send(current.item, "heard", current.replay, () => { automaticallyCompleted.add(key(current.item)); void drainQueue(); });
  };

  const stopActive = () => {
    const current = active;
    if (!current) return;
    active = undefined;
    if (current.ended) return;
    current.audio.onended = null;
    current.audio.onerror = null;
    current.audio.pause();
    send(current.item, "stopped", current.replay, () => { void drainQueue(); });
  };

  const authorized = (candidate: PlaybackCandidate, replay: boolean) => !disposed && scopeStatus === "confirmed" && participants.has(candidate.participantId) && matchesScope(candidate, scope) && (candidate.authorizationGeneration === undefined || candidate.authorizationGeneration === scope?.generation) && (replay || candidate.originGeneration === scope?.generation);
  const eligible = (candidate: PlaybackCandidate, replay: boolean) => authorized(candidate, replay) && !muted(candidate, settings);

  const play = async (item: PlaybackCandidate, options: { replay?: boolean } = {}) => {
    const replay = options.replay === true;
    if (!authorized(item, replay)) {
      send(item, "blocked", replay, () => { void drainQueue(); });
      return;
    }
    if (muted(item, settings) || automaticallyCompleted.has(key(item)) || [...pendingReports.values()].some((entry) => key(entry.event.item) === key(item))) {
      send(item, "skipped", replay, () => { void drainQueue(); });
      return;
    }
    stopActive();
    const audio = createAudio(item.audioUrl);
    const current: ActivePlayback = { item, audio, replay, authorizationGeneration: item.authorizationGeneration ?? scope!.generation, reportedStart: false, ended: false, completionSent: false };
    active = current;
    const participant = settings.participants[item.participantId];
    audio.volume = settings.master.volume * (participant?.volume ?? 1);
    audio.playbackRate = settings.master.rate;
    audio.onended = () => {
      if (disposed || active !== current || !eligible(item, replay)) return;
      current.ended = true;
      if (!current.reportedStart) return;
      active = undefined;
      completeEnded(current);
    };
    audio.onerror = () => {
      if (disposed || active !== current) return;
      active = undefined;
      send(item, "failed", replay, () => { void drainQueue(); });
    };
    try {
      await audio.play();
      if (disposed || !eligible(item, replay) || (active !== current && !current.ended)) {
        audio.pause();
        return;
      }
      send(item, "started", replay, () => {
        if (disposed || !eligible(item, replay)) return;
        current.reportedStart = true;
        if (current.ended) {
          if (active === current) active = undefined;
          completeEnded(current);
        }
      });
    } catch (error) {
      if (disposed || active !== current) return;
      active = undefined;
      const blocked = error instanceof Error && error.name === "NotAllowedError";
      send(item, blocked ? "blocked" : "failed", replay, () => { void drainQueue(); });
    }
  };

  drainQueue = async () => {
    if (drainingQueue || active || disposed) return;
    drainingQueue = true;
    try {
      while (!active && queued.length) await play(queued.shift()!);
    } finally {
      drainingQueue = false;
    }
  };

  return {
    sync(input: { scope: ListeningScopeResponse | null; scopeStatus: "idle" | "pending" | "confirmed" | "failed" | "unresolved"; participants: readonly string[]; settings: PlaybackSettings }) {
      scope = input.scope;
      scopeStatus = input.scopeStatus;
      participants = new Set(input.participants);
      settings = input.settings;
      if (active && (!eligible(active.item, active.replay) || active.authorizationGeneration !== scope?.generation)) stopActive();
      if (active) {
        const participant = settings.participants[active.item.participantId];
        active.audio.volume = settings.master.volume * (participant?.volume ?? 1);
        active.audio.playbackRate = settings.master.rate;
      }
    },
    play,
    async enqueue(item: PlaybackCandidate) {
      queued.push(item);
      await drainQueue();
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      queued.length = 0;
      stopActive();
    },
    async retryPendingAcknowledgements() {
      await Promise.all(reportWrites);
      const pending = [...pendingReports.values()].sort((left, right) => Number(right.event.outcome === "started") - Number(left.event.outcome === "started"));
      for (const { event, onAccepted } of pending) await deliver(event, onAccepted);
    },
  } satisfies ScopedPlaybackOwner;
};
