import React, { useEffect, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { createPrototypePlaybackOwner, type PrototypePlaybackOwner } from "./prototype-playback";
import { createBrowserApi } from "./ui/api";
import { CatchUpView } from "./ui/catch-up";
import { PlaybackPanel } from "./ui/playback-panel";
import { createScopedPlaybackOwner, type PlaybackCandidate as OwnerPlaybackCandidate, type PlaybackSettings as OwnerPlaybackSettings } from "./ui/playback";
import { createBrowserSession } from "./ui/session";
import { HierarchyView } from "./ui/tree";
import "./ui/tree.css";

const ownerSettings = (settings: { master: { muted: boolean; volume: number; speed: number }; participants: Array<{ sourceId: string; participantId: string; muted: boolean; volume: number }> }, sourceId: string, participantIds: readonly string[]): OwnerPlaybackSettings => ({
  master: { muted: settings.master.muted, volume: settings.master.volume, rate: settings.master.speed },
  participants: Object.fromEntries(participantIds.map((participantId) => {
    const participant = settings.participants.find((item) => item.sourceId === sourceId && item.participantId === participantId);
    return [participantId, { muted: participant?.muted ?? false, volume: participant?.volume ?? 1 }];
  })),
});

function App() {
  const [message, setMessage] = useState("Loading...");
  const [audioEnabled, setAudioEnabled] = useState(false);
  const [audioError, setAudioError] = useState<string>();
  const playbackOwner = React.useRef<PrototypePlaybackOwner | undefined>(undefined);
  const browserApi = React.useMemo(() => createBrowserApi(), []);
  const browserSession = React.useMemo(() => createBrowserSession(browserApi), [browserApi]);
  const browserSnapshot = useSyncExternalStore(browserSession.subscribe, browserSession.snapshot, browserSession.snapshot);
  const scopedPlaybackOwner = React.useMemo(() => createScopedPlaybackOwner({
    createAudio: (url) => new Audio(url),
    report: (event) => {
      if (!event.item.attemptId || event.item.authorizationGeneration === undefined) return Promise.reject(new Error("Playback acknowledgement is missing its attempt receipt"));
      return browserApi.updatePlaybackAttempt(event.item.attemptId, { state: event.outcome, authorizationGeneration: event.item.authorizationGeneration }).then(() => undefined);
    },
  }), [browserApi]);
  const preparedItems = React.useRef(new Set<string>());
  const automaticAttempts = React.useRef(new Map<string, string>());
  const automaticWrites = React.useRef(new Set<string>());
  const [attemptRetry, setAttemptRetry] = useState(0);
  const demoSuspended = !browserSnapshot.ready || browserSnapshot.scopeStatus === "pending" || browserSnapshot.scopeStatus === "unresolved" || browserSnapshot.scopeStatus === "failed" || Boolean(browserSnapshot.confirmedScope);
  const joinedScopeKey = browserSnapshot.confirmedScope && browserSnapshot.scopeStatus === "confirmed" ? `${browserSnapshot.confirmedScope.sourceId}:${browserSnapshot.confirmedScope.workspaceId ?? browserSnapshot.confirmedScope.tabId}:${browserSnapshot.confirmedScope.generation}` : undefined;
  const playbackSettings = browserSnapshot.playback && browserSnapshot.playbackAuthorization ? ownerSettings(browserSnapshot.playback.settings, browserSnapshot.playback.scope.sourceId, browserSnapshot.playbackAuthorization.participantIds) : { master: { muted: false, volume: 1, rate: 1 }, participants: {} };

  useEffect(() => {
    void browserSession.start();
    return () => browserSession.stop();
  }, [browserSession]);

  useEffect(() => {
    if (!joinedScopeKey) return;
    void browserSession.refreshPlayback().catch(() => undefined);
  }, [browserSession, joinedScopeKey]);

  useEffect(() => {
    scopedPlaybackOwner.sync({
      scope: browserSnapshot.playbackAuthorization?.scope ?? null,
      scopeStatus: browserSnapshot.scopeStatus,
      participants: browserSnapshot.playbackAuthorization?.participantIds ?? [],
      settings: playbackSettings,
    });
  }, [browserSnapshot.playbackAuthorization, browserSnapshot.scopeStatus, playbackSettings, scopedPlaybackOwner]);

  useEffect(() => {
    const playback = browserSnapshot.playback;
    if (!playback || !joinedScopeKey) return;
    for (const candidate of playback.candidates) {
      if ((candidate.media.state !== "pending" && candidate.media.state !== "preparing") || preparedItems.current.has(candidate.itemId)) continue;
      preparedItems.current.add(candidate.itemId);
      void browserSession.preparePlaybackItem(candidate.itemId).then(() => preparedItems.current.delete(candidate.itemId), () => preparedItems.current.delete(candidate.itemId));
    }
    if (playback.candidates.some((candidate) => candidate.media.state === "pending" || candidate.media.state === "preparing")) {
      const refresh = window.setTimeout(() => { void browserSession.refreshPlayback().catch(() => undefined); }, 1_000);
      return () => window.clearTimeout(refresh);
    }
  }, [browserSession, browserSnapshot.playback, joinedScopeKey]);

  useEffect(() => {
    const playback = browserSnapshot.playback;
    if (!playback || !joinedScopeKey) return;
    for (const candidate of playback.candidates) {
      const key = `${candidate.itemId}:${playback.scope.generation}`;
      if (candidate.media.state !== "ready" || candidate.originGeneration !== playback.scope.generation || candidate.playback?.status !== "unattempted" || automaticWrites.current.has(key)) continue;
      const attemptId = automaticAttempts.current.get(key) ?? crypto.randomUUID();
      automaticAttempts.current.set(key, attemptId);
      automaticWrites.current.add(key);
      void browserSession.authorizePlayback(candidate.itemId, "automatic", attemptId).then((receipt) => {
        if (!receipt.mediaUrl || receipt.state !== "prepared") throw new Error("Playback attempt was not prepared");
        const item: OwnerPlaybackCandidate = { id: candidate.itemId, attemptId: receipt.attemptId, authorizationGeneration: receipt.authorizationGeneration, kind: candidate.kind, audioUrl: receipt.mediaUrl, sourceId: candidate.sourceId, ...(candidate.workspaceId ? { workspaceId: candidate.workspaceId } : {}), ...(candidate.tabId ? { tabId: candidate.tabId } : {}), participantId: candidate.participantId, originGeneration: receipt.originGeneration ?? candidate.originGeneration };
        return scopedPlaybackOwner.enqueue(item);
      }).catch(() => {
        if (automaticAttempts.current.get(key) === attemptId) window.setTimeout(() => setAttemptRetry((current) => current + 1), 1_000);
      }).finally(() => {
        automaticWrites.current.delete(key);
      });
    }
  }, [attemptRetry, browserSession, browserSnapshot.playback, joinedScopeKey, scopedPlaybackOwner]);

  useEffect(() => {
    const retry = window.setInterval(() => { void scopedPlaybackOwner.retryPendingAcknowledgements(); }, 5_000);
    return () => window.clearInterval(retry);
  }, [scopedPlaybackOwner]);

  useEffect(() => () => scopedPlaybackOwner.dispose(), [scopedPlaybackOwner]);

  useEffect(() => {
    fetch("/api/hello")
      .then((response) => {
        if (!response.ok) throw new Error("API request failed");
        return response.json() as Promise<{ message: string }>;
      })
      .then((data) => setMessage(data.message))
      .catch(() => setMessage("Could not reach the API."));
  }, []);

  useEffect(() => {
    if (!audioEnabled || demoSuspended) return;
    const owner = createPrototypePlaybackOwner({ onError: setAudioError });
    playbackOwner.current = owner;
    owner.check();
    const interval = window.setInterval(() => owner.check(), 3_000);
    return () => {
      window.clearInterval(interval);
      owner.dispose();
      if (playbackOwner.current === owner) playbackOwner.current = undefined;
    };
  }, [audioEnabled, demoSuspended]);

  const replay = (itemId: string) => {
    const candidate = browserSnapshot.playback?.candidates.find((entry) => entry.itemId === itemId);
    if (!candidate || candidate.media.state !== "ready") return;
    const attemptId = crypto.randomUUID();
    void browserSession.authorizePlayback(candidate.itemId, "replay", attemptId).then((receipt) => {
      if (!receipt.mediaUrl || receipt.state !== "prepared") throw new Error("Playback attempt was not prepared");
      return scopedPlaybackOwner.play({ id: candidate.itemId, attemptId: receipt.attemptId, authorizationGeneration: receipt.authorizationGeneration, kind: candidate.kind, audioUrl: receipt.mediaUrl, sourceId: candidate.sourceId, ...(candidate.workspaceId ? { workspaceId: candidate.workspaceId } : {}), ...(candidate.tabId ? { tabId: candidate.tabId } : {}), participantId: candidate.participantId, originGeneration: receipt.originGeneration ?? candidate.originGeneration }, { replay: true });
    }).catch(() => undefined);
  };

  const updatePlaybackSettings = (next: OwnerPlaybackSettings) => {
    const playback = browserSnapshot.playback;
    if (!playback) return;
    scopedPlaybackOwner.sync({ scope: browserSnapshot.playbackAuthorization?.scope ?? null, scopeStatus: browserSnapshot.scopeStatus, participants: browserSnapshot.playbackAuthorization?.participantIds ?? [], settings: next });
    if (next.master.muted !== playbackSettings.master.muted || next.master.volume !== playbackSettings.master.volume || next.master.rate !== playbackSettings.master.rate) {
      void browserSession.setPlaybackSettings({ master: { muted: next.master.muted, volume: next.master.volume, speed: next.master.rate } }).catch(() => { void browserSession.refreshPlayback().catch(() => undefined); });
      return;
    }
    const participantId = Object.keys(next.participants).find((id) => next.participants[id]?.muted !== playbackSettings.participants[id]?.muted || next.participants[id]?.volume !== playbackSettings.participants[id]?.volume);
    if (participantId) {
      const participant = next.participants[participantId]!;
      void browserSession.setPlaybackSettings({ participant: { sourceId: playback.scope.sourceId, participantId, muted: participant.muted, volume: participant.volume } }).catch(() => { void browserSession.refreshPlayback().catch(() => undefined); });
    }
  };

  return (
    <main>
      <h1>Speak Now</h1>
      <p>{message}</p>
      {browserSnapshot.transportError && <p role="alert">Could not refresh source state: {browserSnapshot.transportError} <button type="button" onClick={() => { void browserSession.start(browserSnapshot.selectedSourceId); }}>Retry source state</button></p>}
        <HierarchyView snapshot={browserSnapshot} onBrowse={(sourceId) => { void browserSession.selectSource(sourceId); }} onRetry={() => { void browserSession.start(browserSnapshot.selectedSourceId); }} onJoinWorkspace={(sourceId, workspaceId) => { void browserSession.joinWorkspace(sourceId, workspaceId); }} onJoinTab={(sourceId, tabId) => { void browserSession.joinTab(sourceId, tabId); }} onLeave={() => { void browserSession.leave(); }} onHistoryFilters={(filters) => { void browserSession.setHistoryFilters(filters); }} onLoadMore={() => { void browserSession.loadMoreHistory(); }} />
        <CatchUpView snapshot={browserSnapshot} onCatchUp={() => { void browserSession.catchUp(); }} />
      {browserSnapshot.playback ? <PlaybackPanel scope={browserSnapshot.playback.scope} scopeStatus={browserSnapshot.scopeStatus} settings={browserSnapshot.playback.settings} participants={browserSnapshot.playbackAuthorization?.participantIds ?? []} candidates={browserSnapshot.playback.candidates} onSettings={updatePlaybackSettings} onReplay={replay} /> : browserSnapshot.confirmedScope && <p role="status">Loading playback controls for the joined session.</p>}
      <section className="prototype-demo" aria-label="Prototype demo"><h2>Prototype demo</h2><p>This separate demo is suspended while listening is joined.</p><button
        type="button"
        onClick={() => {
          if (audioError) playbackOwner.current?.check();
          else setAudioEnabled(true);
        }}
        disabled={demoSuspended || (audioEnabled && !audioError)}
      >
        {audioError ? "Retry audio" : audioEnabled ? "Audio enabled" : "Enable audio"}
      </button>
      {audioError && <p role="alert">{audioError}</p>}</section>
      </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
