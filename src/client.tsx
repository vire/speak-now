import React, { useEffect, useState, useSyncExternalStore } from "react";
import { createRoot } from "react-dom/client";
import { createPrototypePlaybackOwner, type PrototypePlaybackOwner } from "./prototype-playback";
import { createBrowserApi } from "./ui/api";
import { createBrowserSession } from "./ui/session";
import { HierarchyView } from "./ui/tree";
import "./ui/tree.css";

function App() {
  const [message, setMessage] = useState("Loading...");
  const [audioEnabled, setAudioEnabled] = useState(false);
  const [audioError, setAudioError] = useState<string>();
  const playbackOwner = React.useRef<PrototypePlaybackOwner | undefined>(undefined);
  const browserSession = React.useMemo(() => createBrowserSession(createBrowserApi()), []);
  const browserSnapshot = useSyncExternalStore(browserSession.subscribe, browserSession.snapshot, browserSession.snapshot);
  const demoSuspended = !browserSnapshot.ready || browserSnapshot.scopeStatus === "pending" || browserSnapshot.scopeStatus === "unresolved" || browserSnapshot.scopeStatus === "failed" || Boolean(browserSnapshot.confirmedScope);

  useEffect(() => {
    void browserSession.start();
    return () => browserSession.stop();
  }, [browserSession]);

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

  return (
    <main>
      <h1>Speak Now</h1>
      <p>{message}</p>
      {browserSnapshot.transportError && <p role="alert">Could not refresh source state: {browserSnapshot.transportError} <button type="button" onClick={() => { void browserSession.start(browserSnapshot.selectedSourceId); }}>Retry source state</button></p>}
      <HierarchyView snapshot={browserSnapshot} onBrowse={(sourceId) => { void browserSession.selectSource(sourceId); }} onRetry={() => { void browserSession.start(browserSnapshot.selectedSourceId); }} onJoinWorkspace={(sourceId, workspaceId) => { void browserSession.joinWorkspace(sourceId, workspaceId); }} onJoinTab={(sourceId, tabId) => { void browserSession.joinTab(sourceId, tabId); }} onLeave={() => { void browserSession.leave(); }} onHistoryFilters={(filters) => { void browserSession.setHistoryFilters(filters); }} onLoadMore={() => { void browserSession.loadMoreHistory(); }} />
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
      <section className="unavailable-actions" aria-label="Unavailable controls"><h2>Future controls</h2><button type="button" disabled aria-describedby="catch-up-note">Catch up</button><p id="catch-up-note">Catch up is unavailable pending SN-07.</p><button type="button" disabled aria-describedby="audio-controls-note">Mute, volume, speed, and playback history</button><p id="audio-controls-note">These controls are unavailable pending SN-08.</p></section>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
