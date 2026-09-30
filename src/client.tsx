import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { rooms } from "./demo";

function App() {
  const [roomId, setRoomId] = useState(rooms[0]!.id);
  const room = rooms.find((candidate) => candidate.id === roomId)!;
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<string>();
  const [audioUrl, setAudioUrl] = useState<string>();
  const [error, setError] = useState<string>();
  const player = useRef<HTMLAudioElement | null>(null);

  useEffect(() => () => { if (audioUrl) URL.revokeObjectURL(audioUrl); }, [audioUrl]);

  async function play(nextRoomId: string) {
    if (loading) return;
    player.current?.pause();
    setRoomId(nextRoomId);
    setMessage(undefined);
    setAudioUrl(undefined);
    setLoading(true);
    setError(undefined);
    try {
      const response = await fetch(`/api/demo/speech?room=${encodeURIComponent(nextRoomId)}`, { method: "POST" });
      if (!response.ok) {
        const data = await response.json() as { error?: string };
        throw new Error(data.error ?? "Could not generate speech. Please try again.");
      }
      const blob = await response.blob();
      setMessage(response.headers.get("X-Demo-Message") ?? "Sample agent update");
      setAudioUrl(URL.createObjectURL(blob));
    } catch (error) {
      setError(error instanceof Error ? error.message : "Could not generate speech. Please try again.");
    } finally { setLoading(false); }
  }

  return (
    <main>
      <header className="toolbar">
        <div className="brand"><span className="brand-icon" aria-hidden="true">◖◗</span><h1>Speak Now</h1><span className="demo-badge">DEMO</span></div>
        <span className="server-status"><span className="dot" /> Voice channels</span>
      </header>
      <div className="server-heading">
        <div><span className="eyebrow">YOUR WORKSPACE</span><h2>Agent lounge</h2></div>
        <span className="room-count">4 channels / 12 sample updates</span>
      </div>
      <div className="channels" aria-label="Voice channels">
        {rooms.map((candidate, index) => (
          <section className={`channel ${candidate.id === roomId ? "selected" : ""}`} key={candidate.id}
            aria-labelledby={`room-${candidate.id}`}>
            <header className="channel-heading">
              <span className="channel-icon" aria-hidden="true">◉</span>
              <div><span className="channel-number">CHANNEL 0{index + 1}</span><h3 id={`room-${candidate.id}`}>{candidate.name}</h3></div>
              <span className="message-count">3</span>
            </header>
            <ol className="updates">
              {candidate.messages.map((text, messageIndex) => (
                <li key={text} className={candidate.id === roomId && message === text ? "active-update" : ""}>
                  <div className="update-meta"><span className="avatar" aria-hidden="true">A{messageIndex + 1}</span><span>Agent 0{messageIndex + 1}</span><span className="sample-label">sample</span></div>
                  <p>{text}</p>
                </li>
              ))}
            </ol>
            <button className="play-button" type="button" aria-label={`Play a random ${candidate.name} message`}
              onClick={() => void play(candidate.id)} disabled={loading}>
              <span aria-hidden="true">▶</span> {loading && candidate.id === roomId ? "Generating…" : "Play channel"}
            </button>
          </section>
        ))}
      </div>
      <footer className="playback">
        <div className="playback-info"><span className={`dot ${loading ? "busy" : ""}`} /><div>
          <strong>{loading ? "Generating voice…" : audioUrl ? room.name : "Ready to listen"}</strong>
          <p role="status">{message ?? (loading ? `Preparing an update from ${room.name}` : "Choose a channel to hear a sample update")}</p>
        </div></div>
        {audioUrl && <audio key={audioUrl} ref={player} src={audioUrl} controls autoPlay
          aria-label={`Generated ${room.name} update`} onError={() => setError("Could not play the audio. Please try again.")} />}
        {error && <p className="error" role="alert">{error}</p>}
      </footer>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
