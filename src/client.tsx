import React, { useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { rooms } from "./demo";

function App() {
  const [roomId, setRoomId] = useState(rooms[0]!.id);
  const room = rooms.find((candidate) => candidate.id === roomId)!;
  const [playbackId, setPlaybackId] = useState(0);
  const [message, setMessage] = useState<string>();
  const [audioUrl, setAudioUrl] = useState<string>();
  const [error, setError] = useState<string>();
  const player = useRef<HTMLAudioElement | null>(null);

  function play(nextRoomId: string) {
    const nextRoom = rooms.find((candidate) => candidate.id === nextRoomId)!;
    const index = Math.floor(Math.random() * nextRoom.messages.length);
    player.current?.pause();
    setRoomId(nextRoomId);
    setMessage(nextRoom.messages[index]);
    setAudioUrl(`/audio/${nextRoom.id}-${index + 1}.mp3`);
    setPlaybackId((id) => id + 1);
    setError(undefined);
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
              onClick={() => play(candidate.id)}>
              <span aria-hidden="true">▶</span> Play channel
            </button>
          </section>
        ))}
      </div>
      <footer className="playback">
        <div className="playback-info"><span className="dot" /><div>
          <strong>{audioUrl ? room.name : "Ready to listen"}</strong>
          <p role="status">{message ?? "Choose a channel to hear a sample update"}</p>
        </div></div>
        {audioUrl && <audio key={playbackId} ref={player} src={audioUrl} controls autoPlay
          aria-label={`${room.name} sample update`} onError={() => setError("Could not play the audio. Please try again.")} />}
        {error && <p className="error" role="alert">{error}</p>}
      </footer>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
