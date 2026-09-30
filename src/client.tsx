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

  async function play() {
    if (loading) return;
    player.current?.pause();
    setLoading(true);
    setError(undefined);
    try {
      const response = await fetch(`/api/demo/speech?room=${encodeURIComponent(roomId)}`, { method: "POST" });
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
      <h1>Speak Now</h1>
      <nav className="rooms" aria-label="Rooms">
        {rooms.map((candidate) => (
          <button type="button" key={candidate.id} aria-pressed={candidate.id === roomId}
            disabled={loading} onClick={() => {
              player.current?.pause();
              setRoomId(candidate.id);
              setMessage(undefined);
              setAudioUrl(undefined);
              setError(undefined);
            }}>{candidate.name}</button>
        ))}
      </nav>
      <section className="room">
        <h2>{room.name}</h2>
        <ul>{room.messages.map((text) => <li key={text}>{text}</li>)}</ul>
      </section>
      <button type="button" aria-label={`Play a random ${room.name} message`} onClick={() => void play()} disabled={loading}>
        {loading ? "Generating…" : "▶ Play"}
      </button>
      {message && <p>{message}</p>}
      {audioUrl && <audio key={audioUrl} ref={player} src={audioUrl} controls autoPlay
        aria-label={`Generated ${room.name} update`} onError={() => setError("Could not play the audio. Please try again.")} />}
      <p className="status" role="status">{loading ? "Generating a random sample update" : ""}</p>
      {error && <p role="alert">{error}</p>}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
