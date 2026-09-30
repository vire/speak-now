import React, { useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { messages } from "./demo";

function App() {
  const [playing, setPlaying] = useState<string>();
  const [error, setError] = useState<string>();
  const current = useRef<HTMLAudioElement | null>(null);

  useEffect(() => () => { current.current?.pause(); }, []);

  async function play(message: typeof messages[number]) {
    current.current?.pause();
    const audio = new Audio(message.audio);
    current.current = audio;
    setError(undefined);
    setPlaying(message.id);
    const failed = () => {
      if (current.current !== audio) return;
      setPlaying(undefined);
      setError("Could not play the audio. Please try again.");
    };
    audio.onended = () => {
      if (current.current === audio) setPlaying(undefined);
    };
    audio.onerror = failed;
    try { await audio.play(); } catch { failed(); }
  }

  return (
    <main>
      <h1>Speak Now</h1>
      <p><em>Coming soon</em></p>
      <p>Hear a sample agent update.</p>
      <div className="messages">
        {messages.map((message, index) => (
          <section key={message.id}>
            <p>{message.text}</p>
            <button type="button" onClick={() => void play(message)} disabled={playing === message.id}
              aria-label={`Play sample ${index + 1}`}>
              {playing === message.id ? "Playing…" : "▶ Play"}
            </button>
          </section>
        ))}
      </div>
      <p className="status" role="status">{playing ? "Playing sample update" : "Hardcoded demo messages"}</p>
      {error && <p role="alert">{error}</p>}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
