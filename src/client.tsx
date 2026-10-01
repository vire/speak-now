import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { createPrototypePlaybackOwner, type PrototypePlaybackOwner } from "./prototype-playback";

function App() {
  const [message, setMessage] = useState("Loading...");
  const [audioEnabled, setAudioEnabled] = useState(false);
  const [audioError, setAudioError] = useState<string>();
  const playbackOwner = React.useRef<PrototypePlaybackOwner | undefined>(undefined);

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
    if (!audioEnabled) return;
    const owner = createPrototypePlaybackOwner({ onError: setAudioError });
    playbackOwner.current = owner;
    owner.check();
    const interval = window.setInterval(() => owner.check(), 3_000);
    return () => {
      window.clearInterval(interval);
      owner.dispose();
      if (playbackOwner.current === owner) playbackOwner.current = undefined;
    };
  }, [audioEnabled]);

  return (
    <main>
      <h1>Speak Now</h1>
      <p>{message}</p>
      <button
        type="button"
        onClick={() => {
          if (audioError) playbackOwner.current?.check();
          else setAudioEnabled(true);
        }}
        disabled={audioEnabled && !audioError}
      >
        {audioError ? "Retry audio" : audioEnabled ? "Audio enabled" : "Enable audio"}
      </button>
      {audioError && <p role="alert">{audioError}</p>}
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
