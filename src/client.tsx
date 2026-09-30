import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";

function App() {
  const [message, setMessage] = useState("Loading...");

  useEffect(() => {
    fetch("/api/hello")
      .then((response) => {
        if (!response.ok) throw new Error("API request failed");
        return response.json() as Promise<{ message: string }>;
      })
      .then((data) => setMessage(data.message))
      .catch(() => setMessage("Could not reach the API."));
  }, []);

  return (
    <main>
      <h1>Speak Now</h1>
      <p>{message}</p>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
