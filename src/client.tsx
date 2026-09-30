import React from "react";
import { createRoot } from "react-dom/client";

function App() {
  return (
    <main>
      <h1>Speak Now</h1>
      <p><em>Coming soon</em></p>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(<App />);
