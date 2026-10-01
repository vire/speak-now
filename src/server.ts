import { appLogger } from "./logging";

const port = Number(Bun.env.PORT ?? 3000);
const dataDir = Bun.env.SPEAK_NOW_DATA_DIR ?? "data";
const logger = appLogger({ dataDirectory: dataDir });

Bun.serve({
  port,
  routes: {
    "/api/health": () => Response.json({ status: "ok" }),
    "/api/hello": () => Response.json({ message: "Hello from Speak Now!" }),
    "/api/config": () => Response.json({
      collector: "bun src/collector.ts",
      liveSpeechConfigured: Boolean(Bun.env.ELEVENLABS_API_KEY && Bun.env.ELEVENLABS_VOICE_ID),
      dataDir,
      diagnostics: logger.getStatus(),
    }),
    "/assets/client.js": () => new Response(Bun.file("public/assets/client.js"), {
      headers: { "Content-Type": "text/javascript; charset=utf-8" },
    }),
    "/": () => new Response(Bun.file("public/index.html"), {
      headers: { "Content-Type": "text/html; charset=utf-8" },
    }),
  },
  fetch: (request) => {
    const path = new URL(request.url).pathname;
    if (path === "/api/prototype/latest") {
      const file = Bun.file(`${dataDir}/latest-announcement.json`);
      return file.exists().then((exists) => { void logger.log(exists ? "info" : "warn", { operation: "server.latest", message: exists ? "Served latest announcement" : "Latest announcement unavailable", outcome: exists ? "succeeded" : "missing" }); return exists ? new Response(file, { headers: { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" } }) : new Response("No announcement", { status: 404 }); });
    }
    const match = path.match(/^\/api\/prototype\/audio\/([a-f0-9]{64})$/);
    if (match) {
      const file = Bun.file(`${dataDir}/audio/${match[1]}.mp3`);
      return file.exists().then((exists) => { void logger.log(exists ? "info" : "warn", { operation: "server.media", message: exists ? "Served audio clip" : "Audio clip unavailable", outcome: exists ? "succeeded" : "missing", metadata: { clip: match[1] } }); return exists ? new Response(file, { headers: { "Content-Type": "audio/mpeg", "Cache-Control": "no-store" } }) : new Response("Audio unavailable", { status: 404 }); });
    }
    return new Response("Not found", { status: 404 });
  },
});

console.log(`Speak Now is running at http://localhost:${port}`);
