import { appLogger } from "./logging";
import { appTracer } from "./tracing";

const port = Number(Bun.env.PORT ?? 3000);
const dataDir = Bun.env.SPEAK_NOW_DATA_DIR ?? "data";
const logger = appLogger({ dataDirectory: dataDir });
const tracer = appTracer({ dataDirectory: dataDir });
const traceContext = (request: Request) => {
  const match = (request.headers.get("traceparent") ?? new URL(request.url).searchParams.get("traceparent") ?? "").match(/^00-([a-f0-9]{32})-([a-f0-9]{16})-[a-f0-9]{2}$/i);
  return match ? { traceId: match[1], spanId: match[2] } : undefined;
};

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
      traces: tracer.getStatus(),
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
        const span = tracer.start("media.read.latest");
        return (async () => { try { const exists = await file.exists(); if (!exists) { await span.end("missing"); return new Response("No announcement", { status: 404 }); } const announcement = await file.json() as { announcementId?: string }; await span.end("succeeded", { announcementId: announcement.announcementId }); return Response.json(announcement, { headers: { "Cache-Control": "no-store" } }); } catch { await span.end("failed"); return new Response("No announcement", { status: 500 }); } })();
    }
    const match = path.match(/^\/api\/prototype\/audio\/([a-f0-9]{64})$/);
      if (match) {
        const file = Bun.file(`${dataDir}/audio/${match[1]}.mp3`);
        const span = tracer.start("media.read", { ...traceContext(request), clipId: match[1] });
        return file.exists().then(async (exists) => { try { const audio = exists ? await file.arrayBuffer() : undefined; await span.end(exists ? "succeeded" : "missing"); void logger.log(exists ? "info" : "warn", { operation: "server.media", message: exists ? "Served audio clip" : "Audio clip unavailable", outcome: exists ? "succeeded" : "missing", metadata: { clip: match[1] } }); return exists ? new Response(audio, { headers: { "Content-Type": "audio/mpeg", "Cache-Control": "no-store" } }) : new Response("Audio unavailable", { status: 404 }); } catch (error) { await span.end("failed"); return new Response("Audio unavailable", { status: 500 }); } });
    }
    return new Response("Not found", { status: 404 });
  },
});

console.log(`Speak Now is running at http://localhost:${port}`);
