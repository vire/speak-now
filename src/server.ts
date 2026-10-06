import { appLogger } from "./logging";
import { appTracer } from "./tracing";
import { createErrorReporter } from "./errors";
import { createServer } from "node:http";

const port = Number(Bun.env.PORT ?? 3000);
const dataDir = Bun.env.SPEAK_NOW_DATA_DIR ?? "data";
const logger = appLogger({ dataDirectory: dataDir });
const tracer = appTracer({ dataDirectory: dataDir });
const errors = createErrorReporter({ dataDirectory: dataDir });
const clientErrorLimit = 8_192;
const traceContext = (value: string | string[] | undefined) => {
  const match = (Array.isArray(value) ? value[0] : value ?? "").match(/^00-([a-f0-9]{32})-([a-f0-9]{16})-[a-f0-9]{2}$/i);
  return match ? { traceId: match[1], spanId: match[2] } : undefined;
};

const json = (response: import("node:http").ServerResponse, status: number, value: unknown) => { response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" }); response.end(JSON.stringify(value)); };
createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`); const path = url.pathname;
  if (path === "/api/health") return json(response, 200, { status: "ok" });
  if (path === "/api/hello") return json(response, 200, { message: "Hello from Speak Now!" });
  if (path === "/api/config") return json(response, 200, {
      collector: "bun src/collector.ts",
      liveSpeechConfigured: Boolean(Bun.env.ELEVENLABS_API_KEY && Bun.env.ELEVENLABS_VOICE_ID),
      dataDir,
      diagnostics: logger.getStatus(),
      traces: tracer.getStatus(),
      errors: errors.getStatus(),
    });
  if (path === "/assets/client.js" || path === "/") {
    const file = Bun.file(path === "/" ? "public/index.html" : "public/assets/client.js");
    response.writeHead(200, { "Content-Type": path === "/" ? "text/html; charset=utf-8" : "text/javascript; charset=utf-8" });
    return response.end(await file.arrayBuffer());
  }
  if (path === "/api/prototype/latest") {
    const span = tracer.start("media.read.latest", traceContext(request.headers.traceparent ?? url.searchParams.get("traceparent") ?? undefined));
    try { const file = Bun.file(`${dataDir}/latest-announcement.json`); if (!(await file.exists())) { await span.end("missing"); response.writeHead(404); return response.end("No announcement"); } const announcement = await file.json() as { announcementId?: string }; await span.end("succeeded", { announcementId: announcement.announcementId }); return json(response, 200, announcement); } catch { await span.end("failed"); response.writeHead(500); return response.end("No announcement"); }
  }
  const audio = path.match(/^\/api\/prototype\/audio\/([a-f0-9]{64})$/);
  if (audio) {
    const span = tracer.start("media.read", { ...traceContext(request.headers.traceparent ?? url.searchParams.get("traceparent") ?? undefined), clipId: audio[1] });
    try { const file = Bun.file(`${dataDir}/audio/${audio[1]}.mp3`); const exists = await file.exists(); if (!exists) { await span.end("missing"); void logger.log("warn", { operation: "server.media", message: "Audio clip unavailable", outcome: "missing", metadata: { clip: audio[1] } }); response.writeHead(404); return response.end("Audio unavailable"); } const bytes = await file.arrayBuffer(); await span.end("succeeded"); void logger.log("info", { operation: "server.media", message: "Served audio clip", outcome: "succeeded", metadata: { clip: audio[1] } }); response.writeHead(200, { "Content-Type": "audio/mpeg", "Cache-Control": "no-store" }); return response.end(bytes); } catch { await span.end("failed"); response.writeHead(500); return response.end("Audio unavailable"); }
  }
  if (path === "/api/client-errors" && request.method === "POST") {
    const origin = `http://${request.headers.host}`;
    if (request.headers.origin !== origin) { response.writeHead(403); return response.end("Origin forbidden"); }
    if (request.headers["content-type"]?.split(";", 1)[0].trim().toLowerCase() !== "application/json") { response.writeHead(415); return response.end("JSON required"); }
    if (Number(request.headers["content-length"]) > clientErrorLimit) { response.writeHead(413); return response.end("Request too large"); }
    const chunks: Buffer[] = []; let bytes = 0; let overflow = false;
    request.on("data", (chunk: Buffer) => { bytes += chunk.length; if (bytes > clientErrorLimit && !overflow) { overflow = true; response.writeHead(413, { Connection: "close" }); response.end("Request too large"); request.resume(); } else if (!overflow) chunks.push(Buffer.from(chunk)); });
    request.on("end", async () => {
      if (overflow) return;
      let body: unknown;
      try { body = JSON.parse(Buffer.concat(chunks).toString("utf8")); } catch { response.writeHead(400); return response.end("Invalid JSON"); }
        if (!body || typeof body !== "object" || Array.isArray(body)) { response.writeHead(400); return response.end("Invalid error report"); }
        const value = body as Record<string, unknown>;
        const string = (key: string, limit: number, required = false) => typeof value[key] === "string" && value[key].length > 0 && value[key].length <= limit ? value[key] : required ? undefined : value[key] === undefined ? undefined : null;
        const operation = string("operation", 120, true); const category = string("category", 80, true); const message = string("message", 1_024, true); const stack = string("stack", 2_048); const sourceId = string("sourceId", 128); const participantId = string("participantId", 128); const jobId = string("jobId", 128); const traceId = string("traceId", 32); const spanId = string("spanId", 16);
        if (!operation || !category || !message || [stack, sourceId, participantId, jobId, traceId, spanId].includes(null) || Boolean(traceId) !== Boolean(spanId) || (traceId && !/^[a-f0-9]{32}$/i.test(traceId)) || (spanId && !/^[a-f0-9]{16}$/i.test(spanId)) || (value.context !== undefined && (!value.context || typeof value.context !== "object" || Array.isArray(value.context)))) { response.writeHead(400); return response.end("Invalid error report"); }
        const reportId = await errors.report({ service: "app", operation, category, error: new Error(message), stack: stack ?? undefined, trace: { ...(traceId && spanId ? { traceId, spanId } : {}), ...(sourceId ? { sourceId } : {}), ...(participantId ? { participantId } : {}), ...(jobId ? { jobId } : {}) }, context: value.context as Record<string, unknown> | undefined });
        return json(response, 202, { reportId });
    });
    return;
  }
  response.writeHead(404); response.end("Not found");
}).listen(port);
console.log(`Speak Now is running at http://localhost:${port}`);
