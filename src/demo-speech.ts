import { randomInt } from "node:crypto";
import { messages } from "./demo";

let windowStarted = Date.now();
let requests = 0;

export async function generateDemo(request: Request): Promise<Response> {
  const origin = request.headers.get("origin");
  if (origin && origin !== (Bun.env.SPEAK_NOW_PUBLIC_ORIGIN ?? new URL(request.url).origin)) {
    return Response.json({ error: "Please play from the Speak Now page." }, { status: 403 });
  }
  const apiKey = Bun.env.ELEVENLABS_API_KEY;
  const voiceId = Bun.env.ELEVENLABS_VOICE_ID;
  if (!apiKey || !voiceId) {
    return Response.json({ error: "Speech is not configured yet. Please try again later." }, { status: 503 });
  }
  if (Date.now() - windowStarted >= 60_000) { windowStarted = Date.now(); requests = 0; }
  if (requests >= 20) {
    return Response.json({ error: "The demo is busy. Please try again in a minute." }, { status: 429, headers: { "Retry-After": "60" } });
  }
  requests++;
  const text = messages[randomInt(messages.length)]!;
  try {
    const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(voiceId)}`, {
      method: "POST",
      signal: AbortSignal.timeout(30_000),
      headers: { "xi-api-key": apiKey, "Content-Type": "application/json", Accept: "audio/mpeg" },
      body: JSON.stringify({ text, model_id: "eleven_flash_v2_5" }),
    });
    if (!response.ok || !response.body) {
      return Response.json({ error: "Speech generation failed. Please try again." }, { status: 502 });
    }
    const audio = await response.arrayBuffer();
    return new Response(audio, { headers: {
      "Content-Type": "audio/mpeg", "Cache-Control": "no-store", "X-Demo-Message": text,
    } });
  } catch {
    return Response.json({ error: "Speech generation timed out or is unavailable. Please try again." }, { status: 502 });
  }
}
