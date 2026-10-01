import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join } from "node:path";
import type { Logger } from "./logging";

export interface SpeechConfig { apiKey?: string; voiceId?: string; dataDir: string; model?: string; timeoutMs?: number; logger?: Logger; operationId?: string; jobId?: string; }
export interface Clip { path: string; cached: boolean; }
const pending = new Map<string, Promise<Clip>>();
const audioLimit = 20_000_000;

const positiveTimeout = (value: number | undefined) => {
  const timeout = value ?? 30_000;
  if (!Number.isFinite(timeout) || timeout <= 0) throw new Error("speech timeout must be a finite positive number");
  return timeout;
};

function cancellationError(): Error {
  return new Error("speech request was cancelled");
}

async function readAudio(body: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<Buffer> {
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  const cancel = () => { void reader.cancel(); };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    for (;;) {
      if (signal.aborted) throw cancellationError();
      const next = await reader.read();
      if (next.done) break;
      size += next.value.byteLength;
      if (size > audioLimit) {
        void reader.cancel();
        throw new Error("ElevenLabs audio exceeded the limit");
      }
      chunks.push(next.value);
    }
  } finally {
    signal.removeEventListener("abort", cancel);
    reader.releaseLock();
  }
  if (signal.aborted) throw cancellationError();
  return Buffer.concat(chunks);
}

export async function synthesize(text: string, key: string, config: SpeechConfig, fetcher: typeof fetch = fetch, signal?: AbortSignal): Promise<Clip> {
  const log = config.logger;
  const fields = { operationId: config.operationId, jobId: config.jobId, clipKey: key };
  if (!config.apiKey || !config.voiceId) {
    await log?.log("error", { operation: "speech.synthesize", message: "Speech configuration is unavailable", outcome: "rejected", jobId: config.jobId, metadata: fields });
    throw new Error("ELEVENLABS_API_KEY and ELEVENLABS_VOICE_ID are required for live speech");
  }
  const timeoutMs = positiveTimeout(config.timeoutMs);
  const apiKey = config.apiKey;
  const voiceId = config.voiceId;
  const directory = join(config.dataDir, "audio");
  const cacheKey = createHash("sha256").update(`${key}\0${voiceId}\0${config.model ?? "eleven_flash_v2_5"}`).digest("hex");
  const path = join(directory, `${cacheKey}.mp3`);
  if (await Bun.file(path).exists()) { await log?.log("info", { operation: "media.cache", message: "Reused cached audio", outcome: "cached", jobId: config.jobId, metadata: fields }); return { path, cached: true }; }
  const existing = pending.get(cacheKey);
  if (existing) return existing;
  const work = (async () => {
    await mkdir(directory, { recursive: true });
    const temporary = `${path}.${crypto.randomUUID()}.tmp`;
    const controller = new AbortController();
    const cancel = () => controller.abort();
    signal?.addEventListener("abort", cancel, { once: true });
    const deadline = setTimeout(cancel, timeoutMs);
      try {
        await log?.log("info", { operation: "speech.synthesize", message: "Started speech request", outcome: "started", jobId: config.jobId, metadata: fields });
      if (signal?.aborted) throw cancellationError();
      const response = await fetcher(`https://api.elevenlabs.io/v1/text-to-speech/${encodeURIComponent(voiceId)}`, {
        method: "POST",
        signal: controller.signal,
        headers: { "xi-api-key": apiKey, "Content-Type": "application/json", Accept: "audio/mpeg" },
        body: JSON.stringify({ text, model_id: config.model ?? "eleven_flash_v2_5" }),
      });
      if (controller.signal.aborted) throw cancellationError();
      if (!response.ok) throw new Error(`ElevenLabs returned ${response.status}`);
      if (!response.body) throw new Error("ElevenLabs returned no audio body");
      const audio = await readAudio(response.body, controller.signal);
      await writeFile(temporary, audio);
      if (controller.signal.aborted) throw cancellationError();
        await rename(temporary, path);
        await log?.log("info", { operation: "media.write", message: "Published audio clip", outcome: "succeeded", jobId: config.jobId, metadata: fields });
        return { path, cached: false };
      } catch (error) {
        await rm(temporary, { force: true });
        await log?.log("error", { operation: "speech.synthesize", message: "Speech request failed", outcome: "failed", jobId: config.jobId, metadata: { ...fields, error: error instanceof Error ? error.message : "unknown" } });
        throw error;
    } finally {
      clearTimeout(deadline);
      signal?.removeEventListener("abort", cancel);
    }
  })();
  pending.set(cacheKey, work);
  try { return await work; } finally { pending.delete(cacheKey); }
}
