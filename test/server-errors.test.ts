import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";

test("accepts only bounded same-origin client errors", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-server-errors-"));
  const port = 41_000 + Math.floor(Math.random() * 1_000);
  const origin = `http://localhost:${port}`;
  const server = Bun.spawn([Bun.which("bun")!, "src/server.ts"], { cwd: process.cwd(), stdout: "ignore", stderr: "pipe", env: { ...process.env, PORT: String(port), SPEAK_NOW_DATA_DIR: dataDirectory } });
  try {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      try { if ((await fetch(`${origin}/api/health`)).ok) break; } catch { /* Server is starting. */ }
      await Bun.sleep(10);
    }
    const body = { operation: "browser.playback", category: "playback", message: "Bearer synthetic-secret", traceId: "a".repeat(32), spanId: "b".repeat(16), context: { transcript: "do not retain", attempt: 1 } };
    const accepted = await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: JSON.stringify(body) });
    expect(accepted.status).toBe(202);
    expect((await accepted.json()).reportId).toMatch(/^[a-f0-9]{32}$/);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: "http://example.invalid", "Content-Type": "application/json" }, body: JSON.stringify(body) })).status).toBe(403);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "text/plain" }, body: "no" })).status).toBe(415);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json-not-json" }, body: JSON.stringify(body) })).status).toBe(415);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json; charset=utf-8" }, body: JSON.stringify({ operation: "browser.ids", category: "browser", message: "safe", sourceId: "source-a", participantId: "participant-a", jobId: "job-a" }) })).status).toBe(202);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: JSON.stringify({ operation: "browser.partial", category: "browser", message: "safe", traceId: "a".repeat(32) }) })).status).toBe(400);
    expect((await fetch(`${origin}/api/client-errors`, { method: "POST", headers: { Origin: origin, "Content-Type": "application/json" }, body: JSON.stringify({ ...body, message: "x".repeat(8_193) }) })).status).toBe(413);

    const reports = await readFile(join(dataDirectory, "errors", "reports.jsonl"), "utf8");
    expect(reports).toContain("browser.playback");
    expect(reports).not.toContain("synthetic-secret");
    expect(reports).not.toContain("do not retain");
    expect(reports).toContain("source-a");
  } finally {
    server.kill();
    await server.exited;
    await rm(dataDirectory, { recursive: true, force: true });
  }
}, 10_000);
