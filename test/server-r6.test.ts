import { connect } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";

test("closes chunked oversized client errors before the terminator", async () => {
  const data = await mkdtemp(join(tmpdir(), "speak-now-r6-test-")); const port = 44300 + Math.floor(Math.random() * 400); const origin = `http://localhost:${port}`;
  const server = Bun.spawn([process.execPath, "src/server.ts"], { cwd: process.cwd(), stdout: "ignore", stderr: "pipe", env: { ...process.env, PORT: String(port), SPEAK_NOW_DATA_DIR: data } });
  const send = (complete: boolean, fragmented = false) => new Promise<{ before: boolean; response: string }>((resolve, reject) => {
    const socket = connect({ host: "127.0.0.1", port }); let sentTerminator = false; let response = "";
    const timer = setTimeout(() => { socket.destroy(); reject(new Error("oversized connection stayed open")); }, 1_000);
    socket.on("error", () => undefined); socket.on("data", (chunk) => { response += chunk; }); socket.on("close", () => { clearTimeout(timer); resolve({ before: !sentTerminator, response }); });
    socket.on("connect", () => { socket.write(`POST /api/client-errors HTTP/1.1\r\nHost: localhost:${port}\r\nOrigin: ${origin}\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n`); if (fragmented) {
      socket.write("2ee0\r\n");
      const fragments = ["x".repeat(4_096), "x".repeat(4_096), "x"];
      const writeNext = (index: number) => {
        if (index === fragments.length || socket.destroyed) return;
        socket.write(fragments[index], (error) => {
          if (error) { clearTimeout(timer); reject(error); return; }
          setTimeout(() => writeNext(index + 1), 20);
        });
      };
      writeNext(0);
      return;
    }
    const body = "x".repeat(9_000); socket.write(`${body.length.toString(16)}\r\n${body}\r\n`); if (complete) { sentTerminator = true; socket.write("0\r\n\r\n"); } });
  });
  try {
    for (let i = 0; i < 100; i += 1) { try { if ((await fetch(`${origin}/api/health`)).ok) break; } catch {} await Bun.sleep(10); }
    const incomplete = await send(false); expect(incomplete.before).toBe(true); expect(incomplete.response).toContain("413 Payload Too Large");
    const fragmented = await send(false, true); expect(fragmented.before).toBe(true); expect(fragmented.response).toContain("413 Payload Too Large");
    expect((await fetch(`${origin}/api/health`)).ok).toBe(true);
    const completed = await send(true); expect(completed.before).toBe(false); expect(completed.response).toContain("413 Payload Too Large");
    expect((await fetch(`${origin}/api/health`)).ok).toBe(true);
    expect(await Bun.file(join(data, "errors", "reports.jsonl")).exists()).toBe(false);
  } finally { server.kill(); await server.exited; await rm(data, { recursive: true, force: true }); }
}, 10_000);
