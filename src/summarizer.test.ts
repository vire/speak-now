import { expect, test } from "bun:test";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

interface WorkerResult {
  summary?: {
    speak: boolean;
    kind: string;
    text: string;
    evidenceEventIds: string[];
  };
  error?: string;
}

async function runWorker(
  directory: string,
  carrier: string,
  registry = "empty",
  errorEnvelope = false,
): Promise<WorkerResult> {
  await writeFile(join(directory, "carrier"), carrier);
  await writeFile(join(directory, "registry"), registry);
  await writeFile(join(directory, "error"), errorEnvelope ? "true" : "false");
  const script = `
import { summarize } from ${JSON.stringify(join(process.cwd(), "src/summarizer.ts"))};

try {
  const summary = await summarize({ text: "generic evidence", evidenceEventIds: ["event-a"] }, "claude", "fixture");
  console.log(JSON.stringify({ summary }));
} catch (error) {
  console.log(JSON.stringify({ error: error instanceof Error ? error.message : "unknown error" }));
}
`;
  const child = Bun.spawn([Bun.which("bun")!, "-e", script], {
    cwd: directory,
    stdin: "ignore",
    stdout: "pipe",
    stderr: "pipe",
    env: {
      PATH: `${directory}:${process.env.PATH}`,
      HOME: directory,
      USER: "fixture-user",
      UNRELATED_FIXTURE: "not-allowed",
    },
  });
  const timer = setTimeout(() => child.kill(), 5_000);
  try {
    const [stdout, stderr] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    return JSON.parse(stdout) as WorkerResult;
  } finally {
    clearTimeout(timer);
    if (child.exitCode === null) child.kill();
    await child.exited;
  }
}

test("Claude worker preserves the controlled prompt and rejects error envelopes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "speak-now-summary-test-"));
  const command = join(directory, "claude");
  const payload = JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: [] });

  try {
    await writeFile(
      command,
      `#!${Bun.which("bun")!}
import { readFile, writeFile } from "node:fs/promises";
const root = ${JSON.stringify(directory)};
const prompt = await readFile(0, "utf8");
await writeFile(root + "/prompt", prompt);
await writeFile(root + "/environment.json", JSON.stringify({
  hasConfig: Boolean(process.env.CLAUDE_CONFIG_DIR),
  hasUser: Boolean(process.env.USER),
  hasUnrelated: Boolean(process.env.UNRELATED_FIXTURE),
}));
const nonempty = (await readFile(root + "/registry", "utf8")).trim() === "nonempty";
const error = (await readFile(root + "/error", "utf8")).trim() === "true";
console.log(JSON.stringify({ type: "system", tools: nonempty ? ["tool"] : [], mcp_servers: nonempty ? ["server"] : [] }));
console.log(JSON.stringify({
  type: "result",
  subtype: "success",
  is_error: error,
  result: error ? "provider failed" : await readFile(root + "/carrier", "utf8"),
}));
`,
    );
    await chmod(command, 0o755);

    const result = await runWorker(directory, payload, "empty", true);
    expect(result.error).toContain("successful result");
    const prompt = await readFile(join(directory, "prompt"), "utf8");
    expect(prompt).toContain('"required"');
    expect(prompt).toContain("Return JSON only, without Markdown fences, prose, or additional text.");
    expect(JSON.parse(await readFile(join(directory, "environment.json"), "utf8"))).toEqual({
      hasConfig: false,
      hasUser: true,
      hasUnrelated: false,
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Claude worker accepts only one strict JSON carrier and schema-valid result", async () => {
  const directory = await mkdtemp(join(tmpdir(), "speak-now-summary-carrier-"));
  const command = join(directory, "claude");
  const valid = JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: ["event-a"] });
  const spokenTooLong = JSON.stringify({
    speak: true,
    kind: "progress",
    text: Array.from({ length: 61 }, () => "word").join(" "),
    evidenceEventIds: ["event-a"],
  });
  const cases = [
    { name: "plain JSON", carrier: valid, accepted: true },
    { name: "CRLF json fence", carrier: ` \r\n\`\`\`json\r\n${valid}\r\n\`\`\`\r\n `, accepted: true },
    { name: "prose around JSON", carrier: `note\n${valid}`, accepted: false },
    { name: "prose after fence", carrier: `\`\`\`json\n${valid}\n\`\`\`\nmore`, accepted: false },
    { name: "concatenated documents", carrier: `${valid}\n${valid}`, accepted: false },
    { name: "two fences", carrier: `\`\`\`json\n${valid}\n\`\`\`\n\`\`\`json\n${valid}\n\`\`\``, accepted: false },
    { name: "wrong fence label", carrier: `\`\`\`JSON\n${valid}\n\`\`\``, accepted: false },
    { name: "incomplete fence", carrier: `\`\`\`json\n${valid}`, accepted: false },
    { name: "malformed fenced JSON", carrier: "```json\n{bad}\n```", accepted: false },
    { name: "kind array", carrier: JSON.stringify({ speak: false, kind: ["progress"], text: "stored", evidenceEventIds: [] }), accepted: false },
    { name: "extra key", carrier: JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: [], extra: true }), accepted: false },
    { name: "unknown evidence", carrier: JSON.stringify({ speak: false, kind: "progress", text: "stored", evidenceEventIds: ["other"] }), accepted: false },
    { name: "spoken text over bound", carrier: spokenTooLong, accepted: false },
    { name: "nonempty registry", carrier: valid, registry: "nonempty", accepted: false },
  ];

  try {
    await writeFile(
      command,
      `#!${Bun.which("bun")!}
import { readFile, writeFile } from "node:fs/promises";
const root = ${JSON.stringify(directory)};
await writeFile(root + "/prompt", await readFile(0, "utf8"));
const nonempty = (await readFile(root + "/registry", "utf8")).trim() === "nonempty";
console.log(JSON.stringify({ type: "system", tools: nonempty ? ["tool"] : [], mcp_servers: nonempty ? ["server"] : [] }));
console.log(JSON.stringify({ type: "result", subtype: "success", is_error: false, result: await readFile(root + "/carrier", "utf8") }));
`,
    );
    await chmod(command, 0o755);

    for (const entry of cases) {
      const result = await runWorker(directory, entry.carrier, entry.registry);
      if (entry.accepted) {
        expect(result.summary, entry.name).toEqual({
          speak: false,
          kind: "progress",
          text: "stored",
          evidenceEventIds: ["event-a"],
        });
      } else {
        expect(result.error, entry.name).toBeDefined();
      }
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}, 45_000);
