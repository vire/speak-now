import { readdir, writeFile } from "node:fs/promises";

const [dataDirectory, controlDirectory, operation, mode] = process.argv.slice(2);

if (mode === "root-gate") {
  const { mock } = await import("bun:test");
  const fs = { ...await import("node:fs/promises") };
  let paused = false;
  mock.module("node:fs/promises", () => ({
    ...fs,
    async readdir(path: string | Buffer | URL, ...args: never[]) {
      const entries = await fs.readdir(path, ...args);
      if (!paused && String(path) === `${dataDirectory}/traces` && entries.some((entry) => String(entry.name ?? entry).includes("seed-dead"))) {
        paused = true;
        await fs.writeFile(`${controlDirectory}/ready`, "ready");
        while (!(await Bun.file(`${controlDirectory}/go`).exists())) await Bun.sleep(2);
      }
      return entries;
    },
  }));
}

const { createTracer } = await import("../../src/tracing");
const tracer = createTracer({ dataDirectory });
await tracer.start(operation).end("succeeded");
await tracer.flush();
await writeFile(`${controlDirectory}/result.json`, JSON.stringify(tracer.getStatus()));

if (mode === "exit-with-partial") {
  const generation = (await readdir(`${dataDirectory}/traces`)).find((entry) => entry.startsWith(`writer-${process.pid}-`));
  if (generation) await writeFile(`${dataDirectory}/traces/${generation}/calls.jsonl`, "{partial", { flag: "a" });
}
