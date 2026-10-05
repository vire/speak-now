import { mkdtemp, readFile, readdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";

test("serializes independent report writers within the configured file cap", async () => {
  const dataDirectory = await mkdtemp(join(tmpdir(), "speak-now-errors-process-"));
  const module = join(process.cwd(), "src/errors.ts");
  const worker = `import {createErrorReporter} from ${JSON.stringify(module)}; const r=createErrorReporter({dataDirectory:process.argv[1],maxFileBytes:600,maxRecordBytes:600,maxRetainedFiles:100}); const ids=[]; for(let i=0;i<20;i++) ids.push(await r.report({service:'app',operation:'process',category:'fixture',error:new Error(process.argv[2]+'-'+i)})); await r.flush(); console.log(JSON.stringify(ids));`;
  try {
    const children = ["a", "b", "c"].map((name) => Bun.spawn([process.execPath, "-e", worker, dataDirectory, name], { stdout: "pipe", stderr: "pipe" }));
    const ids = (await Promise.all(children.map(async (child) => { expect(await child.exited).toBe(0); return JSON.parse(await new Response(child.stdout).text()) as string[]; }))).flat();
    const files = (await readdir(join(dataDirectory, "errors"))).filter((name) => name.endsWith(".jsonl"));
    const rows = (await Promise.all(files.map((name) => readFile(join(dataDirectory, "errors", name), "utf8")))).flatMap((text) => text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)));
    expect(rows.map((row) => row.reportId).sort()).toEqual(ids.sort());
    expect((await Promise.all(files.map((name) => stat(join(dataDirectory, "errors", name))))).every((file) => file.size <= 600)).toBe(true);
  } finally { await rm(dataDirectory, { recursive: true, force: true }); }
}, 15_000);
