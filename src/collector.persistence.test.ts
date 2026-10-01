import { expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

test("atomic state publication failure keeps the acknowledged cursor recoverable", async () => {
  const root = await mkdtemp(join(tmpdir(), "speak-now-persist-"));
  const bin = join(root, "bin"); const data = join(root, "data"); const transcript = join(root, "source.jsonl");
  try {
    await mkdir(bin); await mkdir(data);
    const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;
    await writeFile(transcript, `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${message("baseline", "baseline output")}`);
    await writeFile(join(root, "snapshot.json"), JSON.stringify({ result: { snapshot: { agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } }] } } }));
    await writeFile(join(bin, "herdr"), `#!/bin/sh\n/bin/cat ${join(root, "snapshot.json")}\n`);
    await writeFile(join(bin, "rg"), `#!/bin/sh\n/bin/echo ${transcript}\n`);
    await writeFile(join(bin, "claude"), "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"system\",\"tools\":[],\"mcp_servers\":[]}' '{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"result\":\"{\\\"speak\\\":false,\\\"kind\\\":\\\"progress\\\",\\\"text\\\":\\\"stored\\\",\\\"evidenceEventIds\\\":[]}\"}'\n");
    for (const name of ["herdr", "rg", "claude"]) await chmod(join(bin, name), 0o755);
    const script = `import {appendFile,rename,rm,mkdir,readFile} from 'node:fs/promises'; import {runOnce,initialState} from './src/collector.ts'; const data=${JSON.stringify(data)}, source=${JSON.stringify(transcript)}, state=initialState(), config={sourceNamespace:'fixture',dataDir:data,excludePaneIds:[]}; await runOnce(config,state); const before=state.cursors[Object.keys(state.cursors)[0]].offset; await appendFile(source,${JSON.stringify(message("update", "recoverable update"))}); await rename(data+'/collector-state.json',data+'/saved.json'); await mkdir(data+'/collector-state.json'); let failed=false; try{await runOnce(config,state)}catch{failed=true}; const unchanged=state.cursors[Object.keys(state.cursors)[0]].offset===before; await rm(data+'/collector-state.json',{recursive:true}); await rename(data+'/saved.json',data+'/collector-state.json'); await runOnce(config,state); const saved=JSON.parse(await readFile(data+'/collector-state.json','utf8')); console.log(JSON.stringify({failed,unchanged,recovered:saved.jobs.some((j:any)=>j.done)&&saved.activityIds.length===1}));`;
    const child = Bun.spawn([Bun.which("bun")!, "-e", script], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:${process.env.PATH}`, HOME: root, SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "fixture" } });
    const output = await new Response(child.stdout).text(); const error = await new Response(child.stderr).text(); await child.exited;
    expect(child.exitCode, error).toBe(0);
    expect(JSON.parse(output)).toEqual({ failed: true, unchanged: true, recovered: true });
  } finally { await rm(root, { recursive: true, force: true }); }
});
