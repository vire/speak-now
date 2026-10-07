import assert from "node:assert/strict";
import { appendFile, chmod, mkdtemp, mkdir, readFile, realpath, rm, stat, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { createServer } from "node:net";
import { join } from "node:path";
import { Database } from "bun:sqlite";

const scenario = process.env.SN05_SCENARIO;
assert(scenario === "single-participant-count" || scenario === "group-text-budget" || scenario === "nonzero-return" || scenario === "unchanged-terminal-cursors" || scenario === "silent-tail-101" || scenario === "silent-tail-long" || scenario === "silent-tail-partial" || scenario === "silent-tail-lost-ack" || scenario === "bad-candidate-recovery" || scenario === "silent-fragment-300" || scenario === "silent-fragment-600" || scenario === "silent-fragment-same-write-300" || scenario === "silent-fragment-same-write-600", "unknown NF scenario");
const token = "synthetic-nf-boundary-token";
const message = (id: string, text: string) => `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id, content: [{ type: "output_text", text }] } })}\n`;

async function port(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  assert(address && typeof address !== "string");
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function waitFor(test: () => Promise<boolean>) {
  const deadline = performance.now() + 5_000;
  while (!await test()) {
    assert(performance.now() < deadline, "fixture server did not start");
    await Bun.sleep(10);
  }
}

const root = await mkdtemp(join(process.env.REVIEW_SAFE_ROOT!, "nf-boundary-"));
const bin = join(root, "bin");
const home = join(root, "home");
const data = join(root, "collector-data");
const appData = join(root, "app-data");
const transcripts = join(root, "transcripts");
const snapshot = join(root, "snapshot.json");
const terminal = join(root, "terminal.txt");
const herdr = join(bin, "herdr");
const rg = join(bin, "rg");
const summary = join(bin, "claude");
const sessions = Array.from({ length: scenario === "unchanged-terminal-cursors" ? 50 : scenario === "group-text-budget" ? 3 : 1 }, (_, index) => `session-${index}`);
const topology: { result: { snapshot: Record<string, unknown> } } = { result: { snapshot: {
  workspaces: [{ workspace_id: "w", label: "Synthetic" }],
  tabs: [{ tab_id: "t", workspace_id: "w", label: "Synthetic" }],
  panes: sessions.map((_, index) => ({ pane_id: `p${index}`, tab_id: "t", terminal_id: `term${index}` })),
  agents: sessions.map((sessionId, index) => scenario === "unchanged-terminal-cursors"
    ? { agent: "unsupported", pane_id: `p${index}`, terminal_id: `term${index}` }
    : { agent: "codex", pane_id: `p${index}`, terminal_id: `term${index}`, agent_session: { kind: "id", source: "fixture", value: sessionId } }),
} } };

let app: ReturnType<typeof Bun.spawn> | undefined;
let proxy: ReturnType<typeof Bun.serve> | undefined;
let appOut: Promise<string> | undefined;
let appError: Promise<string> | undefined;
try {
  await Promise.all([mkdir(bin), mkdir(home), mkdir(transcripts)]);
  await writeFile(snapshot, JSON.stringify(topology));
  await writeFile(terminal, "\u0001".repeat(20_000));
  for (const sessionId of sessions) await writeFile(join(transcripts, `${sessionId}.jsonl`), `${JSON.stringify({ type: "session_meta", payload: { id: sessionId } })}\n${message("baseline", "synthetic baseline")}`);
  await writeFile(herdr, `#!/bin/sh\nprintf "%s\\n" "$0" >> "$REVIEW_HERDR_MARKER"\nif [ "$1" = "pane" ] && [ "$2" = "read" ]; then /bin/cat "$REVIEW_TERMINAL"; else /bin/cat "$REVIEW_SNAPSHOT"; fi\n`);
  const writeRg = async (resultPath = "$REVIEW_TRANSCRIPTS/$5.jsonl") => writeFile(rg, `#!/bin/sh\nprintf "%s\\n" "$0" >> "$REVIEW_RG_MARKER"\nprintf "%s\\n" "${resultPath}"\n`);
  await writeRg();
  await writeFile(summary, `#!/bin/sh\nprintf "%s\\n" "$0" >> "$REVIEW_SUMMARY_MARKER"\nexit 91\n`);
  await Promise.all([herdr, rg, summary].map((path) => chmod(path, 0o755)));
  const origin = `http://127.0.0.1:${await port()}`;
  app = Bun.spawn([process.env.REVIEW_SAFE_BUN!, "--no-env-file", "src/server.ts"], {
    cwd: process.cwd(), stdout: "pipe", stderr: "pipe",
    env: { PATH: process.env.PATH!, HOME: home, TMPDIR: root, XDG_CONFIG_HOME: join(home, "config"), XDG_CACHE_HOME: join(home, "cache"), PORT: origin.split(":").at(-1)!, SPEAK_NOW_DATA_DIR: appData, SPEAK_NOW_COLLECTOR_TOKEN: token },
  });
  appOut = new Response(app.stdout as ReadableStream<Uint8Array>).text();
  appError = new Response(app.stderr as ReadableStream<Uint8Array>).text();
  let discardNextBaselineReceipt = false;
  const posts: Array<{ batchId: string; hash: string; activities: number; cursors: Array<{ participantId: string; previous: string | null; next: string }>; baselineReady: string[]; bytes: number; status: number }> = [];
  proxy = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) => {
    const url = new URL(request.url);
    const headers = new Headers(request.headers);
    headers.delete("host"); headers.delete("content-length");
    const body = request.method === "GET" ? undefined : await request.text();
    const response = await fetch(`${origin}${url.pathname}${url.search}`, { method: request.method, headers, body });
    if (url.pathname === "/api/collector/batches" && body) {
      const dto = JSON.parse(body) as { batchId: string; activities: unknown[]; cursors: Array<{ participantId: string; previous: string | null; next: string }>; baselineReady: string[] };
      posts.push({ batchId: dto.batchId, hash: createHash("sha256").update(body).digest("hex"), activities: dto.activities.length, cursors: dto.cursors, baselineReady: dto.baselineReady, bytes: Buffer.byteLength(body), status: response.status });
      if (discardNextBaselineReceipt && dto.baselineReady.some((participantId) => participantId.includes("session-replacement")) && response.status === 201) {
        discardNextBaselineReceipt = false;
        await response.arrayBuffer();
        return Response.json({ code: "synthetic_lost_receipt" }, { status: 503 });
      }
    }
    return response;
  } });
  const caller = join(root, "caller.ts");
  const collectorModule = process.env.SN05_COLLECTOR_MODULE || join(process.cwd(), "src/collector.ts");
  await writeFile(caller, `import {realpathSync} from "node:fs";import {restore,runOnce} from ${JSON.stringify(collectorModule)};const selected={herdr:realpathSync(Bun.which("herdr")!),rg:realpathSync(Bun.which("rg")!),summary:realpathSync(Bun.which("claude")!)};const state=await restore(${JSON.stringify(data)});const result=await runOnce({sourceNamespace:"fixture",dataDir:${JSON.stringify(data)},excludePaneIds:[],collectorUrl:${JSON.stringify(proxy.url.origin)},collectorToken:${JSON.stringify(token)},workerId:"synthetic-nf-worker"},state);console.log(JSON.stringify({selected,captured:result.activities.length,capturedTextBytes:result.activities.reduce((total,activity)=>total+Buffer.byteLength(activity.text),0),pending:state.remote.pendingBatches.length,statuses:state.sourceStatuses.filter(status=>status.kind==="overflow"),current:state.topology?.participants.map(participant=>({id:participant.id,session:participant.sessionId,generation:participant.generation})),absent:state.remote.absentParticipantIds}));`);
  async function runCollector() {
    const child = Bun.spawn([process.env.REVIEW_SAFE_BUN!, "--no-env-file", caller], { cwd: process.cwd(), stdout: "pipe", stderr: "pipe", env: { PATH: `${bin}:/usr/bin:/bin`, HOME: home, TMPDIR: root, XDG_CONFIG_HOME: join(home, "config"), XDG_CACHE_HOME: join(home, "cache"), SUMMARY_BACKEND: "claude", SUMMARY_MODEL: "synthetic", REVIEW_HERDR_MARKER: join(root, "herdr-marker"), REVIEW_RG_MARKER: join(root, "rg-marker"), REVIEW_SUMMARY_MARKER: join(root, "summary-marker"), REVIEW_SNAPSHOT: snapshot, REVIEW_TRANSCRIPTS: transcripts, REVIEW_TERMINAL: terminal } });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    assert.equal(child.exitCode, 0, stderr); assert.equal(stderr, "");
    const result = JSON.parse(stdout) as { selected: Record<string, string> };
    assert.deepEqual(result.selected, { herdr: await realpath(herdr), rg: await realpath(rg), summary: await realpath(summary) });
    for (const [name, path] of [["herdr", herdr], ...(scenario === "unchanged-terminal-cursors" ? [] : [["rg", rg]])] as Array<[string, string]>) assert((await readFile(join(root, `${name}-marker`), "utf8")).trim().split("\n").every((entry) => entry === path));
    assert.equal(await Bun.file(join(root, "summary-marker")).exists(), false);
    return result as Record<string, unknown>;
  }
  const state = () => Bun.file(join(data, "collector-state.json")).json() as Promise<{ remote: { acknowledgedCursors: Record<string, string>; absentParticipantIds: string[]; lastTopologyPublishedAt?: string; pendingBatches: unknown[] }; cursors: Record<string, { sourceCursor: string }>; sourceStatuses: Array<{ participantId: string; kind: string }> }>;
  const database = () => { const db = new Database(join(appData, "speak-now.sqlite"), { readonly: true }); try { return { events: db.query("SELECT COUNT(*) AS count, COUNT(DISTINCT event_id) AS distinctCount FROM events").get() as { count: number; distinctCount: number }, eventEvidence: db.query("SELECT evidence_json AS evidenceJson FROM events ORDER BY source_cursor").all() as Array<{ evidenceJson: string }>, cursors: db.query("SELECT participant_id AS participantId, cursor_value AS cursor FROM cursors ORDER BY participant_id").all() as Array<{ participantId: string; cursor: string }> }; } finally { db.close(); } };
  const eventCount = () => database().events.count;
  await waitFor(async () => { try { return (await fetch(`${origin}/api/health`, { signal: AbortSignal.timeout(100) })).ok; } catch { return false; } });
  const seed = await runCollector();
  if (scenario !== "unchanged-terminal-cursors") assert.equal(seed.pending, 0);
  let result: Record<string, unknown>;
  if (scenario === "single-participant-count" || scenario === "group-text-budget") {
    const groups = scenario === "single-participant-count" ? [101] : [35, 35, 10];
    const bytes = scenario === "single-participant-count" ? 1_000 : 7_000;
    for (const [index, count] of groups.entries()) for (let event = 0; event < count; event += 1) await appendFile(join(transcripts, `${sessions[index]}.jsonl`), message(`event-${index}-${event}`, "x".repeat(bytes)));
    const start = posts.length;
    const first = await runCollector();
    const firstPosts = posts.slice(start);
    for (let attempt = 0; attempt < 8 && eventCount() < groups.reduce((total, count) => total + count, 0); attempt += 1) await runCollector();
    result = { scenario, first, firstPosts, storedEvents: eventCount(), allBodiesWithinCap: posts.every((post) => post.bytes <= 524_288), final: await state(), durable: database() };
  } else if (scenario === "nonzero-return") {
    assert.equal((await fetch(`${origin}/api/listening`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sourceId: "fixture:herdr", workspaceId: "fixture:w" }) })).status, 200);
    await runCollector();
    const b = "session-replacement";
    await writeFile(join(transcripts, `${b}.jsonl`), `${JSON.stringify({ type: "session_meta", payload: { id: b } })}\n${message("replacement", "replacement baseline")}`);
    const agents = topology.result.snapshot.agents as Array<Record<string, unknown>>;
    const panes = topology.result.snapshot.panes as Array<Record<string, unknown>>;
    agents[0] = { ...agents[0], agent_session: { kind: "id", source: "fixture", value: b } }; await writeFile(snapshot, JSON.stringify(topology));
    const replaced = await runCollector(); const old = (replaced.current as Array<{ id: string; generation: number }>)[0]!; assert.equal(old.generation, 1);
    const oldAcknowledgedCursor = (await state()).remote.acknowledgedCursors[old.id];
    const pane = panes[0]!; const agent = agents[0]!;
    topology.result.snapshot.panes = []; topology.result.snapshot.agents = []; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
    await appendFile(join(transcripts, `${b}.jsonl`), message("absent", "silent while absent"));
    const c = "session-interim"; await writeFile(join(transcripts, `${c}.jsonl`), `${JSON.stringify({ type: "session_meta", payload: { id: c } })}\n${message("interim", "interim baseline")}`);
    topology.result.snapshot.panes = [pane]; topology.result.snapshot.agents = [{ ...agent, agent_session: { kind: "id", source: "fixture", value: c } }]; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
    topology.result.snapshot.panes = []; topology.result.snapshot.agents = []; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
    const beforeReturnEvents = eventCount(); const postStart = posts.length;
    topology.result.snapshot.panes = [pane]; topology.result.snapshot.agents = [agent]; await writeFile(snapshot, JSON.stringify(topology)); const returned = await runCollector(); const afterReturnEvents = eventCount();
    const returnPost = posts.slice(postStart).find((post) => post.baselineReady.includes(old.id));
    await appendFile(join(transcripts, `${b}.jsonl`), message("visible", "visible after return")); await runCollector();
    result = { scenario, oldId: old.id, oldGeneration: old.generation, oldAcknowledgedCursor, returned: (returned.current as unknown[])[0], returnPost, beforeReturnEvents, afterReturnEvents, afterVisibleEvents: eventCount(), after: await state() };
  } else if (scenario === "unchanged-terminal-cursors") {
    for (let attempt = 0; attempt < 40 && (await state()).remote.pendingBatches.length; attempt += 1) await runCollector();
    const before = await state(); const postCount = posts.length;
    before.remote.lastTopologyPublishedAt = "1970-01-01T00:00:00.000Z"; await Bun.write(join(data, "collector-state.json"), JSON.stringify(before));
    const heartbeat = await runCollector(); const after = await state();
    result = { scenario, heartbeat, before, after, heartbeatPost: posts.at(-1), noFollowupPost: posts.length === postCount, unchangedTokens: Object.entries(before.cursors).every(([id, cursor]) => after.cursors[id]?.sourceCursor === cursor.sourceCursor) };
  } else if (scenario === "bad-candidate-recovery" || scenario === "silent-fragment-300" || scenario === "silent-fragment-600" || scenario === "silent-fragment-same-write-300" || scenario === "silent-fragment-same-write-600") {
    assert.equal((await fetch(`${origin}/api/listening`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sourceId: "fixture:herdr", workspaceId: "fixture:w" }) })).status, 200);
    await runCollector();
    const b = "session-replacement";
    const bPath = join(transcripts, `${b}.jsonl`);
    await writeFile(bPath, `${JSON.stringify({ type: "session_meta", payload: { id: b } })}\n${message("replacement", "replacement baseline")}`);
    const agents = topology.result.snapshot.agents as Array<Record<string, unknown>>;
    const panes = topology.result.snapshot.panes as Array<Record<string, unknown>>;
    agents[0] = { ...agents[0], agent_session: { kind: "id", source: "fixture", value: b } }; await writeFile(snapshot, JSON.stringify(topology));
    const replaced = await runCollector(); const old = (replaced.current as Array<{ id: string; generation: number }>)[0]!; assert.equal(old.generation, 1);
    const previous = (await state()).remote.acknowledgedCursors[old.id];
    const pane = panes[0]!; const agent = agents[0]!;
    topology.result.snapshot.panes = []; topology.result.snapshot.agents = []; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
    if (scenario === "bad-candidate-recovery") {
      for (let index = 0; index < 5; index += 1) await appendFile(bPath, message(`unseen-${index}`, `unseen ${index}`));
      const highWater = (await stat(bPath)).size;
      const decoy = join(transcripts, "different-session.jsonl");
      await writeFile(decoy, `${JSON.stringify({ type: "session_meta", payload: { id: "different-session" } })}\n${message("decoy", `mentions ${b} but is not its transcript`)}`);
      await writeRg(decoy);
      const postStart = posts.length;
      topology.result.snapshot.panes = [pane]; topology.result.snapshot.agents = [agent]; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
      const rejected = await state();
      const rejectedBaseline = posts.slice(postStart).find((post) => post.baselineReady.includes(old.id));
      const eventsAfterRejected = eventCount();
      await writeRg();
      const recoveryStart = posts.length;
      await runCollector();
      const recoveryBaseline = posts.slice(recoveryStart).find((post) => post.baselineReady.includes(old.id));
      const eventsAfterRecovery = eventCount();
      await appendFile(bPath, message("visible-after-recovery", "visible after recovery")); await runCollector();
      result = { scenario, oldId: old.id, previous, highWater, rejectedBaseline, absentAfterRejected: rejected.remote.absentParticipantIds, acknowledgedAfterRejected: rejected.remote.acknowledgedCursors[old.id], unavailableAfterRejected: rejected.sourceStatuses.some((status) => status.participantId === old.id && status.kind === "unavailable"), eventsAfterRejected, recoveryBaseline, eventsAfterRecovery, eventsAfterVisible: eventCount() };
    } else {
      await appendFile(bPath, message("absent", "silent while absent"));
      const fragment = message("partial-before-h", "old fragment must stay silent");
      const cut = fragment.length - 9;
      await appendFile(bPath, fragment.slice(0, cut));
      const partialRemainder = fragment.slice(cut);
      topology.result.snapshot.panes = [pane]; topology.result.snapshot.agents = [agent]; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
      const sameWrite = scenario === "silent-fragment-same-write-300" || scenario === "silent-fragment-same-write-600";
      const bytes = scenario === "silent-fragment-300" || scenario === "silent-fragment-same-write-300" ? 300_000 : 600_000;
      if (sameWrite) await appendFile(bPath, `${partialRemainder}${message("large-post-fragment", `${"x".repeat(bytes)}large post-fragment record`)}`);
      else {
        await appendFile(bPath, partialRemainder);
        await runCollector();
        await appendFile(bPath, message("large-post-fragment", `${"x".repeat(bytes)}large post-fragment record`));
      }
      const polls = sameWrite ? (bytes === 300_000 ? 2 : 7) : (bytes === 300_000 ? 1 : 5);
      for (let attempt = 0; attempt < polls; attempt += 1) await runCollector();
      const durable = database();
      const events = durable.eventEvidence.map(({ evidenceJson }) => JSON.parse(evidenceJson) as { status: string; truncated: boolean; originalTextBytes: number; text: string });
      result = { scenario, polls, events, finalCursor: (await state()).cursors[old.id]?.sourceCursor, expectedCursor: String((await stat(bPath)).size) };
    }
  } else {
    assert.equal((await fetch(`${origin}/api/listening`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sourceId: "fixture:herdr", workspaceId: "fixture:w" }) })).status, 200);
    await runCollector();
    const b = "session-replacement";
    const bPath = join(transcripts, `${b}.jsonl`);
    await writeFile(bPath, `${JSON.stringify({ type: "session_meta", payload: { id: b } })}\n${message("replacement", "replacement baseline")}`);
    const agents = topology.result.snapshot.agents as Array<Record<string, unknown>>;
    const panes = topology.result.snapshot.panes as Array<Record<string, unknown>>;
    agents[0] = { ...agents[0], agent_session: { kind: "id", source: "fixture", value: b } }; await writeFile(snapshot, JSON.stringify(topology));
    const replaced = await runCollector(); const old = (replaced.current as Array<{ id: string; generation: number }>)[0]!; assert.equal(old.generation, 1);
    const previous = (await state()).remote.acknowledgedCursors[old.id];
    const pane = panes[0]!; const agent = agents[0]!;
    topology.result.snapshot.panes = []; topology.result.snapshot.agents = []; await writeFile(snapshot, JSON.stringify(topology)); await runCollector();
    const absenceCount = scenario === "silent-tail-long" ? 300 : 101;
    for (let index = 0; index < absenceCount; index += 1) await appendFile(bPath, message(`absent-${index}`, "x".repeat(1_000)));
    let partialRemainder = "";
    if (scenario === "silent-tail-partial") {
      const fragment = message("partial-before-h", "old fragment must stay silent");
      const cut = fragment.length - 9;
      await appendFile(bPath, fragment.slice(0, cut));
      partialRemainder = fragment.slice(cut);
    }
    const highWater = (await stat(bPath)).size;
    const postStart = posts.length;
    discardNextBaselineReceipt = scenario === "silent-tail-lost-ack";
    topology.result.snapshot.panes = [pane]; topology.result.snapshot.agents = [agent]; await writeFile(snapshot, JSON.stringify(topology));
    const returned = await runCollector();
    const returnPosts = posts.slice(postStart);
    const returnBaseline = returnPosts.find((post) => post.baselineReady.includes(old.id));
    if (scenario === "silent-tail-lost-ack") {
      await appendFile(bPath, message("post-h", "post h visible"));
      const retry = await runCollector();
      const retryBaseline = posts.slice(postStart).filter((post) => post.baselineReady.includes(old.id));
      await runCollector();
      result = { scenario, oldId: old.id, previous, highWater, returned, returnBaseline, retry, retryBaseline, events: eventCount(), final: await state() };
    } else {
      const idle = await runCollector();
      if (partialRemainder) {
        await appendFile(bPath, partialRemainder);
        await runCollector();
        await appendFile(bPath, message("visible-after-fragment", "visible after fragment"));
      } else await appendFile(bPath, message("visible-after-return", "visible after return"));
      const visible = await runCollector();
      result = { scenario, oldId: old.id, previous, highWater, returned, returnBaseline, idle, eventsAfterIdle: eventCount() - (visible.captured as number), eventsAfterVisible: eventCount(), visible, final: await state() };
    }
  }
  console.log(JSON.stringify([result]));
} finally {
  proxy?.stop(true);
  if (app?.exitCode === null) app.kill("SIGTERM");
  if (app) { const timer = setTimeout(() => { if (app!.exitCode === null) app!.kill("SIGKILL"); }, 2_000); await app.exited; clearTimeout(timer); assert.equal(app.exitCode, 0, await appError!); await appOut; }
  await rm(root, { recursive: true, force: true });
}
