import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

type Scenario = "single-participant-count" | "group-text-budget" | "nonzero-return" | "unchanged-terminal-cursors" | "silent-tail-101" | "silent-tail-long" | "silent-tail-partial" | "silent-tail-lost-ack" | "bad-candidate-recovery" | "silent-fragment-300" | "silent-fragment-600" | "silent-fragment-same-write-300" | "silent-fragment-same-write-600";

async function runBoundaryScenario(scenario: Scenario) {
  const root = await mkdtemp(join(tmpdir(), "sn05-tb0043-"));
  try {
    const child = Bun.spawn([process.execPath, "--no-env-file", join(process.cwd(), "src/collector.nf-boundary.fixture.ts")], {
      cwd: process.cwd(),
      stdout: "pipe",
      stderr: "pipe",
      env: {
        PATH: "/usr/bin:/bin",
        HOME: join(root, "home"),
        TMPDIR: root,
        XDG_CONFIG_HOME: join(root, "config"),
        XDG_CACHE_HOME: join(root, "cache"),
        REVIEW_SAFE_ROOT: root,
        REVIEW_SAFE_BUN: process.execPath,
        SN05_SCENARIO: scenario,
        SN05_COLLECTOR_MODULE: process.env.SN05_COLLECTOR_MODULE ?? "",
      },
    });
    const [stdout, stderr] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text()]);
    await child.exited;
    expect(child.exitCode, stderr).toBe(0);
    expect(stderr).toBe("");
    return JSON.parse(stdout) as Array<Record<string, unknown>>;
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("NF1 count limits one remote group at 100 activities and leaves the 101st at its record boundary", async () => {
  const [result] = await runBoundaryScenario("single-participant-count");
  const collected = result.first as { captured: number; capturedTextBytes: number };
  expect(collected).toMatchObject({ captured: 100, capturedTextBytes: 100_000 });
  expect(result.storedEvents).toBe(101);
  expect(result.allBodiesWithinCap).toBe(true);
  const final = result.final as { cursors: Record<string, { sourceCursor: string }>; remote: { acknowledgedCursors: Record<string, string> } };
  const durable = result.durable as { events: { count: number; distinctCount: number }; cursors: Array<{ participantId: string; cursor: string }> };
  expect(durable.events).toEqual({ count: 101, distinctCount: 101 });
  expect(durable.cursors).toEqual(Object.entries(final.cursors).map(([participantId, cursor]) => ({ participantId, cursor: cursor.sourceCursor })));
  expect(final.remote.acknowledgedCursors).toEqual(Object.fromEntries(Object.entries(final.cursors).map(([participantId, cursor]) => [participantId, cursor.sourceCursor])));
}, 60_000);

test("NF1 text limits all participants in one remote group to 512000 text bytes", async () => {
  const [result] = await runBoundaryScenario("group-text-budget");
  const collected = result.first as { captured: number; capturedTextBytes: number };
  expect(collected.captured).toBeLessThan(80);
  expect(collected.capturedTextBytes).toBeLessThanOrEqual(512_000);
  expect(result.storedEvents).toBe(80);
  expect((result.durable as { events: { count: number; distinctCount: number } }).events).toEqual({ count: 80, distinctCount: 80 });
}, 60_000);

test("NF2 restores the exact nonzero identity after absence, interim occupant, and fresh callers", async () => {
  const [result] = await runBoundaryScenario("nonzero-return");
  const returned = result.returned as { id: string; generation: number };
  expect(returned.id).toBe(result.oldId as string);
  expect(returned.generation).toBe(1);
  expect(result.afterReturnEvents).toBe(result.beforeReturnEvents);
  expect(result.afterVisibleEvents).toBe((result.beforeReturnEvents as number) + 1);
  const returnPost = result.returnPost as { cursors: Array<{ participantId: string; previous: string | null; next: string }> };
  expect(returnPost.cursors).toContainEqual({ participantId: result.oldId as string, previous: result.oldAcknowledgedCursor as string, next: expect.any(String) });
}, 60_000);

test("NF3 sends an idle topology heartbeat without unchanged terminal cursor units", async () => {
  const [result] = await runBoundaryScenario("unchanged-terminal-cursors");
  expect(result.unchangedTokens).toBe(true);
  expect(result.noFollowupPost).toBe(false);
  const heartbeatPost = result.heartbeatPost as { activities: number; cursors: unknown[]; status: number };
  expect(heartbeatPost).toMatchObject({ activities: 0, cursors: [], status: 201 });
  expect((result.heartbeat as { pending: number }).pending).toBe(0);
}, 60_000);

test("NF4 silently baselines all 101 absent records at the verified join-time tail", async () => {
  const [result] = await runBoundaryScenario("silent-tail-101");
  const baseline = result.returnBaseline as { cursors: Array<{ participantId: string; previous: string | null; next: string }> };
  expect(baseline.cursors).toContainEqual({ participantId: result.oldId as string, previous: result.previous as string, next: String(result.highWater) });
  expect(result.eventsAfterIdle).toBe(0);
  expect(result.eventsAfterVisible).toBe(1);
}, 60_000);

test("NF4 baselines an absence history beyond the bounded active read window", async () => {
  const [result] = await runBoundaryScenario("silent-tail-long");
  const baseline = result.returnBaseline as { cursors: Array<{ participantId: string; previous: string | null; next: string }> };
  expect(Number(result.highWater)).toBeGreaterThan(256_000);
  expect(baseline.cursors).toContainEqual({ participantId: result.oldId as string, previous: result.previous as string, next: String(result.highWater) });
  expect(result.eventsAfterIdle).toBe(0);
  expect(result.eventsAfterVisible).toBe(1);
}, 60_000);

test("NF4 discards a pre-tail partial JSONL suffix and delivers only the later record", async () => {
  const [result] = await runBoundaryScenario("silent-tail-partial");
  expect((result.returnBaseline as { cursors: Array<{ next: string }> }).cursors.map((cursor) => cursor.next)).toContain(String(result.highWater));
  expect(result.eventsAfterIdle).toBe(0);
  expect(result.eventsAfterVisible).toBe(1);
  expect((result.visible as { captured: number }).captured).toBe(1);
}, 60_000);

test("NF4 retries the immutable verified-tail baseline after a lost acknowledgement", async () => {
  const [result] = await runBoundaryScenario("silent-tail-lost-ack");
  const retries = result.retryBaseline as Array<{ batchId: string; hash: string; cursors: Array<{ participantId: string; previous: string | null; next: string }> }>;
  expect(retries).toHaveLength(2);
  expect(retries[0]?.batchId).toBe(retries[1]?.batchId);
  expect(retries[0]?.hash).toBe(retries[1]?.hash);
  expect(retries[0]?.cursors).toEqual(retries[1]?.cursors);
  expect(retries[0]?.cursors).toContainEqual({ participantId: result.oldId as string, previous: result.previous as string, next: String(result.highWater) });
  expect(result.events).toBe(1);
}, 60_000);

test("NF5 rejects a different-path unverified candidate, then silently recovers the exact tail", async () => {
  const [result] = await runBoundaryScenario("bad-candidate-recovery");
  expect(result.rejectedBaseline).toBeUndefined();
  expect(result.absentAfterRejected).toContain(result.oldId);
  expect(result.acknowledgedAfterRejected).toBe(result.previous);
  expect(result.unavailableAfterRejected).toBe(true);
  expect(result.eventsAfterRejected).toBe(0);
  const recovery = result.recoveryBaseline as { cursors: Array<{ participantId: string; previous: string | null; next: string }> };
  expect(recovery.cursors).toContainEqual({ participantId: result.oldId as string, previous: result.previous as string, next: String(result.highWater) });
  expect(result.eventsAfterRecovery).toBe(0);
  expect(result.eventsAfterVisible).toBe(1);
}, 60_000);

test("NF6 uses the post-fragment cursor for a supported 300KB record in one poll", async () => {
  const [result] = await runBoundaryScenario("silent-fragment-300");
  expect(result.polls).toBe(1);
  const events = result.events as Array<{ status: string; truncated: boolean; originalTextBytes: number; text: string }>;
  expect(events).not.toContainEqual(expect.objectContaining({ status: "gap", text: "A malformed complete transcript record was skipped." }));
  const genuine = events.filter((event) => event.text === `${"x".repeat(19_974)}large post-fragment record`);
  expect(genuine).toHaveLength(1);
  expect(genuine[0]).toMatchObject({ status: "partial", truncated: true, originalTextBytes: 300_026 });
  expect(result.finalCursor).toBe(result.expectedCursor);
}, 60_000);

test("NF6 preserves the post-fragment cursor through bounded scans for a supported 600KB record", async () => {
  const [result] = await runBoundaryScenario("silent-fragment-600");
  expect(result.polls).toBeGreaterThan(1);
  const events = result.events as Array<{ status: string; truncated: boolean; originalTextBytes: number; text: string }>;
  expect(events).not.toContainEqual(expect.objectContaining({ status: "gap", text: "A malformed complete transcript record was skipped." }));
  const genuine = events.filter((event) => event.text === `${"x".repeat(19_974)}large post-fragment record`);
  expect(genuine).toHaveLength(1);
  expect(genuine[0]).toMatchObject({ status: "partial", truncated: true, originalTextBytes: 600_026 });
  expect(result.finalCursor).toBe(result.expectedCursor);
}, 60_000);

test("NF6 discards a same-write partial suffix before one 300KB post-fragment record", async () => {
  const [result] = await runBoundaryScenario("silent-fragment-same-write-300");
  const events = result.events as Array<{ status: string; truncated: boolean; originalTextBytes: number; text: string }>;
  expect(result.polls).toBeGreaterThanOrEqual(2);
  expect(events).not.toContainEqual(expect.objectContaining({ status: "gap", text: "A malformed complete transcript record was skipped." }));
  const genuine = events.filter((event) => event.text === `${"x".repeat(19_974)}large post-fragment record`);
  expect(genuine).toHaveLength(1);
  expect(genuine[0]).toMatchObject({ status: "partial", truncated: true, originalTextBytes: 300_026 });
  expect(result.finalCursor).toBe(result.expectedCursor);
}, 60_000);

test("NF6 preserves same-write 600KB post-fragment progress through fresh callers", async () => {
  const [result] = await runBoundaryScenario("silent-fragment-same-write-600");
  const events = result.events as Array<{ status: string; truncated: boolean; originalTextBytes: number; text: string }>;
  expect(result.polls).toBeGreaterThan(2);
  expect(events).not.toContainEqual(expect.objectContaining({ status: "gap", text: "A malformed complete transcript record was skipped." }));
  const genuine = events.filter((event) => event.text === `${"x".repeat(19_974)}large post-fragment record`);
  expect(genuine).toHaveLength(1);
  expect(genuine[0]).toMatchObject({ status: "partial", truncated: true, originalTextBytes: 600_026 });
  expect(result.finalCursor).toBe(result.expectedCursor);
}, 60_000);
