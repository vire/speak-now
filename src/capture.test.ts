import { describe, expect, test } from "bun:test";
import { captureStructured, captureTerminal, MAX_CAPTURE_TEXT_BYTES, reconcileTopology, topologyFromHerdr } from "./capture";
import type { Cursor, Participant } from "./shared";

const participant = (sessionId = "session-a"): Participant => ({ id: `local:${sessionId}:0` as Participant["id"], sourceId: "local:herdr" as Participant["sourceId"], paneId: "local:pane-a" as Participant["paneId"], rawPaneId: "pane-a", terminalId: "terminal-a", kind: "codex", sessionId, generation: 0, active: true });
const cursor = (initialized = true): Cursor => ({ participantId: participant().id, sourceCursor: "0", offset: 0, initialized });
const codex = (text: string) => `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id: "message-a", content: [{ type: "output_text", text }] } })}\n`;

describe("exact-session structured capture", () => {
  test("captures a response longer than a terminal viewport without truncation", () => {
    const text = "word ".repeat(3_000);
    const result = captureStructured("codex", "session-a", { text: codex(text), offset: 0, cursor: cursor() }, participant());
    expect(result.activities).toHaveLength(1);
    expect(result.activities[0]?.text).toBe(text);
    expect(result.activities[0]?.truncated).toBe(false);
  });

  test("uses stable Claude message identity while preserving meaningful revisions", () => {
    const claudeParticipant = { ...participant(), kind: "claude" as const };
    const record = (uuid: string, messageId: string, text: string) => `${JSON.stringify({
      type: "assistant",
      sessionId: "session-a",
      uuid,
      message: { id: messageId, role: "assistant", content: [{ type: "text", text }] },
    })}\n`;
    const first = captureStructured("claude", "session-a", {
      text: record("record-one", "message-a", "same visible content"),
      offset: 0,
      cursor: cursor(),
    }, claudeParticipant);
    const equivalent = captureStructured("claude", "session-a", {
      text: record("record-two", "message-a", "same visible content"),
      offset: first.cursor.offset,
      cursor: first.cursor,
    }, claudeParticipant);
    const changed = captureStructured("claude", "session-a", {
      text: record("record-three", "message-a", "meaningful replacement"),
      offset: equivalent.cursor.offset,
      cursor: equivalent.cursor,
    }, claudeParticipant);
    const distinct = captureStructured("claude", "session-a", {
      text: record("record-four", "message-b", "meaningful replacement"),
      offset: changed.cursor.offset,
      cursor: changed.cursor,
    }, claudeParticipant);

    expect(equivalent.activities[0]?.id).toBe(first.activities[0]?.id);
    expect(changed.activities[0]?.id).not.toBe(first.activities[0]?.id);
    expect(changed.activities[0]?.revisionOf).toBe(first.activities[0]?.revisionOf);
    expect(distinct.activities[0]?.id).not.toBe(changed.activities[0]?.id);
  });

  test("uses a UTF-8-safe tail and explicit limitation metadata beyond the capture bound", () => {
    const decisive = "DECISIVE_RESULT";
    const text = `${"prefix😀".repeat(MAX_CAPTURE_TEXT_BYTES)}${decisive}`;
    const result = captureStructured("codex", "session-a", { text: codex(text), offset: 0, cursor: cursor() }, participant());
    const activity = result.activities[0];

    expect(activity?.truncated).toBe(true);
    expect(activity?.excerpt).toBe("tail");
    expect(activity?.text.endsWith(decisive)).toBe(true);
    expect(Buffer.byteLength(activity?.text ?? "")).toBeLessThanOrEqual(MAX_CAPTURE_TEXT_BYTES);
    expect(activity?.originalTextBytes).toBeGreaterThan(MAX_CAPTURE_TEXT_BYTES);
  });

  test("does not duplicate already acknowledged bytes and retries a partial record", () => {
    const complete = codex("first update");
    const first = captureStructured("codex", "session-a", { text: complete, offset: 0, cursor: cursor() }, participant());
    const duplicate = captureStructured("codex", "session-a", { text: "", offset: first.cursor.offset, cursor: first.cursor }, participant());
    const partialLine = JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id: "message-b", content: [{ type: "output_text", text: "second update" }] } });
    const partial = captureStructured("codex", "session-a", { text: partialLine.slice(0, 30), offset: first.cursor.offset, cursor: first.cursor }, participant());
    const recovered = captureStructured("codex", "session-a", { text: partialLine + "\n", offset: partial.cursor.offset, cursor: partial.cursor }, participant());
    expect(duplicate.activities).toHaveLength(0);
    expect(partial.status).toBe("partial");
    expect(partial.cursor.offset).toBe(first.cursor.offset);
    expect(recovered.activities.map((activity) => activity.text)).toEqual(["second update"]);
  });

  test("requires an exact Codex session marker and gives replacements a new participant", () => {
    const unverified = captureStructured("codex", "session-a", { text: `${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", content: [{ type: "output_text", text: "wrong transcript" }] } })}\n`, offset: 0, cursor: cursor() }, participant());
    const topology = topologyFromHerdr("local", { agents: [
      { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-a" } },
      { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture", value: "session-b" } },
    ] });
    expect(unverified.activities).toHaveLength(0);
    expect(topology.participants.map((item) => String(item.id))).toEqual([
      'local:["session","codex","id","fixture","session-a"]:0',
      'local:["session","codex","id","fixture","session-b"]:0',
    ]);
  });

  test("revokes cached verification after a mismatching marker", () => {
    const input = `${JSON.stringify({ type: "session_meta", payload: { id: "session-b" } })}\n${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id: "eligible", content: [{ type: "output_text", text: "unrelated visible output" }] } })}\n`;
    const result = captureStructured("codex", "session-a", { text: input, offset: 100, cursor: { ...cursor(), exactSessionVerified: true } }, participant());
    expect(result.activities).toHaveLength(0);
    expect(result.cursor.exactSessionVerified).toBe(false);
  });

  test("skips malformed complete frames but retains incomplete trailing bytes", () => {
    const malformed = captureStructured("codex", "session-a", { text: `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\n{bad}\n${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id: "later", content: [{ type: "output_text", text: "later visible output" }] } })}\n`, offset: 0, cursor: cursor() }, participant());
    const partial = captureStructured("codex", "session-a", { text: "{bad", offset: malformed.cursor.offset, cursor: malformed.cursor }, participant());
    expect(malformed.activities.map((activity) => activity.text)).toContain("later visible output");
    expect(malformed.activities.some((activity) => activity.status === "gap")).toBe(true);
    expect(partial.status).toBe("partial");
    expect(partial.cursor.offset).toBe(malformed.cursor.offset);
  });

  test("skips complete non-object envelopes and reaches later visible output", () => {
    const input = `${JSON.stringify({ type: "session_meta", payload: { id: "session-a" } })}\nnull\n[]\n${JSON.stringify({ type: "response_item", payload: { type: "message", role: "assistant", phase: "commentary", id: "later", content: [{ type: "output_text", text: "after invalid envelope" }] } })}\n`;
    const result = captureStructured("codex", "session-a", { text: input, offset: 0, cursor: cursor() }, participant());
    expect(result.activities.filter((activity) => activity.status === "gap")).toHaveLength(2);
    expect(result.activities.map((activity) => activity.text)).toContain("after invalid envelope");
  });

  test("keeps empty tabs and shell panes visible while reconciling moves, replacement, and removal", () => {
    const first = topologyFromHerdr("local", {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }],
      tabs: [{ tab_id: "empty", workspace_id: "workspace", label: "Empty" }, { tab_id: "active", workspace_id: "workspace", label: "Active" }],
      panes: [{ pane_id: "pane-a", tab_id: "active", terminal_id: "shell" }],
      agents: [{ agent: "codex", pane_id: "pane-a", terminal_id: "shell", agent_session: { kind: "id", source: "herdr:codex", value: "session-a" } }],
    });
    const moved = reconcileTopology(first, topologyFromHerdr("local", {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }], tabs: [{ tab_id: "empty", workspace_id: "workspace", label: "Empty" }, { tab_id: "active", workspace_id: "workspace", label: "Active" }],
      panes: [{ pane_id: "pane-b", tab_id: "active", terminal_id: "shell" }], agents: [{ agent: "codex", pane_id: "pane-b", terminal_id: "shell", agent_session: { kind: "id", source: "herdr:codex", value: "session-a" } }],
    }));
    const replaced = reconcileTopology(moved, topologyFromHerdr("local", {
      workspaces: [{ workspace_id: "workspace", label: "Workspace" }], tabs: [{ tab_id: "empty", workspace_id: "workspace", label: "Empty" }], panes: [],
      agents: [{ agent: "codex", pane_id: "pane-b", terminal_id: "shell", agent_session: { kind: "id", source: "herdr:codex", value: "session-b" } }],
    }));
    expect(first.tabs).toHaveLength(2);
    expect(first.panes).toHaveLength(1);
    expect(String(moved.participants[0]?.id)).toBe(String(first.participants[0]?.id));
    expect(replaced.panes).toHaveLength(0);
    expect(replaced.participants[0]?.generation).toBe(1);
  });

  test("keeps identityless terminal occupants stable across moves and separates replacements", () => {
    const snapshot = (pane: string, terminal: string, kind = "codex") => topologyFromHerdr("local", {
      agents: [{ agent: kind, pane_id: pane, terminal_id: terminal }],
    });
    const initial = snapshot("pane-a", "terminal-a");
    const unchanged = reconcileTopology(initial, snapshot("pane-a", "terminal-a"));
    const moved = reconcileTopology(unchanged, snapshot("pane-b", "terminal-a"));
    const replaced = reconcileTopology(moved, snapshot("pane-b", "terminal-a", "claude"));

    expect(unchanged.participants[0]?.id).toBe(initial.participants[0]?.id);
    expect(moved.participants[0]?.id).toBe(initial.participants[0]?.id);
    expect(replaced.participants[0]?.id).not.toBe(moved.participants[0]?.id);
    expect(replaced.participants[0]?.generation).toBe(1);
  });

  test("does not merge exact references that differ only by source", () => {
    const topology = topologyFromHerdr("local", { agents: [
      { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "fixture-a", value: "same" } },
      { agent: "codex", pane_id: "pane-b", terminal_id: "terminal-b", agent_session: { kind: "id", source: "fixture-b", value: "same" } },
    ] });
    expect(new Set(topology.participants.map((item) => item.id)).size).toBe(2);
  });

  test("keeps delimiter-containing exact reference tuples as separate source identities", () => {
    const topology = topologyFromHerdr("local", { agents: [
      { agent: "codex", pane_id: "pane-a", terminal_id: "terminal-a", agent_session: { kind: "id", source: "source-a:part-b", value: "session-c" } },
      { agent: "codex", pane_id: "pane-b", terminal_id: "terminal-b", agent_session: { kind: "id", source: "source-a", value: "part-b:session-c" } },
    ] });
    expect(new Set(topology.participants.map((participant) => participant.id)).size).toBe(2);
  });

  test("terminal fallback establishes a baseline and does not replay an unchanged snapshot", () => {
    const baseline = captureTerminal("status line", cursor(false), participant());
    const unchanged = captureTerminal("status line", baseline.cursor, participant());
    const appended = captureTerminal("status line\nnew visible update", baseline.cursor, participant());
    expect(baseline.activities).toHaveLength(0);
    expect(unchanged.activities).toHaveLength(0);
    expect(appended.activities.map((activity) => activity.text)).toEqual(["new visible update"]);
  });
});
