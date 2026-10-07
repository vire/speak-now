import { createHash } from "node:crypto";
import type { Activity, AgentKind, CaptureStatus, Cursor, Pane, Participant, Source, Tab, Topology, Workspace } from "./shared";
import { COLLECTOR_WIRE_LIMITS, opaqueId } from "./shared";

export const MAX_CAPTURE_TEXT_BYTES = COLLECTOR_WIRE_LIMITS.activityTextBytes;
type Json = Record<string, unknown>;
const hash = (value: string) => createHash("sha256").update(value).digest("hex");

export interface TranscriptRead { text: string; offset: number; cursor: Cursor; }
export interface CaptureResult { activities: Activity[]; cursor: Cursor; status: CaptureStatus; verificationFingerprint?: string; }

function sessionId(record: Json): string | undefined {
  const payload = record.payload as Json | undefined;
  if (typeof record.sessionId === "string") return record.sessionId;
  if (typeof record.session_id === "string") return record.session_id;
  return record.type === "session_meta" && typeof payload?.id === "string" ? payload.id : undefined;
}

function textBlocks(value: unknown): string {
  if (!Array.isArray(value)) return "";
  return value.filter((item): item is Json => Boolean(item) && typeof item === "object" && ((item as Json).type === "text" || (item as Json).type === "output_text")).map((item) => String(item.text ?? "")).join("\n");
}

function visibleMessage(kind: AgentKind, record: Json): { text: string; stableId: string } | undefined {
  const payload = record.payload as Json | undefined;
  if (kind === "claude" && record.type === "assistant") {
    const message = record.message as Json | undefined;
    const text = textBlocks(message?.content);
    const messageId = typeof message?.id === "string" ? message.id : String(record.uuid ?? hash(JSON.stringify(record)));
    return text ? { text, stableId: `claude:${messageId}` } : undefined;
  }
  if (kind === "codex" && record.type === "response_item" && payload?.type === "message" && payload.role === "assistant" && ["commentary", "final_answer", "final"].includes(String(payload.phase))) {
    const text = textBlocks(payload.content);
    return text ? { text, stableId: `codex:${String(payload.id ?? record.ordinal ?? hash(JSON.stringify(record)))}` } : undefined;
  }
}

function tailUtf8(text: string, limit: number): { text: string; truncated: boolean; originalTextBytes: number } {
  const bytes = Buffer.from(text);
  if (bytes.byteLength <= limit) return { text, truncated: false, originalTextBytes: bytes.byteLength };
  let start = bytes.byteLength - limit;
  while (start < bytes.byteLength && (bytes[start] & 0b1100_0000) === 0b1000_0000) start += 1;
  return { text: bytes.subarray(start).toString("utf8"), truncated: true, originalTextBytes: bytes.byteLength };
}

export function captureStructured(kind: AgentKind, expectedSessionId: string, input: TranscriptRead, participant: Participant): CaptureResult {
  let offset = input.offset;
  let verified = input.cursor.exactSessionVerified === true;
  let status: CaptureStatus = "complete";
  let verificationFingerprint: string | undefined;
  const activities: Activity[] = [];
  for (const line of input.text.split(/(?<=\n)/)) {
    const start = offset;
    const bytes = Buffer.byteLength(line);
    if (!line.trim()) { offset += bytes; continue; }
    let record: Json;
    try {
      const decoded = JSON.parse(line) as unknown;
      if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) throw new Error("invalid transcript envelope");
      record = decoded as Json;
    } catch {
      if (!line.endsWith("\n")) { status = "partial"; break; }
      offset += bytes;
      status = "gap";
      const text = "A malformed complete transcript record was skipped.";
      activities.push({ id: hash(`${participant.id}\0malformed\0${start}`), participantId: participant.id, sourceCursor: String(start), observedAt: new Date().toISOString(), kind: "unknown", text, captureMode: "structured", status: "gap", truncated: false, excerpt: "full", originalTextBytes: Buffer.byteLength(text) });
      continue;
    }
    const recordId = sessionId(record);
    if (recordId === expectedSessionId) {
      verified = true;
      verificationFingerprint ??= hash(line);
    }
    offset += bytes;
    if (recordId && recordId !== expectedSessionId) { verified = false; continue; }
    if (!verified || (kind === "claude" && !recordId)) continue;
    const message = visibleMessage(kind, record);
    if (!message) continue;
    const excerpt = tailUtf8(message.text, MAX_CAPTURE_TEXT_BYTES);
    const revisionId = hash(message.text);
    const revisionOf = hash(`${participant.id}\0${message.stableId}`);
    const id = hash(`${revisionOf}\0${revisionId}`);
    const { text, truncated, originalTextBytes } = excerpt;
    if (truncated) status = "partial";
    activities.push({ id, stableMessageId: message.stableId, revisionId, revisionOf, participantId: participant.id, sourceCursor: String(start), observedAt: new Date().toISOString(), kind: "assistant", text, captureMode: "structured", status: truncated ? "partial" : "complete", truncated, excerpt: truncated ? "tail" : "full", originalTextBytes });
  }
  return {
    activities,
    status,
    verificationFingerprint,
    cursor: { ...input.cursor, offset, sourceCursor: String(offset), initialized: true, exactSessionVerified: verified },
  };
}

export function captureTerminal(snapshot: string, cursor: Cursor, participant: Participant): CaptureResult {
  const lines = snapshot.replace(/\x1b\[[0-?]*[ -/]*[@-~]/g, "").split("\n").filter((line) => line.trim() && !/^\s*(?:\$|>|›|⠋|thinking|tokens used)/i.test(line));
  const previous = cursor.sourceCursor ? cursor.sourceCursor.split("\n") : [];
  const previousHashes = cursor.terminalLineHashes ?? previous.map(hash);
  const hashes = lines.map(hash);
  let overlap = 0;
  for (let count = Math.min(previousHashes.length, hashes.length); count > 0; count--) {
    if (previousHashes.slice(-count).every((value, index) => value === hashes[index])) {
      overlap = count;
      break;
    }
  }
  const fresh = lines.slice(overlap).join("\n");
  const retained = tailUtf8(lines.slice(-500).join("\n"), MAX_CAPTURE_TEXT_BYTES);
  const next = { ...cursor, initialized: true, offset: cursor.offset + 1, sourceCursor: retained.text, terminalLineHashes: hashes.slice(-500) };
  if (!cursor.initialized) return { activities: [], status: "limited", cursor: next };
  const excerpt = tailUtf8(fresh, MAX_CAPTURE_TEXT_BYTES);
  const status: CaptureStatus = excerpt.truncated || retained.truncated || overlap ? "limited" : "gap";
  return {
    activities: fresh ? [{
      id: hash(`${participant.id}\0${next.offset}\0${fresh}`),
      participantId: participant.id,
      sourceCursor: String(next.offset),
      observedAt: new Date().toISOString(),
      kind: "assistant",
      text: excerpt.text,
      captureMode: "terminal",
      status,
      truncated: excerpt.truncated,
      excerpt: excerpt.truncated ? "tail" : "full",
      originalTextBytes: excerpt.originalTextBytes,
    }] : [],
    status,
    cursor: next,
  };
}

interface Snapshot { agents?: Json[]; panes?: Json[]; workspaces?: Json[]; tabs?: Json[]; }
export function participantIdentity(participant: Pick<Participant, "kind" | "sessionId" | "sessionReferenceKind" | "sessionReferenceSource" | "terminalId">): string {
  if (participant.sessionId) {
    return JSON.stringify([
      "session",
      participant.kind,
      participant.sessionReferenceKind ?? "unknown",
      participant.sessionReferenceSource ?? "unknown",
      participant.sessionId,
    ]);
  }
  return JSON.stringify(["terminal", participant.kind, participant.terminalId]);
}

export function topologyFromHerdr(namespace: string, snapshot: Snapshot, observedAt = new Date().toISOString()): Topology {
  const sourceId = opaqueId<Source["id"]>(namespace, "herdr");
  const source: Source = { id: sourceId, namespace, stale: false, observedAt };
  const workspaces: Workspace[] = (snapshot.workspaces ?? []).map((item, order) => ({ id: opaqueId(namespace, String(item.workspace_id)), sourceId, label: String(item.label ?? "Workspace"), order, live: true }));
  const tabs: Tab[] = (snapshot.tabs ?? []).map((item, order) => ({ id: opaqueId(namespace, String(item.tab_id)), workspaceId: opaqueId(namespace, String(item.workspace_id)), label: String(item.label ?? "Tab"), order }));
  const panes: Pane[] = (snapshot.panes ?? []).map((item) => ({ id: opaqueId(namespace, String(item.pane_id)), tabId: opaqueId(namespace, String(item.tab_id)), terminalId: String(item.terminal_id ?? ""), cwd: typeof item.cwd === "string" ? item.cwd : undefined }));
  const participants: Participant[] = (snapshot.agents ?? []).map((item) => {
    const ref = item.agent_session as Json | undefined;
    const kind: Participant["kind"] = item.agent === "claude" || item.agent === "codex" ? item.agent : "unsupported";
    const sessionReferenceKind = typeof ref?.kind === "string" ? ref.kind : undefined;
    const sessionReferenceSource = typeof ref?.source === "string" ? ref.source : undefined;
    const sessionId = typeof ref?.value === "string" && sessionReferenceKind && sessionReferenceSource ? ref.value : undefined;
    const rawPaneId = String(item.pane_id);
    const terminalId = String(item.terminal_id ?? rawPaneId);
    const participant = { kind, sessionId, sessionReferenceKind, sessionReferenceSource, terminalId };
    const identity = participantIdentity(participant);
    return { id: opaqueId(namespace, `${identity}:0`), sourceId, paneId: opaqueId(namespace, rawPaneId), rawPaneId, ...participant, generation: 0, active: true };
  });
  return { source, workspaces, tabs, panes, participants };
}
function retainedGeneration(namespace: string, participant: Participant, retainedIds: Iterable<string>): number | undefined {
  const identity = participantIdentity(participant);
  const prefix = `${namespace}:${identity}:`;
  const generations = new Set<number>();
  for (const id of retainedIds) {
    if (!id.startsWith(prefix)) continue;
    const suffix = id.slice(prefix.length);
    if (!/^(0|[1-9]\d*)$/.test(suffix)) continue;
    const generation = Number(suffix);
    if (!Number.isSafeInteger(generation) || `${prefix}${generation}` !== id) continue;
    generations.add(generation);
  }
  if (generations.size > 1) throw new Error("Ambiguous retained collector participant identity");
  return generations.values().next().value;
}

export function reconcileTopology(previous: Topology, next: Topology, retainedIds: Iterable<string> = []): Topology {
  return {
    ...next,
    participants: next.participants.map((participant) => {
      const identity = participantIdentity(participant);
      const exact = previous.participants.find((item) => participantIdentity(item) === identity);
      const samePane = previous.participants.find((item) => item.rawPaneId === participant.rawPaneId);
      const retained = exact ? undefined : retainedGeneration(next.source.namespace, participant, retainedIds);
      const generation = exact?.generation ?? retained ?? (samePane ? samePane.generation + 1 : 0);
      return { ...participant, generation, id: opaqueId(next.source.namespace, `${identity}:${generation}`) };
    }),
  };
}
