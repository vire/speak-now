import type { PlaybackSettings } from "./playback";

export type PlaybackItemView = {
  id: string;
  participantId: string;
  kind: "announcement" | "recap";
  status: "unattempted" | "prepared" | "started" | "pending" | "preparing" | "media_failed" | "heard" | "failed" | "stopped" | "skipped" | "blocked";
  replayable: boolean;
};

type Props = {
  scope: { sourceId: string; workspaceId?: string; tabId?: string; generation: number } | null;
  scopeStatus: "idle" | "pending" | "confirmed" | "failed" | "unresolved";
  settings: PlaybackSettings;
  participants: readonly string[];
  items: readonly PlaybackItemView[];
  onSettings(settings: PlaybackSettings): void;
  onReplay(item: PlaybackItemView): void;
};

const statusText = (status: PlaybackItemView["status"]) => ({ unattempted: "Not played yet.", prepared: "Ready to play.", started: "Playback started.", pending: "Speech is pending.", preparing: "Preparing speech.", media_failed: "Speech generation failed.", heard: "Heard", failed: "Speech failed.", stopped: "Playback stopped.", skipped: "Playback was skipped.", blocked: "Playback is blocked." })[status];

export function PlaybackControls({ scope, scopeStatus, settings, participants, items, onSettings, onReplay }: Props) {
  const setMaster = (changes: Partial<PlaybackSettings["master"]>) => onSettings({ ...settings, master: { ...settings.master, ...changes } });
  const setParticipant = (participantId: string, changes: Partial<PlaybackSettings["participants"][string]>) => {
    const current = settings.participants[participantId];
    onSettings({ ...settings, participants: { ...settings.participants, [participantId]: { muted: changes.muted ?? current?.muted ?? false, volume: changes.volume ?? current?.volume ?? 1 } } });
  };
  const replayAllowed = Boolean(scope) && scopeStatus === "confirmed";
  return <section aria-label="Playback controls">
    <h2>Playback controls</h2>
    <label>Master mute <input aria-label="Master mute" type="checkbox" checked={settings.master.muted} onChange={(event) => setMaster({ muted: event.target.checked })} /></label>
    <label>Master volume <input aria-label="Master volume" type="range" min="0" max="1" step="0.05" value={settings.master.volume} onChange={(event) => setMaster({ volume: Number(event.target.value) })} /></label>
    <label>Speech speed <input aria-label="Speech speed" type="number" min="0.5" max="2" step="0.05" value={settings.master.rate} onChange={(event) => { const rate = Number(event.target.value); if (Number.isFinite(rate) && rate >= 0.5 && rate <= 2) setMaster({ rate }); }} /></label>
    <p role="status">{replayAllowed ? "Playback is scoped to the joined session." : "Join a workspace or tab to replay audio."}</p>
    <h3>Participants</h3>
    <ul>{participants.map((participantId) => {
      const participant = settings.participants[participantId] ?? { muted: false, volume: 1 };
      return <li key={participantId}>
        <strong>{participantId}</strong>
        <label>{participantId} mute <input aria-label={`${participantId} mute`} type="checkbox" checked={participant.muted} onChange={(event) => setParticipant(participantId, { muted: event.target.checked })} /></label>
        <label>{participantId} volume <input aria-label={`${participantId} volume`} type="range" min="0" max="1" step="0.05" value={participant.volume} onChange={(event) => setParticipant(participantId, { volume: Number(event.target.value) })} /></label>
      </li>;
    })}</ul>
    <h3>Playback history</h3>
    <ul>{items.map((item) => <li key={`${item.kind}-${item.id}`}>
      <strong>{item.kind === "recap" ? "Recap" : "Announcement"}</strong> for {item.participantId}. {statusText(item.status)}
      {item.replayable ? <button type="button" disabled={!replayAllowed} onClick={() => onReplay(item)}>Replay {item.kind}</button> : <span> Audio is unavailable.</span>}
      {item.replayable && !replayAllowed && <p>Join this item’s scope to replay.</p>}
    </li>)}</ul>
  </section>;
}
