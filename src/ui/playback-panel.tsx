import type { PlaybackCandidate, PlaybackSettings as ApiPlaybackSettings } from "./api";
import { PlaybackControls, type PlaybackItemView } from "./playback-controls";
import type { PlaybackSettings } from "./playback";

type Props = {
  scope: { sourceId: string; workspaceId?: string; tabId?: string; generation: number } | null;
  scopeStatus: "idle" | "pending" | "confirmed" | "failed" | "unresolved";
  settings: ApiPlaybackSettings;
  participants: readonly string[];
  candidates: readonly PlaybackCandidate[];
  onSettings(settings: PlaybackSettings): void;
  onReplay(itemId: string): void;
};

const ownerSettings = (settings: ApiPlaybackSettings, sourceId: string, participantIds: readonly string[]): PlaybackSettings => ({
  master: { muted: settings.master.muted, volume: settings.master.volume, rate: settings.master.speed },
  participants: Object.fromEntries(participantIds.map((participantId) => {
    const participant = settings.participants.find((item) => item.sourceId === sourceId && item.participantId === participantId);
    return [participantId, { muted: participant?.muted ?? false, volume: participant?.volume ?? 1 }];
  })),
});

const playbackItems = (candidates: readonly PlaybackCandidate[]): PlaybackItemView[] => candidates.flatMap((candidate) => {
  if (!candidate.playback) return [];
  const status = candidate.media.state === "pending" ? "pending" : candidate.media.state === "preparing" ? "preparing" : candidate.media.state === "failed" ? "media_failed" : candidate.playback.status;
  return [{ id: candidate.itemId, participantId: candidate.participantId, kind: candidate.kind, status, replayable: candidate.media.state === "ready" }];
});

export function PlaybackPanel({ scope, scopeStatus, settings, participants, candidates, onSettings, onReplay }: Props) {
  return <PlaybackControls scope={scope} scopeStatus={scopeStatus} settings={ownerSettings(settings, scope?.sourceId ?? "", participants)} participants={participants} items={playbackItems(candidates)} onSettings={onSettings} onReplay={(item) => onReplay(item.id)} />;
}
