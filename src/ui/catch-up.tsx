import type { BrowserSessionSnapshot } from "./session";

export function CatchUpView({ snapshot, onCatchUp }: { snapshot: BrowserSessionSnapshot; onCatchUp: () => void }) {
  const available = Boolean(snapshot.confirmedScope) && snapshot.scopeStatus === "confirmed";
  return <section aria-label="Catch up">
    <button type="button" onClick={onCatchUp} disabled={!available || snapshot.catchUpPending}>Catch up</button>
    <div role="status" aria-live="polite">
      {!available && <p>Catch up is unavailable until listening is confirmed.</p>}
      {snapshot.catchUp && <>Catch up: {snapshot.catchUp.status}.
        {snapshot.catchUp.status === "unavailable" && snapshot.catchUp.entries.length === 0 && <p>No retained activity is available for this scope.</p>}
        {snapshot.catchUp.entries.map((entry) => <p key={`${entry.participantId}-${entry.timestamp}`}>
          <strong>{entry.participantId}</strong> <time>{entry.timestamp}</time>.
          {entry.location.workspaceId && <> Workspace {entry.location.workspaceId}.</>}
            {entry.location.tabId && <> Tab {entry.location.tabId}.</>}
            {" "}{entry.text ?? "No recap is available."} {entry.reason}
            {entry.evidenceRefs.length ? ` Evidence: ${entry.evidenceRefs.join(", ")}.` : ""}
          </p>)}
      </>}
    </div>
  </section>;
}
