# Speak Now

A minimal host collector and Bun/React app for turning an existing Herdr agent update into a short spoken summary. The collector only reads Herdr state and exact agent transcript records. It never resumes, prompts, attaches to, or controls an observed session.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. The browser shows the current source, workspace, tab, and pane hierarchy and a retained text history feed. Browsing a source or channel only changes the view. **Join** explicitly selects one workspace or tab for listening; **Leave** clears listening without clearing the browsing selection. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

## Capture loop

Run the collector from the host that owns Herdr. Bun loads the Git-ignored root `.env` automatically. It contains `SUMMARY_BACKEND` and `SUMMARY_MODEL` for the local worker, plus the optional live speech configuration. On its first read the collector establishes a silent baseline. Later reads use the complete Claude or Codex reference advertised by Herdr, so a missing identity is never guessed from labels or paths.

```sh
OBSERVATION_EXCLUDE_PANES="raw:$HERDR_PANE_ID" bun src/collector.ts
```

For the SN-01 task, include both the implementation and review pane IDs in `OBSERVATION_EXCLUDE_PANES`. Use `raw:<Herdr pane ID>` for a raw ID and `qualified:<namespace>:<pane ID>` only for an already qualified ID. This keeps opaque raw IDs such as `fixture:pane-a` distinct from qualified IDs. `SUMMARY_BACKEND=claude` selects the supported isolated worker. Codex summary mode fails closed because the installed CLI cannot prove an empty built-in tool registry. Claude subprocesses run in a temporary empty directory with no session resumption, no project cwd, bounded output, empty effective tool/MCP registries, and only the small auth environment allowlist.

Exact transcript capture requires the complete Herdr reference kind, source, and value. A missing reference is not guessed. The collector records a limited read-only `recent-unwrapped` terminal fallback for that pane instead; it cannot satisfy the structured-capture gate. Topology or lookup failures remain visible in local collector state and do not remove the prior inventory.

Structured records up to 2 MB are supported with a UTF-8-safe 20 KB latest-text tail for summary evidence. Larger complete records are scanned in bounded passes, recorded as an explicit gap, and do not silently advance an unverified session. Terminal fallback evidence and its overlap cache use the same 20 KB bound.

## Remote collector connection

Run the collector on the host that owns Herdr and point it at the local app or container. Keep the host collector state directory persistent and distinct from the container's `/data` directory.

```sh
SPEAK_NOW_COLLECTOR_URL=http://127.0.0.1:3000 \
SPEAK_NOW_COLLECTOR_TOKEN='set-the-same-secret-on-server-and-host' \
SOURCE_NAMESPACE='stable-host-namespace' \
SUMMARY_WORKER_ID='stable-worker-id' \
SPEAK_NOW_DATA_DIR=/var/lib/speak-now-host \
OBSERVATION_EXCLUDE_PANES="raw:$HERDR_PANE_ID" \
bun src/collector.ts
```

Set the same nonempty `SPEAK_NOW_COLLECTOR_TOKEN` for the app server. It is required for every `/api/collector/*` route and is never exposed through browser config or diagnostics. `SOURCE_NAMESPACE` and `SUMMARY_WORKER_ID` must remain stable. `SOURCE_EPOCH` is persisted automatically: changing it is not a recovery operation. Exclude implementation and review panes with their explicit raw IDs.

Collector envelopes use finite shared limits: 20,000 UTF-8 bytes for activity text and opaque cursor tokens, 65,536 encoded bytes for a complete topology, and 524,288 encoded bytes for a request or collector response. A collector retries the exact persisted body and batch identity. A saved body beyond the request limit remains recovery-blocked: it is not sent, reset, split, or assigned a replacement identity.

Live speech needs both `ELEVENLABS_API_KEY` and `ELEVENLABS_VOICE_ID` in `.env`. Audio is written atomically to `data/audio/`. Without both values, the code reports the explicit prerequisite instead of pretending playback occurred.

The original prototype audio remains available outside joined listening. Select **Enable audio** to grant browser playback permission for that local demo. Pending or uncertain listening changes and joining a workspace or tab suspend prototype playback so a global latest clip cannot sound like scoped audio. Scoped narration, mute, volume, speed, and playback-history controls are SN-08 work. Text **Catch up** is SN-07 work. Controls shown as unavailable in this hierarchy/read milestone do not provide those functions yet.

## API

| Route | Response |
| --- | --- |
| `GET /api/health` | `{ "status": "ok" }` |
| `GET /api/hello` | `{ "message": "Hello from Speak Now!" }` |
| `GET /api/config` | Safe local speech and diagnostic configuration status |
| `POST /api/client-errors` | Same-origin JSON error report, returning `{ "reportId": "..." }` with status 202 |
| `GET /api/prototype/latest` | The latest completed local announcement, if any |
| `GET /api/prototype/audio/:key` | A completed local MP3 for a validated clip key |
| `POST /api/collector/batches` | Authenticated durable topology, activity, cursor, and receipt ingestion |
| `GET /api/collector/config?sourceId=...` | Authenticated source scope, generation, freshness, and bounded worker settings |
| `POST /api/collector/jobs/claim` | Authenticated bounded worker job claim, or 204 when no work is available |
| `POST /api/collector/jobs/:jobId/result` | Authenticated fenced durable job completion |
| `PUT /api/listening` | Same-origin selection of a known source workspace or tab |
| `GET /api/state?sourceId=...` | Read one source topology, all source metadata, confirmed listening scope, capture facts, freshness, job indicators, and global event sequence; omitting `sourceId` uses the default source |
| `GET /api/history` | Captured-context filters and bounded stable pagination for persisted summary results and announcements |
| `GET /api/events` | Same-origin durable SSE replay and live state/history events |

Set `PORT` to change the listen port (default: `3000`).

`GET /api/state?sourceId=<opaque-id>` is read-only and does not change the listening scope. A duplicate, empty, or malformed `sourceId` returns 400; an unknown source returns 404. `PUT /api/listening` accepts exactly `{ "sourceId": "...", "workspaceId": "..." }`, `{ "sourceId": "...", "tabId": "..." }`, or JSON `null` to leave. Only one scope is active. Its success response is `{ "scope": ..., "generation": ... }`.

History can be queried with `sourceId`, one of `workspaceId` or `tabId`, `participantId`, `from`, `to`, `cursor`, `limit`, and `order=asc|desc`. A workspace or tab filter requires `sourceId`. The default order is ascending; `order=desc` starts with the newest retained results. `from` and `to` are offset-aware ISO timestamps for the job creation time. The server applies stored source, participant, and captured location filters before ordering and pagination, so historical rows remain searchable after a participant moves or exits. For example:

```sh
curl 'http://localhost:3000/api/history?sourceId=source-1&tabId=tab-1&order=desc&limit=20'
curl 'http://localhost:3000/api/history?sourceId=source-1&participantId=participant-1&from=2026-10-01T00%3A00%3A00Z&to=2026-10-08T00%3A00%3A00Z&order=asc'
```

Source freshness, last retained capture facts, agent lifecycle, summary completion, and speech/playback are separate facts. An agent's presence does not establish its Working status, and a completed summary does not establish playback. Missing lifecycle and verified repository metadata remain Unknown. Capture facts expire by time even without a new event.

## Build and run

```sh
bun test ./src ./test
bun run typecheck
bun run build
bun run start
```

For a browser check against an isolated local database, run:

```sh
PORT=33106 SPEAK_NOW_DATA_DIR=tmp/sn06-local-data bun run dev
```

Open <http://localhost:33106>. Once a collector has ingested two sources, browse the second without joining, then use Join and Leave on a known channel. Use the tree with Tab, arrow keys, Enter, and Space, and check that history filters and retry actions remain keyboard reachable. Live speech credentials are not needed for hierarchy and retained-text checks. This port and data directory keep an app already listening on port 3000 untouched.

Trace files are local JSONL files under `data/traces/writer-*/`. Each process owns one unique writer directory; inspect traces by aggregating those files and correlating IDs. `maxRetainedFiles` and `maxFileBytes` apply to each live writer, including its active file and reclaimed archives, so there is no global append order or total-file budget. A live idle writer is reconciled only on its next append. The recorder uses Bun and standard filesystem APIs only. Dead generations are recovered only on the same host and PID namespace after an `ESRCH` liveness result; network filesystems and other platforms are not claimed. Legacy root JSONL migration is quiescent and idempotent: stop older writers before the first generation starts.

## Docker

```sh
docker build -t speak-now .
docker run --rm -p 3000:3000 speak-now
```

## Local error reports

Collector capture, summary worker, speech provider, and announcement publication failures produce redacted JSONL reports in `data/errors/reports.jsonl`. Records include a report ID, UTC timestamp, operation/category, bounded message/stack, and available trace/source/job identifiers. Expected caller cancellation does not create a report. Reports reuse diagnostic rotation and retention, with a default 1 MiB per file and seven-day row retention. Local reporter processes serialize writes with an exclusive destination lock; distributed filesystems and automatic recovery of an orphaned lock are outside this contract.

`POST /api/client-errors` accepts same-origin `application/json` requests up to 8 KiB. Required nonempty strings are `operation` (120 characters), `category` (80), and `message` (1,024). Optional fields are `stack` (2,048), source/participant/job IDs (128 each), a complete hexadecimal trace/span pair (32/16), and a flat `context` object. Stored context is limited to scalar `activityId`, `attempt`, `backend`, `captureMode`, `clipKey`, `operationId`, `originalTextBytes`, `retry`, and `status` values, with strings bounded to 128 characters. Arbitrary payloads and nested context are omitted. Browser event wiring is reserved for the later UI task.

Oversized streamed requests receive 413 before body completion, with the connection closed and no report created. A failed report destination uses bounded redacted stderr diagnostics and exposes its status through `/api/config`; reporting failure does not recursively report itself. Repairing the destination permits subsequent writes to restore available status.
