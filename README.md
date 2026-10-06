# Speak Now

A minimal host collector and Bun/React app for turning an existing Herdr agent update into a short spoken summary. The collector only reads Herdr state and exact agent transcript records. It never resumes, prompts, attaches to, or controls an observed session.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. The UI calls `GET /api/hello` and displays the response. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

## Capture loop

Run the collector from the host that owns Herdr. Bun loads the Git-ignored root `.env` automatically. It contains `SUMMARY_BACKEND` and `SUMMARY_MODEL` for the local worker, plus the optional live speech configuration. On its first read the collector establishes a silent baseline. Later reads use the complete Claude or Codex reference advertised by Herdr, so a missing identity is never guessed from labels or paths.

```sh
OBSERVATION_EXCLUDE_PANES="raw:$HERDR_PANE_ID" bun src/collector.ts
```

For the SN-01 task, include both the implementation and review pane IDs in `OBSERVATION_EXCLUDE_PANES`. Use `raw:<Herdr pane ID>` for a raw ID and `qualified:<namespace>:<pane ID>` only for an already qualified ID. This keeps opaque raw IDs such as `fixture:pane-a` distinct from qualified IDs. `SUMMARY_BACKEND=claude` selects the supported isolated worker. Codex summary mode fails closed because the installed CLI cannot prove an empty built-in tool registry. Claude subprocesses run in a temporary empty directory with no session resumption, no project cwd, bounded output, empty effective tool/MCP registries, and only the small auth environment allowlist.

Exact transcript capture requires the complete Herdr reference kind, source, and value. A missing reference is not guessed. The collector records a limited read-only `recent-unwrapped` terminal fallback for that pane instead; it cannot satisfy the structured-capture gate. Topology or lookup failures remain visible in local collector state and do not remove the prior inventory.

Structured records up to 2 MB are supported with a UTF-8-safe 20 KB latest-text tail for summary evidence. Larger complete records are scanned in bounded passes, recorded as an explicit gap, and do not silently advance an unverified session. Terminal fallback evidence and its overlap cache use the same 20 KB bound.

Live speech needs both `ELEVENLABS_API_KEY` and `ELEVENLABS_VOICE_ID` in `.env`. Audio is written atomically to `data/audio/`. Without both values, the code reports the explicit prerequisite instead of pretending playback occurred.

Open the app and select **Enable audio** once to grant browser playback permission. The page polls only the local prototype announcement endpoint and plays a completed saved MP3 once per clip key.

## API

| Route | Response |
| --- | --- |
| `GET /api/health` | `{ "status": "ok" }` |
| `GET /api/hello` | `{ "message": "Hello from Speak Now!" }` |
| `GET /api/config` | Local collector command, live speech configuration, and diagnostic/report destination status |
| `POST /api/client-errors` | Same-origin JSON error report, returning `{ "reportId": "..." }` with status 202 |
| `GET /api/prototype/latest` | The latest completed local announcement, if any |
| `GET /api/prototype/audio/:key` | A completed local MP3 for a validated clip key |

Set `PORT` to change the listen port (default: `3000`).

## Build and run

```sh
bun run typecheck
bun run build
bun run start
```

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
