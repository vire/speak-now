# Speak Now

A minimal full-stack starter built with Bun, TypeScript, and React. One Bun server serves the React UI and a JSON API.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. The UI displays two hardcoded sample agent updates with Play buttons. Each button plays a bundled MP3 generated with the SN-01 speech provider. The public demo needs no Herdr connection or production speech credentials. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

## API

| Route | Response |
| --- | --- |
| `GET /api/health` | `{ "status": "ok" }` |
| `GET /api/hello` | `{ "message": "Hello from Speak Now!" }` |

Set `PORT` to change the listen port (default: `3000`).

## Build and run

```sh
bun run typecheck
bun run build
bun run start
```

## Docker

```sh
docker build -t speak-now .
docker run --rm -p 3000:3000 speak-now
```

## Public playback demo

Demo messages are defined in `src/demo.ts`; their matching audio files live in `public/demo/`. Update both together. These MP3s are intentional public demo assets, separate from private captured transcripts and runtime audio under `data/`. Playback begins on a button click, stops the previous clip, and supports replay after completion.
