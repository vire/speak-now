# Speak Now

A minimal full-stack starter built with Bun, TypeScript, and React. One Bun server serves the React UI and a JSON API.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. Choose Development, Code Review, Deployments, or Needs Your Input. Each room contains three sample messages. The Play button chooses a random update from the selected room and plays its bundled MP3. No speech API or credentials are needed at runtime. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

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

The four rooms and their twelve messages are defined in `src/demo.ts`. Each message has a matching bundled MP3 at `public/audio/<room-id>-<message-number>.mp3`. These files are intentional public demo assets. Play randomly selects a message within the chosen room and plays its MP3, stopping any previous playback. Repeated selections restart the clip. The server exposes only the twelve known audio routes; there is no runtime speech-generation endpoint or ElevenLabs dependency.
