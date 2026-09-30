# Speak Now

A minimal full-stack starter built with Bun, TypeScript, and React. One Bun server serves the React UI and a JSON API.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. The Play button chooses a random sample agent update and generates fresh audio through ElevenLabs on each click. Set `ELEVENLABS_API_KEY` and `ELEVENLABS_VOICE_ID` in the server environment. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

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

`POST /api/demo/speech` chooses one of the sample messages in `src/demo.ts` and calls ElevenLabs server-side. The API key stays in the server environment. The response contains an MP3 and its text in `X-Demo-Message`; the browser displays the message and plays the audio with native playback controls. Generation is limited to 20 requests per minute per server process. Configure the speech credentials as runtime-only secrets in production. Set `SPEAK_NOW_PUBLIC_ORIGIN` to the public HTTPS origin when deploying behind a reverse proxy.
