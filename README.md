# Speak Now

A minimal full-stack starter built with Bun, TypeScript, and React. One Bun server serves the React UI and a JSON API.

## Requirements

- [Bun](https://bun.sh/) 1.3 or newer

## Run locally

```sh
bun install
bun run dev
```

Open <http://localhost:3000>. The UI calls `GET /api/hello` and displays the response. The development command rebuilds the client once when it starts and watches the server; run `bun run build:client` after editing client code to refresh the browser bundle.

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
