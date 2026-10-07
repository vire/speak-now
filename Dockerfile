FROM oven/bun:1 AS dependencies
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

FROM dependencies AS build
WORKDIR /app
COPY . .
RUN bun run build

FROM oven/bun:1
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV SPEAK_NOW_DATA_DIR=/data
COPY package.json bun.lock ./
RUN bun install --production --frozen-lockfile
COPY --chown=bun:bun --from=build /app/src ./src
COPY --chown=bun:bun --from=build /app/public ./public
RUN mkdir -p /data/audio && chown -R bun:bun /data
EXPOSE 3000
USER bun
CMD ["bun", "run", "start"]
