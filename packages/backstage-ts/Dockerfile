# Backstage - Multi-stage Dockerfile with Bun
# Optimized for Orbstack/Docker environments

FROM oven/bun:1 AS base
COPY . /app
WORKDIR /app

# Build stage
FROM base AS build
RUN bun install --frozen-lockfile
RUN bun run build

# Production stage
FROM base AS prod
RUN bun install --production --frozen-lockfile
COPY --from=build /app/dist /app/dist

# Install Tini to handle SIGTERM correctly for graceful shutdowns
RUN apt-get update && apt-get install -y tini && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/bin/tini", "--"]

# Default command starts the worker pool
CMD ["bun", "run", "dist/cli.js", "run-workers"]
