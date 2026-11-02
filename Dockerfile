# syntax=docker/dockerfile:1

# ==============================================================================
# Stage 1: Build the Quire binary
# ==============================================================================
FROM rust:1.97-slim-bookworm AS builder

WORKDIR /usr/src/quire

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY templates ./templates
COPY static ./static
COPY src ./src

RUN cargo build --release --locked --bin quire

# ==============================================================================
# Stage 2: Minimal non-root runtime image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 quire \
    && useradd --uid 10001 --gid 10001 --home-dir /app --create-home --shell /usr/sbin/nologin quire \
    && mkdir -p /data \
    && chown -R quire:quire /app /data

WORKDIR /app

COPY --from=builder --chown=quire:quire /usr/src/quire/target/release/quire /app/quire
COPY --chown=quire:quire static /app/static

USER quire:quire

ENV QUIRE_HOST=0.0.0.0 \
    QUIRE_PORT=8080 \
    QUIRE_DATA_DIR=/data \
    QUIRE_DATABASE_URL=sqlite:///data/quire.db \
    QUIRE_LOG_LEVEL=info \
    QUIRE_TIMEZONE=America/Los_Angeles

VOLUME ["/data"]

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/app/quire", "healthcheck"]

ENTRYPOINT ["/app/quire"]
