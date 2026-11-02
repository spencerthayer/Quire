# Quire

**Quire** is an open-source, self-hosted RSS/Atom aggregation, full-text article extraction, AI curation, and automated publication platform written in Rust. It runs as a single lightweight container on a home server or Portainer stack, persisting all data and job queues in SQLite without external cloud infrastructure.

---

## Project Overview

Quire turns RSS and Atom feeds into structured, readable publications for e-readers or text-to-speech listening in **ElevenReader**.

### Implementation Status

Quire is developed in incremental phases (see [`docs/roadmap.md`](docs/roadmap.md)):

| Capability | Status | Phase |
|---|---|---|
| Rust HTTP server (`axum`), structured logging, graceful shutdown | **Implemented** | Phase 1 |
| SQLite database (`WAL` mode, foreign keys, automatic migrations) | **Implemented** | Phase 1 |
| Browser interface (`Dashboard`, `Feeds`, `Articles`, `Editions`, `Settings`) | **Implemented** | Phase 1 |
| Feed subscription CRUD (add, edit, enable/disable, delete, SSRF URL validation) | **Implemented** | Phase 1 |
| Three-tier configuration (`env` > SQLite `settings` > defaults) & CSRF protection | **Implemented** | Phase 1 |
| Health check endpoint (`GET /health`) & CLI probe (`quire healthcheck`) | **Implemented** | Phase 1 |
| Multi-stage Docker image, Docker Compose, Portainer stack & GHCR CI/CD | **Implemented** | Phase 1 |
| Asynchronous RSS 2.0, RSS 1.0/RDF & Atom 1.0 fetching (`reqwest` + `feed-rs`) | **Implemented** | Phase 2 |
| Conditional HTTP requests (`ETag` / `Last-Modified` / `304`), retries & backoff | **Implemented** | Phase 2 |
| Canonical URL normalization & cross-feed article deduplication | **Implemented** | Phase 2 |
| Persistent SQLite extraction queue (`article_extraction_jobs`) & crash recovery | **Implemented** | Phase 2 |
| Full-text readability extraction (`dom_smoothie`), HTML sanitization & summary fallback | **Implemented** | Phase 2 |
| Background feed scheduler, manual "Fetch Now", fetch history & article reader UI | **Implemented** | Phase 2 |
| Article library full-text search (FTS5), categories, and retention policies | Planned | Phase 3 |
| AI relevance scoring, classification, and editorial briefings (Ollama / OpenAI) | Planned | Phase 4 |
| Scheduled daily/weekly edition composition and EPUB / TXT export | Planned | Phase 5 |
| Automated ElevenReader publication delivery | Planned | Phase 6 |
| Multi-profile schedules, per-source rules, and advanced automation | Planned | Phase 7 |

> **Note on Current Scope**: Phase 1 and Phase 2 are complete. Quire can subscribe to RSS/Atom feeds, poll them automatically or manually, deduplicate entries across feeds, extract and sanitize full article content (with summary fallback), and display articles and fetch diagnostics in the browser. AI summarization (Phase 4), EPUB/TXT edition generation (Phase 5), and ElevenReader delivery (Phase 6) are not yet active.

> **Note on ElevenReader Integration**: ElevenReader does not currently publish an official public REST API for third-party document uploads. Quire's planned Phase 6 integration will rely on an experimental, undocumented Firebase token refresh and upload workflow unless an official ElevenLabs/ElevenReader API becomes available.

> **Security Notice**: Quire does not include built-in multiuser login authentication in Phase 2. Deploy Quire only on a trusted private homelab network or behind an authenticating reverse proxy (such as Authelia, Authentik, Pangolin, or Cloudflare Access). Do not expose an unauthenticated Quire instance directly to the public internet.

---

## How Ingestion & Extraction Work

1. **Adding a Feed**: Navigate to `/feeds` and submit the **Add Feed** form with a feed name, RSS/Atom URL, and optional per-feed polling interval or entry limit override.
2. **Manual Fetching**: Click **Fetch Now** on any feed row (or on `/feeds/{id}`), or click **Fetch All Enabled** at the top of `/feeds`. The request validates CSRF, claims the feed in SQLite (`fetch_in_progress = 1`), spawns the background fetch immediately, and redirects without blocking the browser.
3. **Automatic Scheduling**: A Tokio background loop (`IngestionCoordinator`) wakes every 30 seconds, checks whether automatic fetching is enabled, claims up to `QUIRE_FETCH_CONCURRENCY` due feeds (`next_scheduled_fetch_at <= now`), and executes them concurrently.
4. **Deduplication**: Discovered entries have their links resolved and canonicalized (stripping fragments and tracking parameters like `utm_*` and `fbclid`). Articles are deduplicated by `canonical_url` globally and tracked per feed in `article_feed_sources`. Deleting one feed preserves articles that were also discovered by another feed.
5. **Persistent Extraction Queue**: Newly discovered or updated articles atomically enqueue a job in `article_extraction_jobs` inside the same SQLite transaction. Bounded workers claim jobs with `BEGIN IMMEDIATE`, fetch the article HTML, run `dom_smoothie` readability extraction, sanitize the HTML with `ammonia`, compute a SHA-256 `content_hash`, and store the result. If full-text extraction fails, Quire falls back to the sanitized feed summary (`fallback` status) when available.
6. **Inspecting Articles & Retrying Extraction**: Browse `/articles` (with filters for feed, extraction status, published date, and discovered date) and open `/articles/{id}` to read the sanitized article body or click **Retry Extraction / Refresh Content**.

### Troubleshooting Ingestion Failures

* Visit `/feeds/{id}` to inspect the **Recent Fetch History** table (`HTTP` status code, duration in milliseconds, error category, and message).
* Visit `/` (**Dashboard**) to see **Recent Ingestion Failures** across all feeds and current extraction queue counts.
* If a feed returns `ssrf_blocked`, verify that the feed URL and its redirects point to public internet addresses rather than private LAN or loopback IPs.

---

## Configuration Reference

Configuration is resolved in strict priority order: **Environment variables (`QUIRE_*`)** > **SQLite `settings` table (`/settings` UI)** > **Compiled defaults**.

| Variable | Default | Phase Status | Description |
|---|---|---|---|
| `QUIRE_HOST` | `0.0.0.0` | Active (Phase 1) | Bind address for the HTTP server |
| `QUIRE_PORT` | `8080` | Active (Phase 1) | HTTP listen port |
| `QUIRE_LOG_LEVEL` | `info` | Active (Phase 1) | Log filter (`error`, `warn`, `info`, `debug`, `trace`) |
| `QUIRE_DATA_DIR` | `/data` (or `./data` locally) | Active (Phase 1) | Directory for persistent SQLite data and generated files |
| `QUIRE_DATABASE_URL` | `sqlite:///data/quire.db` | Active (Phase 1) | SQLite connection URL |
| `QUIRE_TIMEZONE` | `America/Los_Angeles` | Active (Phase 1) | IANA timezone (locks UI setting when set via env) |
| `QUIRE_FETCH_ENABLED` | `true` | Active (Phase 2) | Enable or pause automatic scheduled feed polling |
| `QUIRE_FETCH_INTERVAL_MINUTES` | `60` | Active (Phase 2) | Default polling interval in minutes (`5..=10080`) |
| `QUIRE_FETCH_MIN_INTERVAL_MINUTES` | `5` | Active (Phase 2) | Minimum allowed polling interval in minutes |
| `QUIRE_FETCH_CONCURRENCY` | `4` | Active (Phase 2) | Maximum concurrent feed HTTP requests (`1..=32`) |
| `QUIRE_EXTRACTION_CONCURRENCY` | `4` | Active (Phase 2) | Maximum concurrent article extractions (`1..=32`) |
| `QUIRE_FEED_REQUEST_TIMEOUT_SECONDS` | `30` | Active (Phase 2) | Timeout in seconds for feed HTTP requests (`5..=300`) |
| `QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS` | `45` | Active (Phase 2) | Timeout in seconds for article HTML requests (`5..=300`) |
| `QUIRE_MAX_FEED_RESPONSE_BYTES` | `10485760` (10 MiB) | Active (Phase 2) | Maximum streaming response size for feeds |
| `QUIRE_MAX_ARTICLE_RESPONSE_BYTES` | `15728640` (15 MiB) | Active (Phase 2) | Maximum streaming response size for articles |
| `QUIRE_MAX_FEED_ENTRIES` | `200` | Active (Phase 2) | Maximum entries processed per feed fetch (`1..=5000`) |
| `QUIRE_FETCH_MAX_ATTEMPTS` | `3` | Active (Phase 2) | Bounded retry attempts for transient HTTP errors (`1..=10`) |
| `QUIRE_FETCH_RETRY_INITIAL_SECONDS` | `2` | Active (Phase 2) | Initial retry backoff in seconds |
| `QUIRE_FETCH_RETRY_MAX_SECONDS` | `30` | Active (Phase 2) | Maximum retry backoff in seconds |
| `QUIRE_AI_PROVIDER` | *(empty)* | Reserved (Phase 4) | LLM provider (`ollama`, `openai`, `openrouter`) |
| `QUIRE_AI_BASE_URL` | *(empty)* | Reserved (Phase 4) | Base URL for OpenAI-compatible or Ollama server |
| `QUIRE_AI_MODEL` | *(empty)* | Reserved (Phase 4) | Model identifier for scoring and summarization |
| `QUIRE_AI_API_KEY` | *(empty)* | Reserved (Phase 4) | Secret API key (environment only; never displayed) |
| `QUIRE_ELEVENREADER_REFRESH_TOKEN` | *(empty)* | Reserved (Phase 6) | Secret Firebase refresh token (environment only; never displayed) |

---

## Local Development & Testing

```bash
# Run locally with SQLite stored in ./data/quire.db
QUIRE_DATA_DIR=./data QUIRE_DATABASE_URL=sqlite://./data/quire.db cargo run

# Check formatting, linting, tests, and release build
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
```

---

## Docker Compose & Portainer Deployment

### Build and Run Locally

```bash
docker compose config
docker compose build
docker compose up -d
```

### Deploying in Portainer

1. In Portainer, navigate to **Stacks** → **Add stack**.
2. Deploy using `ghcr.io/spencerthayer/quire:latest` (or an immutable commit SHA tag `ghcr.io/spencerthayer/quire:sha-<commit>`):

```yaml
services:
  quire:
    image: ghcr.io/spencerthayer/quire:latest
    container_name: quire
    restart: unless-stopped
    ports:
      - "${QUIRE_PORT:-8080}:8080"
    environment:
      QUIRE_HOST: "0.0.0.0"
      QUIRE_PORT: "8080"
      QUIRE_DATA_DIR: "/data"
      QUIRE_DATABASE_URL: "sqlite:///data/quire.db"
      QUIRE_LOG_LEVEL: "${QUIRE_LOG_LEVEL:-info}"
      QUIRE_TIMEZONE: "${QUIRE_TIMEZONE:-America/Los_Angeles}"
    volumes:
      - quire_data:/data
    healthcheck:
      test:
        - CMD
        - /app/quire
        - healthcheck
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 30s

volumes:
  quire_data:
```

All SQLite database files (`/data/quire.db`, `/data/quire.db-wal`, `/data/quire.db-shm`) persist in the `quire_data` volume across container restarts and recreations. Back up `/data/quire.db` periodically using standard SQLite backup tools or volume snapshots.

---

## License

Released under the [MIT License](LICENSE).
