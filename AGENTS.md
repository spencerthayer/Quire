# AGENTS.md — Persistent Coding Agent Instructions for Quire

This file defines architectural rules, development conventions, and operational boundaries for coding agents working on **Quire**. Read this file before modifying code, schemas, or deployment manifests.

---

## 1. Project Purpose

**Quire** is a self-hosted application written in Rust that collects articles from RSS and Atom feeds, extracts and deduplicates full-article content, curates and summarizes stories using local or hosted AI models, composes scheduled daily or weekly publications (EPUB and TXT), and delivers generated publications to **ElevenReader** for text-to-speech listening.

Quire is designed to run independently on a local Docker homelab server managed through **Portainer** without requiring cloud infrastructure, external databases, or separate frontend build servers.

---

## 2. Technical Architecture

* **Single Executable, Single Database, Single Container**: Quire compiles to one Rust binary (`quire`) that serves the HTTP web interface, exposes health/API routes, and runs background tasks (`IngestionCoordinator`) within the same Tokio runtime.
* **Single-Instance Invariant**: Exactly one Quire process may operate on a given data directory at a time, enforced at startup via an OS file lock on `/data/quire.lock` (`std::fs::File::try_lock`). `/data` must reside on local storage (not NFS/SMB).
* **Web Framework**: `axum` (0.8) on `tokio`.
* **Database**: SQLite via `sqlx` (`SqlitePool`) with versioned migrations in `./migrations/`.
* **Frontend**: Server-rendered HTML templates using `askama` (and progressive enhancement via HTMX where needed). Static assets live in `./static/` and are served directly by the Rust binary. The production container must never require Node.js.
* **Pipeline Crates**:
  * HTTP client: `reqwest` (with `rustls`, `stream`, `gzip`, `deflate`, `brotli`).
  * Feed parsing: `feed-rs` and `encoding_rs`.
  * Readability HTML extraction & sanitization: `dom_smoothie`, `dom_query`, and `ammonia`.
  * EPUB generation: `epub-builder`.
  * Logging: `tracing` and `tracing-subscriber`.
  * Error handling: `thiserror` and `anyhow`.

---

## 3. Development Conventions

* **Rust Edition**: Use Rust **2024 edition** (`edition = "2024"` in `Cargo.toml`).
* **Strong Typing & Explicit Errors**: Represent domain and infrastructure failures in `src/error.rs` (`AppError`). Avoid `.unwrap()` or `.expect()` in request handlers or background tasks.
* **Small, Testable Modules**: Keep modules focused on a single responsibility. Pass explicit dependencies (`&SqlitePool`, `&AppConfig`, `Arc<dyn HostResolver>`) so functions can be tested against temporary databases and local HTTP fixture servers without global state.
* **Minimal Dependencies**: Do not add crates for unimplemented future phases unless required to establish a clean architectural interface.
* **No Hardcoded Credentials or Committed Secrets**: Never commit `.env` files, API keys, refresh tokens, or SQLite `.db` files. Wrap secret values in `SecretString` (`src/config.rs`) so `Debug` and `Display` output always prints `[REDACTED]`.
* **Standard Validation Commands**: Before completing any task or committing changes, run:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  ```

---

## 4. Architectural Boundaries

Keep the following responsibilities decoupled into separate modules so any external integration can be replaced without rewriting core application logic:

1. **Application Configuration** (`src/config.rs`)
2. **Database & Migrations** (`src/db/`)
3. **Feed Management, Fetching, Parsing & Scheduling** (`src/feeds/mod.rs`, `src/feeds/fetcher.rs`, `src/feeds/parser.rs`, `src/feeds/scheduler.rs`, `src/feeds/validation.rs`)
4. **Article Normalization, Extraction, Sanitization & Storage** (`src/articles/mod.rs`, `src/articles/normalizer.rs`, `src/articles/extractor.rs`, `src/articles/sanitizer.rs`)
5. **AI Processing** (`src/ai/` when implemented in Phase 4)
6. **Edition Composition** (`src/editions/`)
7. **Publication Export** (`src/export/` when implemented in Phase 5)
8. **External Delivery (ElevenReader)** (`src/delivery/` when implemented in Phase 6)
9. **Web Interface & HTTP Handlers** (`src/web/`, `templates/`, `static/`)

---

## 5. SQLite Requirements & Persistent Extraction Queue

SQLite is the primary persistent store and the authoritative source of all durable application and job state. Every database change must follow these rules:

* **WAL Mode & Foreign Keys**: `PRAGMA journal_mode = WAL` and `PRAGMA foreign_keys = ON` must remain enabled on every connection in the pool (`src/db/mod.rs`).
* **Versioned Migrations**: All schema changes must be added as numbered SQL migration files in `./migrations/` and applied automatically on startup via `sqlx::migrate!("./migrations")`. Migrations must be idempotent/repeatable across restarts without corrupting existing data. Never edit an already-released migration file.
* **UTC Timestamps**: Store all timestamps in UTC ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
* **Transactional Writes**: Perform multi-statement or state-changing writes inside explicit `sqlx`-managed transactions (`pool.begin().await?` for write-first transactions; `pool.begin_with("BEGIN IMMEDIATE").await?` via `db::with_write_retry` for read-modify-write transactions). Never use unmanaged raw `BEGIN`/`COMMIT` SQL strings on pooled connections, and never hold a SQLite transaction open across network I/O, DNS lookups, retry sleeps, or HTML extraction.
* **Appropriate Indexes**: Add indexes for foreign keys, deduplication keys (`(feed_id, external_guid)`, `canonical_url`, `content_hash`), and sort/eligibility columns.
* **Non-Blocking Operations**: Use asynchronous `sqlx` queries and offload CPU-intensive feed parsing, readability extraction, and HTML sanitization to `tokio::task::spawn_blocking` so Tokio worker threads are never blocked.

### Standing Requirement: Persistent SQLite Extraction Queue

The article extraction queue (`article_extraction_jobs`) is persisted in SQLite with the following mandatory guarantees across all phases:

1. **Durability**: Pending extraction jobs survive application crashes and container restarts.
2. **Recovery**: Interrupted jobs left in `processing` state are detected on startup (`recover_interrupted_extractions`) and returned to `queued` state (or marked `failed` when `attempts >= max_attempts`). In-process failures, panics, or hard deadline expirations release the job conditional on its `claim_token`.
3. **Deduplication**: `UNIQUE(article_id)` ensures an article cannot accumulate duplicate active extraction jobs.
4. **Concurrency & Ownership**: One coordinator-owned extraction dispatcher claims eligible jobs (`attempts < max_attempts`) using managed `BEGIN IMMEDIATE` transactions with a unique `claim_token`, and verifies `claim_token` on every completion or release.
5. **Retries**: Failed jobs retain `attempts`, `last_error`, and exponential backoff `next_eligible_at` schedules (honoring `Retry-After`) up to `max_attempts` within a fixed budget of at most 6 HTTP requests per article extraction cycle.
6. **Atomicity**: Job creation (`enqueue_extraction_job_in_tx`), article persistence, and feed-run completion (`current_run_id` ownership check) occur within the same SQLite transaction.

Tokio coordinates asynchronous worker execution, but SQLite remains the authoritative source of job state.

---

## 6. Docker and Portainer Requirements

* The `Dockerfile` must use a multi-stage build, produce a minimal Debian slim runtime image, run as a nonroot user (`quire`, UID `10001`), expose port `8080`, persist all state under `/data`, and include a working `HEALTHCHECK` (`["/app/quire", "healthcheck"]`).
* `compose.yaml` must work both with local `docker compose` (via `./start` and `./stop`) and with **Portainer Stacks** deploying the prebuilt multi-arch image (`ghcr.io/spencerthayer/quire:latest` or immutable commit-SHA tags) on `linux/amd64` and `linux/arm64`.
* Never require privileged containers, Docker socket mounts, or host networking.
* All persistent data (SQLite database and generated editions) must reside on the `/data` volume so container recreation preserves state.

---

## 7. Security Requirements

* **Configuration Precedence**: Environment variables (`QUIRE_*`) always take precedence over SQLite `settings` table entries. When an environment variable is set, the web UI must display it as locked and refuse browser overrides.
* **Secret Protection**: Never store `QUIRE_AI_API_KEY` or `QUIRE_ELEVENREADER_REFRESH_TOKEN` in the `settings` table, never log their values, and never expose them in HTML templates or JSON endpoints.
* **SSRF & Outbound HTTP Security**: Validate all feed, website, and article URLs (`src/feeds/validation.rs`, `src/feeds/fetcher.rs`). Allow only `http` and `https` schemes, forbid embedded URL credentials, and reject `localhost`, `.localhost`, `.local`, `.internal`, `metadata.google.internal`, and `instance-data`. Enforce a strict public-unicast IP allow-list (`is_public_unicast`) at four points: URL literals, every DNS resolution result inside the fetcher, every redirect target, and post-connect `response.remote_addr()` peer verification. Disable environment proxies (`.no_proxy()`), pin verified socket addresses into `reqwest`, and enforce HTTP and DNS timeouts on all outbound requests.
* **CSRF Protection**: All state-changing browser `POST` endpoints must verify the double-submit `quire_csrf` cookie against the submitted `csrf_token` form field (`src/web/mod.rs`).
* **Parameterized Queries**: Always use `sqlx` bind parameters (`?`). Never interpolate user input into SQL strings.
* **Sanitized HTML**: All downloaded HTML and feed summaries must be sanitized via `src/articles/sanitizer.rs` before storage and before rendering in templates.
* **Safe Error Responses**: Log internal database or I/O errors via `tracing::error!` and return generic error messages to HTTP clients.
* **Network Exposure**: Until built-in authentication is added in a future phase, Quire's web interface must only be exposed on a trusted private LAN/homelab network or behind an authenticating reverse proxy. Do not trust `X-Forwarded-*` headers unless a trusted proxy is explicitly configured.

---

## 8. Reference Repositories (`./repos/`)

* `./repos/` contains cloned third-party reference projects (`RSSPub`, `kindle-daily-digest`, `condenseit`, `read-everything`, `RSSbrew`, `feedr`, `distillfeed`, `mcp-elevenreader`).
* `./repos/` is excluded from Git via `/repos/` in `.gitignore` and `repos/.gitignore`. Never stage, commit, or submodule anything under `./repos/`.
* Inspect `./repos/` and `docs/references.md` before building new subsystems in Phases 2–7.
* **License Enforcement**: Quire is MIT-licensed. Never copy or translate code from `repos/RSSbrew` (`AGPL-3.0`) or `repos/read-everything` (no license). Only adapt code from MIT-licensed references when appropriate and with proper attribution in `docs/references.md`.

---

## 9. Local Container (`./start`, `./stop`)

Use these scripts for every local Docker run. Do not start, stop, restart, or recreate local containers with raw `docker compose` or `docker run` commands. Those skip `.env` loading, profile isolation, and `./_logs` capture.

* **Default Profile (`default`)**:
  * **Start**: From the repository root, run `./start` (or `./start --profile default`). The script reads `./.env`, builds the image, and starts the container named `quire` on project `quire` using volume `quire_quire_data`. If `./.env` is missing, copy `.env.example` to `.env` first. Never commit `.env`.
  * **Stop**: Run `./stop` (or `./stop --profile default`). It stops the log follower and removes the container while preserving the default SQLite volume and `./_logs/`.
  * **Protection**: Destructive volume cleanup (`--purge`) is strictly forbidden on the default profile.
* **Isolated Review Profile (`review`)**:
  * Use `--profile review` for all automated container tests, crash/recovery probes, seeded database tests, and independent review checks so the default `quire` container and its volume are never touched:
    ```bash
    ./start --profile review [--recreate] [--no-build]
    ./stop --profile review [--purge]
    ```
  * The `review` profile uses a separate Compose project (`quirereview`), a separate container name (`quire-review`), a separate named volume (`quirereview_quire_data`), and a separate host port (`QUIRE_REVIEW_PORT`, default `18080`).
  * `./stop --profile review --purge` removes the `quire-review` container and deletes only the isolated `quirereview` volume.
  * Unknown profile names are rejected with an error; the scripts never fall back to the default profile when an invalid profile is specified.
* **Logs**: `./start` follows container logs into `./_logs/<container>-<UTC timestamp>.log` and points `./_logs/latest.log` (or `./_logs/latest-review.log` for the review profile) at that file. Do not commit `./_logs/`.
* **UI**: After `./start`, the default web UI is at `http://localhost:8080` unless `QUIRE_PORT` in `.env` sets another host port.
* **Scope**: `compose.yaml` sets the in-container listen port (`8080`), data directory (`/data`), and database URL.

---

## 10. Scope Management

* Implement only the functionality authorized for the current phase (see `docs/roadmap.md`).
* Do not build stubbed or fake features that pretend RSS polling, AI summarization, or ElevenReader uploads succeeded when they are not yet implemented.
* Document unimplemented phase features honestly in the UI and documentation.
