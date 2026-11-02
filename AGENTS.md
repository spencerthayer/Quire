# AGENTS.md — Persistent Coding Agent Instructions for Quire

This file defines architectural rules, development conventions, and operational boundaries for coding agents working on **Quire**. Read this file before modifying code, schemas, or deployment manifests.

---

## 1. Project Purpose

**Quire** is a self-hosted application written in Rust that collects articles from RSS and Atom feeds, extracts and deduplicates full-article content, curates and summarizes stories using local or hosted AI models, composes scheduled daily or weekly publications (EPUB and TXT), and delivers generated publications to **ElevenReader** for text-to-speech listening.

Quire is designed to run independently on a local Docker homelab server managed through **Portainer** without requiring cloud infrastructure, external databases, or separate frontend build servers.

---

## 2. Technical Architecture

* **Single Executable, Single Database, Single Container**: Quire compiles to one Rust binary (`quire`) that serves the HTTP web interface, exposes health/API routes, and runs background tasks (`IngestionCoordinator`) within the same Tokio runtime.
* **Web Framework**: `axum` (0.8) on `tokio`.
* **Database**: SQLite via `sqlx` (`SqlitePool`) with versioned migrations in `./migrations/`.
* **Frontend**: Server-rendered HTML templates using `askama`. Static assets live in `./static/` and are served directly by the Rust binary. The production container must never require Node.js.
* **Pipeline Crates**:
  * HTTP client: `reqwest` (with `rustls`, `stream`, `gzip`, `deflate`, `brotli`).
  * Feed parsing: `feed-rs` and `encoding_rs`.
  * Readability HTML extraction & sanitization: `dom_smoothie`, `dom_query`, and `ammonia`.
  * EPUB generation: `epub-builder`.
  * Logging: `tracing` and `tracing-subscriber`.
  * Error handling: `thiserror` and `anyhow`.

---

## 3. Standing Requirement: Persistent SQLite Extraction Queue

The article extraction queue (`article_extraction_jobs`) is persisted in SQLite with the following mandatory guarantees across all phases:

1. **Durability**: Pending extraction jobs survive application crashes and container restarts.
2. **Recovery**: Interrupted jobs left in `processing` state are detected on startup (`recover_interrupted_extractions`) and returned to `queued` state.
3. **Deduplication**: `UNIQUE(article_id)` ensures an article cannot accumulate duplicate active extraction jobs.
4. **Concurrency**: Multiple workers claim eligible jobs using `BEGIN IMMEDIATE` transactions (`claim_next_extraction_job`) so two workers can never claim the same job simultaneously.
5. **Retries**: Failed jobs retain `attempts`, `last_error`, and exponential backoff `next_eligible_at` schedules up to `max_attempts`.
6. **Atomicity**: Job creation (`enqueue_extraction_job_in_tx`) and article persistence occur within the same SQLite transaction.

Tokio coordinates asynchronous worker execution, but SQLite remains the authoritative source of job state.

---

## 4. Development Conventions

* **Rust Edition**: Use Rust **2024 edition** (`edition = "2024"` in `Cargo.toml`).
* **Strong Typing & Explicit Errors**: Represent domain and infrastructure failures in `src/error.rs` (`AppError`). Avoid `.unwrap()` or `.expect()` in request handlers or background tasks.
* **Small, Testable Modules**: Keep modules focused on a single responsibility. Pass explicit dependencies (`&SqlitePool`, `&AppConfig`, `Arc<dyn HostResolver>`) so functions can be tested against temporary databases and local HTTP fixture servers without global state.
* **No Hardcoded Credentials or Committed Secrets**: Never commit `.env` files, API keys, refresh tokens, or SQLite `.db` files. Wrap secret values in `SecretString` (`src/config.rs`) so `Debug` and `Display` output always prints `[REDACTED]`.
* **Standard Validation Commands**: Before completing any task or committing changes, run:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features -- -D warnings
  cargo test
  ```

---

## 5. Architectural Boundaries

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

## 6. SQLite Requirements

SQLite is the primary persistent store. Every database change must follow these rules:

* **WAL Mode & Foreign Keys**: `PRAGMA journal_mode = WAL` and `PRAGMA foreign_keys = ON` must remain enabled on every connection in the pool (`src/db/mod.rs`).
* **Versioned Migrations**: All schema changes must be added as numbered SQL migration files in `./migrations/` and applied automatically on startup via `sqlx::migrate!("./migrations")`. Never edit an already-released migration file.
* **UTC Timestamps**: Store all timestamps in UTC ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
* **Short Transactional Writes**: Perform multi-statement writes inside explicit transactions, and never hold a SQLite transaction open across network I/O or HTML extraction.

---

## 7. Security Requirements

* **Configuration Precedence**: Environment variables (`QUIRE_*`) always take precedence over SQLite `settings` table entries. When an environment variable is set, the web UI must display it as locked and reject browser overrides.
* **SSRF & DNS Rebinding Protection**: All outbound HTTP requests (feeds and articles) must pass through `src/feeds/validation.rs` and `src/feeds/fetcher.rs`. Resolve hostnames via `HostResolver`, verify every returned IP against `is_disallowed_ip`, pin resolved socket addresses into `reqwest`, and validate every HTTP redirect target before following.
* **CSRF Protection**: All state-changing browser `POST` endpoints must verify the double-submit `quire_csrf` cookie against the submitted `csrf_token` form field (`src/web/mod.rs`).
* **Sanitized HTML**: All downloaded HTML and feed summaries must be sanitized via `src/articles/sanitizer.rs` before storage and before rendering in templates.
* **Network Exposure**: Until built-in authentication is added in a future phase, Quire's web interface must only be exposed on a trusted private LAN/homelab network or behind an authenticating reverse proxy.

---

## 8. Reference Repositories (`./repos/`)

* `./repos/` contains cloned third-party reference projects (`RSSPub`, `kindle-daily-digest`, `condenseit`, `read-everything`, `RSSbrew`, `feedr`, `distillfeed`, `mcp-elevenreader`).
* `./repos/` is excluded from Git via `/repos/` in `.gitignore` and `repos/.gitignore`. Never stage, commit, or submodule anything under `./repos/`.
* **License Enforcement**: Quire is MIT-licensed. Never copy or translate code from `repos/RSSbrew` (`AGPL-3.0`) or `repos/read-everything` (no license).

---

## 9. Local Container (`./start`, `./stop`)

Use these scripts for every local Docker run. Do not start or stop the local container with raw `docker compose` or `docker run` commands. Those skip `.env` loading and the `./_logs` capture.

* **Start**: From the repository root, run `./start`. The script reads `./.env`, builds the image, and starts the container named `quire`. If `./.env` is missing, copy `.env.example` to `.env` first. Never commit `.env`.
* **Logs**: `./start` follows container logs into `./_logs/quire-<UTC timestamp>.log` and points `./_logs/latest.log` at that file. When checking server behavior, read `./_logs/latest.log`. Do not commit `./_logs/`.
* **Stop**: Run `./stop`. It stops the log follower and removes the container. The SQLite volume and the files under `./_logs/` remain.
* **UI**: After `./start`, the web UI is at `http://localhost:8080` unless `QUIRE_PORT` in `.env` sets another host port.
* **Scope**: `compose.yaml` still sets the in-container listen port (`8080`), data directory (`/data`), and database URL. Other `QUIRE_*` values from `./.env` are passed into the container.

The first `./start` compiles the Rust binary inside Docker and takes longer than later runs.
