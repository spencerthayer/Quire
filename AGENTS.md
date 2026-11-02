# AGENTS.md — Persistent Coding Agent Instructions for Quire

This file defines architectural rules, development conventions, and operational boundaries for coding agents working on **Quire**. Read this file before modifying code, schemas, or deployment manifests.

---

## 1. Project Purpose

**Quire** is a self-hosted application written in Rust that collects articles from RSS and Atom feeds, extracts and deduplicates full-article content, curates and summarizes stories using local or hosted AI models, composes scheduled daily or weekly publications (EPUB and TXT), and delivers generated publications to **ElevenReader** for text-to-speech listening.

Quire is designed to run independently on a local Docker homelab server managed through **Portainer** without requiring cloud infrastructure, external databases, or separate frontend build servers.

---

## 2. Technical Architecture

* **Single Executable, Single Database, Single Container**: Quire compiles to one Rust binary (`quire`) that serves the HTTP web interface, exposes health/API routes, and runs background tasks within the same Tokio runtime.
* **Web Framework**: `axum` (0.8) on `tokio`.
* **Database**: SQLite via `sqlx` (`SqlitePool`) with versioned migrations in `./migrations/`.
* **Frontend**: Server-rendered HTML templates using `askama` (and progressive enhancement via HTMX where needed). Static assets live in `./static/` and are served directly by the Rust binary. The production container must never require Node.js.
* **Planned Pipeline Crates**:
  * HTTP client: `reqwest` (with `rustls`).
  * Feed parsing: `feed-rs`.
  * Readability HTML extraction: `dom_smoothie`.
  * EPUB generation: `epub-builder`.
  * Logging: `tracing` and `tracing-subscriber`.
  * Error handling: `thiserror` and `anyhow`.

---

## 3. Development Conventions

* **Rust Edition**: Use Rust **2024 edition** (`edition = "2024"` in `Cargo.toml`).
* **Strong Typing & Explicit Errors**: Represent domain and infrastructure failures in `src/error.rs` (`AppError`). Avoid `.unwrap()` or `.expect()` in request handlers or background tasks.
* **Small, Testable Modules**: Keep modules focused on a single responsibility. Pass explicit dependencies (`&SqlitePool`, `&AppConfig`) so functions can be tested against temporary databases without global state.
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
3. **Feed Management & Ingestion** (`src/feeds/`)
4. **Article Extraction & Storage** (`src/articles/`)
5. **AI Processing** (`src/ai/` when implemented in Phase 4)
6. **Edition Composition** (`src/editions/`)
7. **Publication Export** (`src/export/` when implemented in Phase 5)
8. **External Delivery (ElevenReader)** (`src/delivery/` when implemented in Phase 6)
9. **Web Interface & HTTP Handlers** (`src/web/`, `templates/`, `static/`)

---

## 5. SQLite Requirements

SQLite is the primary persistent store. Every database change must follow these rules:

* **WAL Mode & Foreign Keys**: `PRAGMA journal_mode = WAL` and `PRAGMA foreign_keys = ON` must remain enabled on every connection in the pool (`src/db/mod.rs`).
* **Versioned Migrations**: All schema changes must be added as numbered SQL migration files in `./migrations/` and applied automatically on startup via `sqlx::migrate!("./migrations")`. Migrations must be idempotent/repeatable across restarts without corrupting existing data.
* **UTC Timestamps**: Store all timestamps in UTC ISO-8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
* **Transactional Writes**: Perform multi-statement or state-changing writes inside explicit transactions (`pool.begin().await?` ... `tx.commit().await?`).
* **Appropriate Indexes**: Add indexes for foreign keys, deduplication keys (`(feed_id, external_guid)`, `canonical_url`, `content_hash`), and sort columns.
* **Non-Blocking Operations**: Use asynchronous `sqlx` queries and never block the Tokio runtime with synchronous file or database I/O.

---

## 6. Docker and Portainer Requirements

* The `Dockerfile` must use a multi-stage build, produce a minimal Debian slim runtime image, run as a nonroot user (`quire`, UID `10001`), expose port `8080`, persist all state under `/data`, and include a working `HEALTHCHECK` (`["/app/quire", "healthcheck"]`).
* `compose.yaml` must work both with local `docker compose up -d` and with **Portainer Stacks** deploying the prebuilt multi-arch image (`ghcr.io/spencerthayer/quire:latest` or immutable commit-SHA tags) on `linux/amd64` and `linux/arm64`.
* Never require privileged containers, Docker socket mounts, or host networking.
* All persistent data (SQLite database and generated editions) must reside on the `/data` volume so container recreation preserves state.

---

## 7. Security Requirements

* **Configuration Precedence**: Environment variables (`QUIRE_*`) always take precedence over SQLite `settings` table entries. When an environment variable is set, the web UI must display it as locked and refuse browser overrides.
* **Secret Protection**: Never store `QUIRE_AI_API_KEY` or `QUIRE_ELEVENREADER_REFRESH_TOKEN` in the `settings` table, never log their values, and never expose them in HTML templates or JSON endpoints.
* **SSRF & URL Validation**: Validate all feed and website URLs (`src/feeds/mod.rs`). Allow only `http` and `https` schemes, forbid embedded URL credentials, and reject `localhost`, `.local`, `.internal`, loopback, link-local, and RFC1918 private IP addresses. Enforce HTTP client timeouts on all outbound requests.
* **CSRF Protection**: All state-changing browser `POST` endpoints must verify the double-submit `quire_csrf` cookie against the submitted `csrf_token` form field (`src/web/mod.rs`).
* **Parameterized Queries**: Always use `sqlx` bind parameters (`?`). Never interpolate user input into SQL strings.
* **Safe Error Responses**: Log internal database or I/O errors via `tracing::error!` and return generic error messages to HTTP clients.
* **Network Exposure**: Until built-in authentication is added in a future phase, Quire's web interface must only be exposed on a trusted private LAN/homelab network or behind an authenticating reverse proxy. Do not trust `X-Forwarded-*` headers unless a trusted proxy is explicitly configured.

---

## 8. Reference Repositories (`./repos/`)

* `./repos/` contains cloned third-party reference projects (`RSSPub`, `kindle-daily-digest`, `condenseit`, `read-everything`, `RSSbrew`, `feedr`, `distillfeed`, `mcp-elevenreader`).
* `./repos/` is excluded from Git via `/repos/` in `.gitignore` and `repos/.gitignore`. Never stage, commit, or submodule anything under `./repos/`.
* Inspect `./repos/` and `docs/references.md` before building new subsystems in Phases 2–7.
* **License Enforcement**: Quire is MIT-licensed. Never copy or translate code from `repos/RSSbrew` (`AGPL-3.0`) or `repos/read-everything` (no license). Only adapt code from MIT-licensed references when appropriate and with proper attribution in `docs/references.md`.

---

## 9. Scope Management

* Implement only the functionality authorized for the current phase (see `docs/roadmap.md`).
* Do not build stubbed or fake features that pretend RSS polling, AI summarization, or ElevenReader uploads succeeded when they are not yet implemented.
* Document unimplemented phase features honestly in the UI and documentation.
