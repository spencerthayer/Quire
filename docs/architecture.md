# Quire Architecture

## 1. Overview

Quire is a self-hosted RSS aggregation, AI curation, and automated publication platform written in Rust. It is designed around a single-binary, single-container, single-database architecture so it can run reliably on a local homelab Docker server managed through Portainer without external cloud infrastructure, message brokers, or separate database containers.

## 2. Application Components

Quire is structured into focused internal modules within a single Cargo crate:

* **`src/main.rs`**: Process entrypoint, CLI subcommand dispatch (`quire healthcheck`), structured logging initialization (`tracing_subscriber`), SQLite pool startup, and graceful `SIGINT`/`SIGTERM` shutdown.
* **`src/config.rs`**: Environment variable parsing (`AppConfig`), secret redaction (`SecretString`), timezone validation (`chrono-tz`), and configuration precedence resolution (`ConfigSource`).
* **`src/error.rs`**: Central typed error hierarchy (`AppError`) mapping domain, validation, conflict, CSRF, database, migration, and template errors into safe HTTP responses without leaking internal stack traces or SQL details.
* **`src/db/`**: SQLite connection pool initialization (`sqlx::SqlitePool`), WAL and foreign-key enforcement, embedded migration execution, health checks, dashboard metrics, and transactional nonsecret `settings` persistence.
* **`src/feeds/`**: Feed domain model, SSRF-hardened HTTP/HTTPS URL validation, and transactional CRUD operations against the `feeds` table.
* **`src/articles/`**: Article query access against the `articles` table (read path implemented in Phase 1; feed polling and `dom_smoothie` readability extraction planned for Phase 2).
* **`src/editions/`**: Edition query access against `editions` and `edition_articles` (read path implemented in Phase 1; scheduled composition and EPUB/TXT export planned for Phase 5).
* **`src/web/`**: Axum router, Askama HTML template rendering, double-submit cookie CSRF protection, form handlers, and the `GET /health` machine-readable endpoint.

### Architectural Deviation Note (Phase 1)

The prompt's illustrative directory tree listed `src/ai/`, `src/export/`, and `src/delivery/` alongside `src/feeds/`, `src/articles/`, and `src/editions/`. Per the prompt's rule (*"Do not create empty modules merely to match the proposed directory tree. Document any significant architectural deviations."*), `src/ai/`, `src/export/`, and `src/delivery/` are omitted in Phase 1 until their concrete traits and implementations are introduced in Phases 4, 5, and 6.

## 3. End-to-End Data Flow

```
[RSS / Atom Publishers]
        │
        ▼  (Phase 2: reqwest + feed-rs + dom_smoothie)
┌───────────────────────────┐
│  Feed Ingestion & Extract │
└─────────────┬─────────────┘
              │  Transactional upsert + deduplication
              ▼
┌───────────────────────────┐
│   SQLite Database (WAL)   │◄─── [Browser UI / Axum + Askama] (Phase 1)
│  /data/quire.db           │
└──────┬──────────────┬─────┘
       │              │
       │ (Phase 4)    │ (Phase 5)
       ▼              ▼
┌──────────────┐ ┌───────────────────────────┐
│ AI Curation  │ │ Edition Composer & Export │
│ (Ollama/LLM) │ │ (epub-builder / TXT)      │
└──────────────┘ └─────────────┬─────────────┘
                               │ (Phase 6)
                               ▼
                 ┌───────────────────────────┐
                 │  ElevenReader Delivery    │
                 │  (Experimental REST API)  │
                 └───────────────────────────┘
```

## 4. SQLite Schema (`migrations/0001_initial_schema.sql`)

All connections enable `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, `PRAGMA foreign_keys = ON`, and a 10-second busy timeout. All timestamps are stored as UTC ISO-8601 strings (`YYYY-MM-DDTHH:MM:SSZ`).

### `feeds`
* `id` (`INTEGER PRIMARY KEY AUTOINCREMENT`)
* `name` (`TEXT NOT NULL`)
* `feed_url` (`TEXT NOT NULL UNIQUE`)
* `website_url` (`TEXT`)
* `description` (`TEXT`)
* `enabled` (`INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1))`)
* `created_at`, `updated_at` (`TEXT NOT NULL`)
* `last_successful_fetch_at`, `last_attempted_fetch_at`, `last_error` (`TEXT`)
* Indexes: `idx_feeds_enabled`, `idx_feeds_created_at`

### `articles`
* `id` (`INTEGER PRIMARY KEY AUTOINCREMENT`)
* `feed_id` (`INTEGER NOT NULL REFERENCES feeds(id) ON DELETE CASCADE`)
* `external_guid` (`TEXT`)
* `canonical_url` (`TEXT NOT NULL`)
* `title` (`TEXT NOT NULL`)
* `author`, `description`, `published_at` (`TEXT`)
* `discovered_at` (`TEXT NOT NULL`)
* `extracted_content` (`TEXT`)
* `extraction_status` (`TEXT NOT NULL DEFAULT 'pending' CHECK (extraction_status IN ('pending', 'extracted', 'failed', 'skipped'))`)
* `content_hash` (`TEXT`)
* `created_at`, `updated_at` (`TEXT NOT NULL`)
* Deduplication & lookup indexes:
  * `idx_articles_feed_external_guid`: Partial `UNIQUE` index on `(feed_id, external_guid) WHERE external_guid IS NOT NULL AND external_guid != ''`.
  * `idx_articles_canonical_url`: `UNIQUE` index on `(canonical_url)`.
  * `idx_articles_content_hash`: Partial index on `(content_hash) WHERE content_hash IS NOT NULL` for cross-feed body/title hash deduplication when publishers omit GUIDs or vary URLs.
  * `idx_articles_feed_id`, `idx_articles_published_at`, `idx_articles_discovered_at`, `idx_articles_extraction_status`.

### `settings`
* `key` (`TEXT PRIMARY KEY NOT NULL`)
* `value` (`TEXT NOT NULL`)
* `updated_at` (`TEXT NOT NULL`)
* Stores nonsecret runtime configuration (`timezone`, `ai_provider`, `ai_base_url`, `ai_model`). Secret keys (`ai_api_key`, `elevenreader_refresh_token`) are rejected at the repository layer.

### `editions`
* `id` (`INTEGER PRIMARY KEY AUTOINCREMENT`)
* `title` (`TEXT NOT NULL`)
* `publication_date` (`TEXT NOT NULL`)
* `status` (`TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'generating', 'completed', 'failed', 'delivered'))`)
* `created_at`, `updated_at` (`TEXT NOT NULL`)
* `output_file_path` (`TEXT`)
* Indexes: `idx_editions_publication_date`, `idx_editions_status`

### `edition_articles`
* `edition_id` (`INTEGER NOT NULL REFERENCES editions(id) ON DELETE CASCADE`)
* `article_id` (`INTEGER NOT NULL REFERENCES articles(id) ON DELETE CASCADE`)
* `position` (`INTEGER NOT NULL CHECK (position >= 0)`)
* `created_at` (`TEXT NOT NULL`)
* Constraints: `PRIMARY KEY (edition_id, article_id)`, `UNIQUE (edition_id, position)`
* Index: `idx_edition_articles_article_id`

## 5. Web Server Architecture

* **Framework**: Axum 0.8 running on the Tokio multi-threaded runtime.
* **Templates**: Compile-time type-checked server-rendered HTML templates via Askama (`templates/*.html`).
* **Static Assets**: Served directly by the Rust binary from `/static` using `tower_http::services::ServeDir`, requiring no Node.js runtime or separate web server.
* **CSRF Protection**: State-changing browser `POST` routes use a double-submit pattern: `GET` forms set an `HttpOnly; SameSite=Strict` cookie (`quire_csrf`) containing a 256-bit random hex token and embed the same token in a hidden `<input name="csrf_token">`. Every `POST` handler verifies that the cookie and form token match before executing state changes.

## 6. Background Task Strategy

Background tasks (scheduled feed polling in Phase 2, AI batch summarization in Phase 4, and scheduled edition building in Phase 5) will run as supervised asynchronous tasks within the same Tokio runtime as the Axum HTTP server, sharing the `SqlitePool`. SQLite WAL mode allows concurrent readers alongside serialized transactional writers without blocking HTTP request handlers.

## 7. Configuration Precedence

Configuration is resolved in strict priority order:

1. **Environment Variables (`QUIRE_*`)**: Highest precedence. Always wins over database values and locks the corresponding field in the web UI. Required for all secrets (`QUIRE_AI_API_KEY`, `QUIRE_ELEVENREADER_REFRESH_TOKEN`) and bind/storage settings (`QUIRE_HOST`, `QUIRE_PORT`, `QUIRE_DATA_DIR`, `QUIRE_DATABASE_URL`).
2. **SQLite `settings` Table**: Editable through `/settings` in the browser for nonsecret parameters (`timezone`, `ai_provider`, `ai_base_url`, `ai_model`) when the corresponding environment variable is unset.
3. **Compiled Defaults**: Lowest precedence (`0.0.0.0:8080`, `America/Los_Angeles`, `info`).

## 8. Docker Persistence

The production container runs as nonroot user `quire` (`UID 10001`, `GID 10001`). All persistent state—the SQLite database (`quire.db`, `quire.db-wal`, `quire.db-shm`) and future generated publication files—resides under `/data`, mounted to a named Docker volume (`quire_data`). Container recreation or image upgrades preserve `/data`.

## 9. Error Handling and Logging

* Domain and infrastructure errors are represented by `AppError` (`thiserror`).
* Internal database and migration errors are logged with structured fields via `tracing::error!` and return generic HTTP 500 messages to clients so internal schema or file paths are never leaked.
* Secrets are wrapped in `SecretString`, which formats as `[REDACTED]` in `Debug` and `Display` implementations.

## 10. Planned External Integrations

### AI Processing (Phase 4)
* Pluggable provider interface supporting local **Ollama** instances and **OpenAI-compatible** HTTP APIs (OpenAI, OpenRouter, vLLM, llama.cpp).
* Decoupled from RSS ingestion: feed polling stores raw/extracted articles first; AI scoring, classification, and summarization run as a separate stage with token/budget safeguards.

### ElevenReader Delivery (Phase 6)
* Uses the experimental, undocumented ElevenReader HTTP flow researched in `repos/mcp-elevenreader`:
  1. Exchanges `QUIRE_ELEVENREADER_REFRESH_TOKEN` via `https://securetoken.googleapis.com/v1/token` for a short-lived Bearer `id_token`.
  2. Uploads generated EPUB publications or URLs to `https://api.elevenlabs.io/v1/reader/reads/add/v2` with exponential backoff on `HTTP 429`.
* Treated as an optional, replaceable delivery adapter so upstream changes to ElevenReader's private API cannot break core feed aggregation or EPUB generation.
