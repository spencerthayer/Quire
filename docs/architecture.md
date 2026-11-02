# Quire Architecture

## 1. Overview

Quire is a self-hosted RSS/Atom aggregation, full-text article extraction, AI curation, and automated publication platform written in Rust. It uses a single-binary, single-container, single-database architecture so it can run unattended on a local homelab Docker server managed through Portainer without external cloud infrastructure, message brokers, or separate worker containers.

Quire is designed for a single application instance backed by SQLite WAL storage. Distributed multi-node scheduling is explicitly out of scope.

## 2. Application Components

Quire is organized into focused internal modules within one crate:

* **`src/main.rs`**: Process entrypoint, CLI subcommand dispatch (`quire healthcheck`), structured logging initialization (`tracing_subscriber`), SQLite pool startup, startup recovery of interrupted ingestion jobs, background scheduler spawning, and graceful `SIGINT`/`SIGTERM` shutdown.
* **`src/config.rs`**: Environment variable parsing (`AppConfig`), secret redaction (`SecretString`), timezone validation (`chrono-tz`), ingestion setting validators, and three-tier configuration precedence resolution (`Environment` > `Database` > `Default`).
* **`src/error.rs`**: Central typed error hierarchy (`AppError`) mapping validation, conflict, not-found, forbidden, database, migration, and template errors into safe HTTP responses.
* **`src/db/mod.rs`**: SQLite connection pool initialization (`sqlx::SqlitePool`), WAL and foreign-key enforcement, embedded migration execution, health checks, aggregate dashboard counters, recent failure queries, and transactional nonsecret `settings` persistence.
* **`src/feeds/mod.rs`**: Feed domain model, per-feed schedule/limit overrides, cross-feed safe feed deletion, atomic fetch claiming (`try_claim_feed_fetch`), run completion persistence (`complete_feed_fetch_run`), history pruning, and interrupted fetch recovery.
* **`src/feeds/validation.rs`**: Centralized outbound HTTP URL validation, IPv4/IPv6 restricted network classification (`is_disallowed_ip`), and injectable DNS hostname resolution (`HostResolver` / `SystemDnsResolver`).
* **`src/feeds/fetcher.rs`**: Shared outbound HTTP execution engine (`fetch_url_with_retries`) handling DNS pinning, redirect validation, conditional requests (`If-None-Match` / `If-Modified-Since`), streaming byte limits, timeouts, and exponential backoff with jitter and `Retry-After` support.
* **`src/feeds/parser.rs`**: RSS 2.0, RSS 1.0/RDF, and Atom 1.0 parsing via `feed-rs`, character set decoding (`encoding_rs`), XXE/DTD entity rejection, entry metadata extraction, and per-fetch entry truncation.
* **`src/feeds/scheduler.rs`**: `IngestionCoordinator` managing background Tokio polling, bounded concurrency (`JoinSet`), manual trigger coordination, and SQLite extraction queue draining.
* **`src/articles/mod.rs`**: Transactional article deduplication and persistence (`persist_discovered_entries`), cross-feed source tracking (`article_feed_sources`), SQLite-backed extraction job claiming (`claim_next_extraction_job`) and completion (`complete_extraction_job`) using `BEGIN IMMEDIATE`, and paginated/filtered article queries.
* **`src/articles/normalizer.rs`**: Relative URL resolution, conservative canonical URL normalization (stripping fragments and known tracking parameters such as `utm_*`, `fbclid`, `gclid`), whitespace normalization, and SHA-256 content hashing over normalized text.
* **`src/articles/extractor.rs`**: Full-text article extraction using `dom_smoothie::Readability`, charset decoding, and summary fallback orchestration.
* **`src/articles/sanitizer.rs`**: Untrusted HTML sanitization using `ammonia` and `dom_query`, stripping scripts, styles, forms, and navigation noise while converting `<img>` alt text into readable `<figure><figcaption>` captions.
* **`src/editions/mod.rs`**: Edition query access (scheduled EPUB/TXT generation planned for Phase 5).
* **`src/web/mod.rs`**: Axum router, Askama HTML templates, double-submit cookie CSRF protection, manual fetch and retry handlers, pagination/filtering, and `GET /health`.

## 3. Ingestion & Extraction Pipeline

```text
Configured Feed
      |
      v
Scheduler / Manual Trigger
      |
      v
Bounded Fetch Queue (fetch_in_progress claim + JoinSet)
      |
      v
HTTP Fetcher (DNS validation + redirect checks + streaming limit)
      |
      v
RSS / Atom Parser (feed-rs + charset decode + XXE guard)
      |
      v
Entry Normalization (relative URLs + tracking param removal + UTC dates)
      |
      v
SQLite Deduplication (articles + article_feed_sources)
      |
      v
Persistent Extraction Queue (article_extraction_jobs in SQLite)
      |
      v
Article HTTP Fetcher (DNS validation + redirect checks + streaming limit)
      |
      v
DOM Extraction (dom_smoothie Readability + summary fallback)
      |
      v
Sanitization (dom_query noise removal + ammonia allowlist)
      |
      v
Content Hashing (SHA-256 over normalized text)
      |
      v
SQLite Article Storage
      |
      v
Browser Article Library (/articles & /articles/{id})
```

## 4. SQLite Schema (`migrations/0001` & `0002`)

All connections enforce `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, `PRAGMA foreign_keys = ON`, and a 10-second busy timeout.

### `feeds`
Stores feed subscriptions, conditional HTTP cache validators (`etag`, `last_modified`), scheduling state (`next_scheduled_fetch_at`, `fetch_interval_minutes`, `max_entries_override`, `fetch_in_progress`), and latest execution metrics (`last_fetch_status`, `last_http_status`, `last_fetch_duration_ms`, `last_discovered_count`, `last_inserted_count`, `last_extraction_count`, `consecutive_failures`, `last_error`).

### `articles`
Stores deduplicated articles (`canonical_url UNIQUE`, partial unique index on `(feed_id, external_guid)`), extracted HTML (`extracted_content`, `extracted_content_type`), SHA-256 `content_hash`, and extraction attempt diagnostics (`extraction_attempts`, `last_extraction_attempt_at`, `last_successful_extraction_at`, `extraction_error`, `next_extraction_retry_at`).

Logical extraction states exposed to the application and UI:
* `pending`: Waiting in the SQLite extraction queue.
* `processing`: Currently claimed by an extraction worker.
* `completed`: Full-text readability extraction succeeded (`extraction_status = 'extracted'`, `extracted_content_type = 'html'`).
* `fallback`: Full-text extraction failed, but a usable sanitized feed summary was stored (`extraction_status = 'skipped'`, `extracted_content_type = 'summary_fallback'`).
* `failed`: Both full-text extraction and summary fallback failed.

### `article_feed_sources`
Associates articles with every feed that discovered them (`PRIMARY KEY (article_id, feed_id)`). When multiple feeds link to the same `canonical_url`, the article body is stored once in `articles` while each feed's discovery timestamp and GUID are recorded in `article_feed_sources`. When a feed is deleted, any shared article primarily owned by that feed is reassigned to the earliest surviving feed in `article_feed_sources` before the feed row is deleted.

### `feed_fetch_runs`
Stores execution history per feed (`trigger_type`, `status`, `started_at`, `completed_at`, `duration_ms`, `http_status`, `entries_seen`, `entries_skipped`, `entries_truncated`, `articles_inserted`, `articles_updated`, `articles_unchanged`, `extraction_queued`, `error_category`, `error_message`). Automatically pruned to the most recent 100 runs per feed on completion.

### `article_extraction_jobs`
Standing SQLite-backed extraction queue (`article_id UNIQUE`, `status`, `trigger_type`, `attempts`, `max_attempts`, `next_eligible_at`, `claimed_at`, `completed_at`, `last_error`).
* **Atomicity**: Enqueued in the same database transaction as article insertion/update.
* **Deduplication**: `UNIQUE(article_id)` prevents an article from accumulating duplicate active jobs.
* **Concurrency**: Workers claim jobs using `BEGIN IMMEDIATE` transactions (`status = 'queued'` -> `'processing'`).
* **Recovery**: On startup, `recover_interrupted_extractions` resets any job left in `processing` back to `queued`.

## 5. Security Boundaries

### SSRF & DNS Rebinding Protection
* Only `http://` and `https://` URLs without embedded credentials are permitted.
* Hostnames targeting `localhost`, `.localhost`, `.local`, `.internal`, `metadata.google.internal`, or `instance-data` are rejected.
* Every outbound HTTP request resolves the target hostname through `HostResolver` (`SystemDnsResolver` in production) and validates **every** returned `SocketAddr` via `is_disallowed_ip` (blocking loopback, RFC1918 private IPv4, link-local `169.254.0.0/16` including cloud metadata `169.254.169.254`, CGNAT `100.64.0.0/10`, multicast, IPv6 unique-local `fc00::/7`, IPv6 link-local `fe80::/10`, documentation ranges, and IPv4-mapped IPv6 addresses).
* Verified socket addresses are pinned onto the per-hop `reqwest::Client` using `resolve_to_addrs(host, &resolved_addrs)` so the TCP connection cannot rebind to an unverified IP.
* Automatic `reqwest` redirects are disabled (`Policy::none()`). Each HTTP `301`/`302`/`303`/`307`/`308` redirect `Location` is resolved and validated through the exact same URL and DNS policy up to a maximum of 5 hops.
* **Known Limitation**: If an upstream server uses a valid public IP to proxy internal content on its own backend, client-side DNS/IP validation cannot detect that server-side behavior.

### Response & Parser Hardening
* Responses are read via `bytes_stream()` with incremental byte-count enforcement (`10 MiB` default for feeds, `15 MiB` default for articles).
* XML payloads containing `<!ENTITY` or external `SYSTEM`/`PUBLIC` DTD declarations are rejected prior to XML parsing.
* All extracted and fallback HTML is sanitized through `ammonia` before storage and re-sanitized before rendering in Askama templates.
