# Quire Architecture

## 1. Overview

Quire is a self-hosted RSS/Atom aggregation, full-text article extraction, AI curation, and automated publication platform written in Rust. It uses a single-binary, single-container, single-database architecture so it can run unattended on a local homelab Docker server managed through Portainer without external cloud infrastructure, message brokers, or separate worker containers.

Quire is designed for a single application instance backed by SQLite WAL storage. Distributed multi-node scheduling is explicitly out of scope.

## 2. Application Components

Quire is organized into focused internal modules within one crate:

* **`src/main.rs`**: Process entrypoint, CLI subcommand dispatch (`quire healthcheck`), structured logging initialization (`tracing_subscriber`), SQLite pool startup, startup recovery of interrupted ingestion jobs, background scheduler spawning, and graceful `SIGINT`/`SIGTERM` shutdown.
* **`src/config.rs`**: Environment variable parsing (`AppConfig`), secret redaction (`SecretString`), timezone validation (`chrono-tz`), ingestion setting validators, and three-tier configuration precedence resolution (`Environment` > `Database` > `Default`).
* **`src/error.rs`**: Central typed error hierarchy (`AppError`) mapping validation, conflict, not-found, forbidden, database, migration, and template errors into safe HTTP responses.
* **`src/db/mod.rs`**: SQLite connection pool initialization (`sqlx::SqlitePool`), WAL mode, `synchronous = NORMAL`, foreign-key enforcement, embedded migration execution on a dedicated connection, health checks, aggregate dashboard counters, recent failure queries, and transactional nonsecret `settings` persistence. Migrations run before the pool is built. `0004_article_independence.sql` runs with foreign keys off on that dedicated connection, because sqlx 0.8's SQLite driver ignores a `-- no-transaction` marker and `PRAGMA foreign_keys` is a no-op inside a transaction. Pool connections keep foreign keys on. A cancelled transaction only queues `ROLLBACK`. Before a pooled connection is reused, Quire starts an immediate transaction and rolls it back. If that begin fails, the pool closes the connection. That keeps a cancelled claim from leaving the next `BEGIN IMMEDIATE` with `InvalidSavePointStatement`.
* **`src/feeds/mod.rs`**: Feed domain model, per-feed schedule/limit overrides, cross-feed safe feed deletion, atomic fetch claiming (`try_claim_feed_fetch`), run completion persistence (`complete_feed_fetch_run`), history pruning, and interrupted fetch recovery.
* **`src/feeds/validation.rs`**: Centralized outbound HTTP URL validation, IPv4/IPv6 restricted network classification (`is_disallowed_ip`), and injectable DNS hostname resolution (`HostResolver` / `SystemDnsResolver`).
* **`src/feeds/fetcher.rs`**: Shared outbound HTTP execution engine (`fetch_url_with_retries`) handling DNS pinning, redirect validation, conditional requests (`If-None-Match` / `If-Modified-Since`), streaming byte limits, timeouts, and exponential backoff with jitter and `Retry-After` support.
* **`src/feeds/parser.rs`**: RSS 2.0, RSS 1.0/RDF, and Atom 1.0 parsing via `feed-rs`, character set decoding (`encoding_rs`), XXE/DTD entity rejection, entry metadata extraction, and per-fetch entry truncation.
* **`src/feeds/scheduler.rs`**: `IngestionCoordinator` with two dispatchers (feed fetch and article extraction). Each admits a new claim only while its in-flight count is below the configured limit. Manual actions mark SQLite rows due and notify the dispatchers. Startup takes an exclusive lock on `{data_dir}/quire.lock` before opening the database. A second process exits with `Another Quire instance is using {dir}`. The data directory must be on local storage. The lock is not reliable on NFS or SMB.
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

## 4. SQLite Schema (`migrations/0001` through `0004`)

All connections enforce `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, `PRAGMA foreign_keys = ON`, and a 10-second busy timeout. Write-then-read transactions that can hit `SQLITE_BUSY` go through `db::with_write_retry` (`BEGIN IMMEDIATE`, three attempts, 250 ms then 1 s, plus or minus 20 percent jitter).

### Execution model

Exactly one process holds `{data_dir}/quire.lock`. Feed fetches and article extractions are rows in SQLite. The HTTP handlers for **Fetch now** and **Fetch all** set `next_scheduled_fetch_at` and `manual_requested_at`, then notify the dispatchers. A request made just before a restart is still due after startup. Repeated clicks update the same row. A click on a feed that is already running updates nothing. At claim time the run is `manual` when `manual_requested_at` is set, and the claim clears that column. Manual requests still run when scheduled polling is paused.

Each dispatcher polls and also wakes on notify. It claims new work only while `active < limit`. Reducing the limit does not cancel work already running. An extraction backlog does not delay feed polling. Dashboard counts (`due_feeds`, queued and processing extractions, stuck jobs, oldest queued age) are read from SQLite.

Graceful shutdown cancels in-flight work and releases a feed run or extraction job only if this process still owns the claim token. A shutdown release of an extraction job puts the attempt count back to the value it had before that claim. A crash leaves the row in `processing`. The next start requeues it, or marks it failed when `attempts` has reached `max_attempts`.

### `feeds`
Stores feed subscriptions, conditional HTTP cache validators (`etag`, `last_modified`), scheduling state (`next_scheduled_fetch_at`, `manual_requested_at`, `fetch_interval_minutes`, `max_entries_override`, `fetch_in_progress`, `current_run_id`), and latest execution metrics (`last_fetch_status`, `last_http_status`, `last_fetch_duration_ms`, `last_discovered_count`, `last_inserted_count`, `last_extraction_count`, `consecutive_failures`, `last_error`). A failed run backs off `next_scheduled_fetch_at` (doubling, up to 16 times, at most 7 days) and never schedules earlier than `Retry-After` when that header is present (capped at 24 hours).

### `articles`
Articles are deduplicated by `canonical_url`. `feed_id` is nullable and is only the primary display source (`ON DELETE SET NULL`). There is no unique index on `(feed_id, external_guid)`. GUID identity lives on `article_feed_sources (feed_id, external_guid)`. `articles.external_guid` is an inert leftover column.

Content and job state are separate:

| Content | Job | Display |
|---|---|---|
| `none` | `queued` | Pending |
| `none` | `processing` | Extracting |
| `summary_fallback` | `queued` or `processing` | Summary (full text pending) |
| `html` | `queued` or `processing` | Full text (refreshing) |
| `html` | `completed` | Full text |
| `summary_fallback` | `completed` or `failed` | Summary only |
| `none` | `failed` | Failed |

`articles.extraction_status` is a legacy column written from those two fields. New code does not decide from it. A future publication step must select `extracted_content_type = 'html'` and must not treat `extraction_status = 'extracted'` as proof of full text.

Article retries use one budget. The article HTTP layer retries only a connection refused or reset, at most twice, with a fixed 1 second delay. Timeouts, HTTP 5xx, HTTP 429, and DNS errors are not retried there. The job layer retries those, up to `max_attempts` (default 3). `next_eligible_at = now + max(15s * 2^attempts, Retry-After)`, capped at 6 hours. That is at most 6 HTTP requests for one article. A 4xx other than 408 or 429, a TLS error, an SSRF block, a response that is too large, an unsupported type, or a readability failure on HTTP 200 is terminal. On a retryable failure, a sanitized summary is stored as `summary_fallback` while the job stays queued.

### `article_feed_sources`
Associates articles with every feed that discovered them (`PRIMARY KEY (article_id, feed_id)`). When multiple feeds link to the same `canonical_url`, the article body is stored once in `articles` while each feed's discovery timestamp and GUID are recorded in `article_feed_sources`. Deleting a feed removes its source rows. A shared article is then repointed to its earliest remaining source. An article that is in no edition and has no remaining source is deleted. An article that is in an edition stays, with `feed_id` NULL, and the UI shows the source as "(removed feed)".

### `feed_fetch_runs`
Stores execution history per feed (`trigger_type`, `status`, `started_at`, `completed_at`, `duration_ms`, `http_status`, `entries_seen`, `entries_skipped`, `entries_truncated`, `articles_inserted`, `articles_updated`, `articles_unchanged`, `extraction_queued`, `error_category`, `error_message`). Automatically pruned to the most recent 100 runs per feed on completion.

### `article_extraction_jobs`
Standing SQLite-backed extraction queue (`article_id UNIQUE`, `status`, `trigger_type`, `attempts`, `max_attempts`, `next_eligible_at`, `claimed_at`, `completed_at`, `last_error`).
* **Atomicity**: Enqueued in the same database transaction as article insertion/update.
* **Deduplication**: `UNIQUE(article_id)` prevents an article from accumulating duplicate active jobs.
* **Concurrency**: Workers claim jobs using `BEGIN IMMEDIATE` transactions (`status = 'queued'` -> `'processing'`).
* **Recovery**: On startup, `recover_interrupted_extractions` resets any job left in `processing` back to `queued`.

## 5. Security Boundaries

### SSRF and DNS rebinding protection
Quire fetches only public unicast addresses. `is_public_unicast` is an allow-list.

* IPv4 is allowed unless it sits in a special-purpose block that is not globally reachable: `0.0.0.0/8`, `10.0.0.0/8`, `100.64.0.0/10`, `127.0.0.0/8`, `169.254.0.0/16`, `172.16.0.0/12`, `192.0.0.0/24`, `192.0.2.0/24`, `192.88.99.0/24`, `192.168.0.0/16`, `198.18.0.0/15`, `198.51.100.0/24`, `203.0.113.0/24`, `224.0.0.0/4`, and `240.0.0.0/4`.
* IPv6 is allowed only inside `2000::/3`, and not inside `2001::/23` (which covers Teredo), `2001:db8::/32`, `2002::/16` (6to4), or `3fff::/20`. Everything outside `2000::/3` is denied, including IPv4-mapped addresses, NAT64 `64:ff9b::/96`, unique-local, link-local, site-local `fec0::/10`, multicast, and loopback.
* Only `http` and `https` URLs without embedded credentials are permitted. Hostnames `localhost`, `.localhost`, `.local`, `.internal`, `metadata.google.internal`, and `instance-data` are rejected. Any TCP port is allowed.
* The classifier runs at four points: the URL literal, every DNS answer inside the fetcher (one restricted answer fails the request), every redirect, and `response.remote_addr()` after connect. A missing or unexpected peer is `SsrfBlocked`.
* The client is built with `no_proxy()`, so `HTTP_PROXY` and `HTTPS_PROXY` cannot bypass pinning. Verified addresses are pinned with `resolve_to_addrs`. DNS resolution is wrapped in the request's remaining timeout. Redirects are followed manually, up to 5 hops.
* If an upstream server uses a public address to proxy internal content, this check cannot see that.

### Response & Parser Hardening
* Responses are read via `bytes_stream()` with incremental byte-count enforcement (`10 MiB` default for feeds, `15 MiB` default for articles).
* XML payloads containing `<!ENTITY` or external `SYSTEM`/`PUBLIC` DTD declarations are rejected prior to XML parsing.
* All extracted and fallback HTML is sanitized through `ammonia` before storage and re-sanitized before rendering in Askama templates.
