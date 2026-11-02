# Quire Development Roadmap

This roadmap organizes Quire's development into seven incremental phases, distinguishing **required core features** from **optional enhancements** in each phase.

---

## Phase 1 — Foundation (Completed)

Establish the Rust application architecture, SQLite persistence, web interface, Docker deployment, and CI/CD pipelines.

### Required Features
* [x] Single-executable Rust application (`axum`, `tokio`, `sqlx`, `askama`).
* [x] SQLite connection pool with WAL mode, foreign keys enabled, and embedded versioned migrations (`0001_initial_schema.sql`).
* [x] Core schema for `feeds`, `articles`, `settings`, `editions`, and `edition_articles`.
* [x] Three-tier configuration system (environment variables > SQLite `settings` > defaults) with secret redaction.
* [x] Responsive server-rendered web interface (`Dashboard`, `Feeds`, `Articles`, `Editions`, `Settings`).
* [x] Working feed CRUD (create, list, edit, enable/disable, delete with confirmation) and SSRF-safe URL validation.
* [x] CSRF protection on state-changing browser requests.
* [x] Machine-readable `GET /health` endpoint and `quire healthcheck` CLI subcommand.
* [x] Multi-stage nonroot `Dockerfile`, `compose.yaml`, and Portainer deployment documentation.
* [x] Automated unit/integration tests and GitHub Actions workflows (`ci.yml` and `publish-container.yml`).

---

## Phase 2 — RSS Ingestion (Completed)

Implement automated and manual feed fetching, article discovery, full-text readability extraction, persistent SQLite job queuing, and deduplication.

### Required Features
* [x] Asynchronous HTTP feed fetching (`reqwest`) with strict connect/read timeouts, streaming response size limits, redirect validation, and SSRF IP verification on resolved DNS records.
* [x] Conditional HTTP fetching (`If-None-Match` / `ETag` and `If-Modified-Since` / `Last-Modified` with `304 Not Modified` handling).
* [x] Bounded exponential backoff retries with jitter and `Retry-After` header support.
* [x] RSS 2.0, RSS 1.0/RDF, and Atom 1.0 parsing using `feed-rs` with character-encoding normalization and XXE/DTD entity rejection.
* [x] Canonical URL normalization (resolving relative links, stripping fragments, and removing known tracking query parameters such as `utm_*`, `fbclid`, `gclid`).
* [x] Multi-layered transactional deduplication across feeds using `canonical_url`, `(feed_id, external_guid)`, and `article_feed_sources` cross-feed associations, plus safe feed deletion that preserves shared articles.
* [x] Persistent SQLite extraction queue (`article_extraction_jobs`) with crash recovery, atomic job claiming, and bounded retry schedules.
* [x] Full-article HTML extraction using `dom_smoothie` with automatic fallback to sanitized feed summaries when extraction fails.
* [x] HTML sanitization (`ammonia` + `dom_query`) and SHA-256 `content_hash` calculation over normalized text.
* [x] Background polling scheduler and manual "Fetch Now" / "Fetch All Enabled" triggers in the web UI.
* [x] Feed detail view (`GET /feeds/{id}`), execution history (`feed_fetch_runs` with 100-run retention), paginated/filtered article browser (`GET /articles`), and article reader view (`GET /articles/{id}`) with manual extraction retry/refresh.

### Optional Enhancements (Deferred to Phase 3+)
* [ ] OPML subscription import and export.
* [ ] Per-feed custom `User-Agent` overrides.

---

## Phase 3 — Article Library & Search

Build rich article search, categorization, tags, and retention management in the web interface.

### Required Features
* [ ] Full-text search across article titles, authors, and extracted content (SQLite FTS5).
* [ ] Feed categories/tags and source priority weighting.
* [ ] Configurable article retention and cleanup policies to bound disk usage.

### Optional Enhancements
* [ ] Star/bookmark articles for manual inclusion in the next edition.
* [ ] Mark-as-read / dismiss controls for manual curation.

---

## Phase 4 — AI Processing

Integrate local and hosted LLM providers to analyze headlines, score relevance, classify topics, and summarize articles.

### Required Features
* [ ] Provider-agnostic LLM client supporting local **Ollama** and **OpenAI-compatible** (`/v1/chat/completions`) endpoints.
* [ ] Strictly decoupled AI pipeline that processes stored articles independently of RSS fetching.
* [ ] Headline analysis and topic/category classification.
* [ ] Configurable relevance scoring based on user-defined topic profiles.
* [ ] Concise per-article summaries and multi-article editorial briefings.
* [ ] Token usage tracking and daily/monthly cost circuit breakers.

### Optional Enhancements
* [ ] Semantic embedding similarity ranking cached in SQLite.
* [ ] Per-category custom summarization prompts.

---

## Phase 5 — Publication Engine

Combine selected articles and AI briefings into scheduled daily or weekly publications exported as EPUB and TXT.

### Required Features
* [ ] Edition composer selecting eligible articles by time window, relevance score, and per-feed volume caps.
* [ ] Deterministic article ordering within editions persisted in `edition_articles`.
* [ ] "Quick Skim" table-of-contents generation with in-document section anchors and source links.
* [ ] Valid EPUB 3 generation using `epub-builder` written to `/data/editions/`.
* [ ] Clean plain-text (`.txt`) publication export suitable for TTS engines.
* [ ] Scheduled daily and weekly edition generation plus manual ad-hoc edition builds from the web UI.
* [ ] Browser download endpoints for generated editions.

### Optional Enhancements
* [ ] Custom cover image generation for EPUB editions.
* [ ] Markdown (`.md`) and OPDS catalog export endpoints.

---

## Phase 6 — ElevenReader Integration

Deliver generated publications automatically to ElevenReader for text-to-speech listening.

### Required Features
* [ ] Experimental ElevenReader client module isolated behind a delivery trait.
* [ ] Firebase token refresh flow exchanging `QUIRE_ELEVENREADER_REFRESH_TOKEN` for short-lived Bearer access tokens with in-memory expiry caching.
* [ ] Automated upload of generated EPUB/TXT files to ElevenReader (`/v1/reader/reads/add/v2`).
* [ ] Delivery status tracking on `editions` (`delivered` / `failed`), rate-limit backoff (`HTTP 429`), and bounded retry handling.
* [ ] Clear UI status indicators and warnings noting that ElevenReader relies on an undocumented upstream API.

### Optional Enhancements
* [ ] ElevenReader library deduplication and cleanup of old daily editions.
* [ ] Optional URL-based article delivery for single high-priority stories.

---

## Phase 7 — Advanced Automation

Expand Quire with multi-profile workflows, editorial rules, and additional delivery targets.

### Required Features
* [ ] Multiple publication profiles (for example, a weekday morning tech briefing and a weekend long-form digest).
* [ ] Per-source filtering rules (keyword allow/block lists, minimum length thresholds, extraction overrides).
* [ ] Delivery history audit log and operational notification hooks.

### Optional Enhancements
* [ ] SMTP / Send-to-Kindle email delivery adapter.
* [ ] Webhook / ntfy push notifications on edition completion or pipeline errors.
* [ ] Built-in optional HTTP authentication middleware for environments without an external authenticating reverse proxy.
