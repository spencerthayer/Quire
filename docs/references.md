# Reference Repositories

This document records the third-party repositories cloned into `./repos/` for architectural research and implementation reference.

## Isolation Policy

* `./repos/` is ignored in both the root `.gitignore` (`/repos/`) and `repos/.gitignore` (`*` / `!.gitignore`).
* Reference repositories must never be committed to version control or added as Git submodules.
* No code from a reference repository may be copied into Quire unless its license is compatible with Quire's MIT license and proper copyright attribution is preserved.
* In Phase 1, **no source code was copied verbatim** from any reference repository. All Phase 1 modules were written from scratch for Quire.

---

## 1. RSSPub (Primary Reference)

* **Upstream URL**: [https://github.com/harshit181/RSSPub](https://github.com/harshit181/RSSPub)
* **Local Path**: `repos/RSSPub`
* **License**: MIT License (`Copyright (c) 2025 harshit181`)
* **Reuse Restrictions**: Permissive (MIT). Direct adaptation or reuse is permitted provided the copyright and permission notice are preserved.
* **Purpose**: Rust RSS-to-EPUB publication server with scheduled compilation, full-article extraction, and web administration.
* **Components Useful to Quire**:
  * Single-binary Rust architecture combining Axum HTTP routes, Askama templates, and background scheduling (`tokio-cron-scheduler`).
  * Concurrent feed fetching with `reqwest`, `feed-rs`, and `tokio::sync::Semaphore` per-feed concurrency limits (`src/feed.rs`).
  * Full-article HTML readability extraction using `dom_smoothie` and HTML sanitization with `ammonia` (`src/processor.rs`, `src/util/`).
  * EPUB assembly patterns using `epub-builder` (`src/epub_gen.rs`).

## 2. Kindle Daily Digest

* **Upstream URL**: [https://github.com/taruncherukurigit/kindle-daily-digest](https://github.com/taruncherukurigit/kindle-daily-digest)
* **Local Path**: `repos/kindle-daily-digest`
* **License**: MIT License (`Copyright (c) 2026 Tarun Cherukuri`)
* **Reuse Restrictions**: Permissive (MIT). Requires preserving copyright notice if code is reused.
* **Purpose**: Automated twice-daily RSS digest pipeline assembling categorized articles with a "Quick Skim" in-document index into EPUB files.
* **Components Useful to Quire**:
  * Per-feed volume caps so high-volume feeds cannot crowd out lower-frequency sources within a category.
  * Strict per-article extraction timeouts with automatic fallback to the RSS summary when full-text extraction fails or times out.
  * "Quick Skim" table-of-contents layout at the beginning of each edition with internal anchor links to full articles and external links to canonical URLs.
  * Cross-category title deduplication rules.

## 3. CondenseIt

* **Upstream URL**: [https://github.com/wildlifechorus/condenseit](https://github.com/wildlifechorus/condenseit)
* **Local Path**: `repos/condenseit`
* **License**: MIT License (`Copyright (c) 2026 CondenseIt contributors`)
* **Reuse Restrictions**: Permissive (MIT). Requires preserving copyright notice if code is reused.
* **Purpose**: Self-hosted AI news digest supporting local Ollama, OpenRouter, and OpenAI-compatible endpoints with preference learning and budget tracking.
* **Components Useful to Quire**:
  * Unified LLM provider abstraction supporting local Ollama and OpenAI-compatible `/v1/chat/completions` endpoints.
  * Structured LLM enrichment extracting topics, entities, novelty scores, and multi-paragraph summaries in a single pass.
  * Daily and monthly token/cost budget circuit breakers that halt AI calls before limits are exceeded.
  * Explicit vs. environment-locked admin configuration patterns.

## 4. ReadEverything

* **Upstream URL**: [https://github.com/berleary/read-everything](https://github.com/berleary/read-everything)
* **Local Path**: `repos/read-everything`
* **License**: No `LICENSE` file present in repository (All Rights Reserved by default under copyright law).
* **Reuse Restrictions**: **Do not copy code** from `read-everything`. Inspect only for high-level architectural ideas.
* **Purpose**: Pluggable content aggregator and LLM digest engine built with FastAPI, HTMX, and SQLite.
* **Components Useful to Quire**:
  * Task-based LLM routing (mapping logical operations such as `card_summary` and `digest_compose` to configurable models).
  * Multi-stage daily digest composition pipeline (time-window selection, enrichment, topic grouping, and publication rendering).
  * Server-rendered HTML + HTMX workflow without a client-side JavaScript build step.

## 5. RSSBrew

* **Upstream URL**: [https://github.com/yinan-c/RSSbrew](https://github.com/yinan-c/RSSbrew)
* **Local Path**: `repos/RSSbrew`
* **License**: GNU Affero General Public License v3.0 (`AGPL-3.0`)
* **Reuse Restrictions**: **Strong copyleft (AGPL-3.0) — incompatible with Quire's MIT license.** **Do not copy or translate source code** from `RSSbrew`. Use strictly for high-level feature and UX reference.
* **Purpose**: RSS filter, AI summarizer, and scheduled digest publisher.
* **Components Useful to Quire**:
  * Conceptual separation between entry-filtering rules (include/exclude by title, URL, or description) and summary-generation rules.
  * Per-feed custom `User-Agent` override concept for publishers that block default HTTP client headers.

## 6. feedr

* **Upstream URL**: [https://github.com/bahdotsh/feedr](https://github.com/bahdotsh/feedr)
* **Local Path**: `repos/feedr`
* **License**: MIT License (`Copyright (c) 2025 Gokul`)
* **Reuse Restrictions**: Permissive (MIT). Requires preserving copyright notice if code is reused.
* **Purpose**: Terminal-based RSS and Atom reader written in Rust.
* **Components Useful to Quire**:
  * Rust feed parsing patterns with `feed-rs`, `reqwest`, `encoding_rs`, and `dom_smoothie` (`src/feed.rs`).
  * Handling character set decoding and malformed XML feeds gracefully.
  * OPML import and export patterns using the `opml` crate.

## 7. DistillFeed

* **Upstream URL**: [https://github.com/ibaaj/distillfeed](https://github.com/ibaaj/distillfeed)
* **Local Path**: `repos/distillfeed`
* **License**: Apache License 2.0 (`Copyright 2026 DistillFeed contributors`)
* **Reuse Restrictions**: Permissive (Apache-2.0). Compatible with open-source distribution, but any reused code would require preserving Apache-2.0 notices and `NOTICE` attributions. Prefer clean-room Rust implementations in Quire to keep root licensing pure MIT.
* **Purpose**: RSS reader with AI relevance scoring, topic summaries, Ollama/OpenAI support, and cost safeguards.
* **Components Useful to Quire**:
  * Strict decoupling of **feed ingestion** (fetching and storing raw articles without invoking AI) from **AI processing** (scoring and summarizing eligible stored articles under cost safeguards).
  * Operational system notices that surface partial feed failures, missing AI configuration, and budget blocks in the UI.

## 8. ElevenReader MCP

* **Upstream URL**: [https://github.com/MIt9/mcp-elevenreader](https://github.com/MIt9/mcp-elevenreader)
* **Local Path**: `repos/mcp-elevenreader`
* **License**: MIT License (declared as `license = { text = "MIT" }` in `pyproject.toml` and `MIT` in `README.md`; `mit9`)
* **Reuse Restrictions**: Permissive (MIT). Note that ElevenReader's internal HTTP endpoints are undocumented and unofficial.
* **Purpose**: Python MCP server interacting with ElevenLabs ElevenReader (`elevenreader.io`).
* **Components Useful to Quire**:
  * Firebase token exchange workflow: exchanging a long-lived `refresh_token` against `https://securetoken.googleapis.com/v1/token?key=...` (`grant_type=refresh_token`) for a short-lived Bearer `id_token`, caching until 60 seconds before expiration (`src/mcp_elevenreader/server.py`).
  * Document upload endpoint mechanics: `POST https://api.elevenlabs.io/v1/reader/reads/add/v2` with multipart `from_document` (for EPUB/PDF files) or form `from_url` (for web URLs).
  * Rate-limit (`HTTP 429`) backoff and retry handling during document uploads.
