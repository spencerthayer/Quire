# Quire Phase 2 — Independent Review

**Reviewed commit:** `66980ec076cb8e5db536498d77395339efbff438` ("feat: implement RSS ingestion and article extraction pipeline")
**Baseline:** `f0ec769e6dc5675bcf21e93c0a9b643e2772c3c9` (Phase 1)
**Review date:** 2026-11-01
**Decision:** **REJECT** (one confirmed P1; see §13)

---

## 1. Executive Summary

| Question | Answer |
|---|---|
| Does core functionality work? | Yes for a single feed at a time. 100 feeds / 1,000 articles ingested and extracted with zero duplicates in the sequential scheduler path. |
| Is concurrent ingestion reliable? | **No.** Feed persistence uses a deferred SQLite transaction that reads before writing. Concurrent writers make it fail immediately with `SQLITE_BUSY` ("database is locked"). The UI's **Fetch All** loses 8–9 of 12 feeds per click. Scheduled polling survives locally only because of a 50 ms stagger (QUA-001, P1). |
| Are security requirements satisfied? | Mostly. Redirect validation, DNS pinning, streaming/decompression limits, CSRF, and HTML sanitization all held under test. Remaining gaps are defense-in-depth (P3): IPv6 transition prefixes are not blocked, the fetcher trusts resolver output, and system proxy env vars are honored. |
| Is SQLite queue durability verified? | **Yes.** After a SIGKILL during extraction, every queued and in-flight job was recovered and completed on restart. The same held after container recreation. Claims are atomic: 16 concurrent claimers over 300 jobs produced 300 unique claims with 0 errors. |
| Do the queue's retry semantics meet the requirement? | **No.** Any failure is finalized as a summary fallback when the entry has a summary, even transient 5xx, timeout, or DNS failures. Automatic retries therefore apply only to summary-less entries (QUA-002). |
| Does Docker deployment work? | Yes. The image builds and runs as UID 10001, the healthcheck passes, and state persists across restart and recreation. Portainer deployment was not exercised. |
| Ready for acceptance? | **No.** Fix QUA-001; QUA-002 through QUA-009 should be fixed in the same pass. |

---

## 2. Repository State

| Item | Value |
|---|---|
| Branch | `main` (in sync with `origin/main`) |
| HEAD | `66980ec` — single Phase 2 commit |
| Reviewed range | `f0ec769..66980ec` (30 files, +7,861 / −800) |
| Working tree | Clean at review time (the session-start snapshot showing `M Cargo.toml` / untracked migration predates the commit) |
| New migration | `migrations/0002_feed_ingestion.sql` (0001 unmodified) |
| New dependencies | `ammonia 4.2.4`, `dom_query 0.29`, `encoding_rs 0.8`, `futures-util 0.3`; reqwest features `gzip`, `deflate`, `brotli`, `stream` |
| New modules | `src/feeds/{fetcher,parser,scheduler,validation}.rs`, `src/articles/{extractor,normalizer,sanitizer}.rs` |
| New tables | `article_feed_sources`, `feed_fetch_runs`, `article_extraction_jobs`; 15 columns added to `feeds`, 8 to `articles` |
| New routes | `GET /feeds/{id}`, `POST /feeds/fetch-all`, `POST /feeds/{id}/fetch`, `GET /articles/{id}`, `POST /articles/{id}/extract` |
| Background tasks | `IngestionCoordinator::spawn_background_loop` (30 s poll); detached `tokio::spawn` per manual fetch |
| New env vars | `QUIRE_FETCH_ENABLED`, `_FETCH_INTERVAL_MINUTES`, `_FETCH_MIN_INTERVAL_MINUTES`, `_FETCH_CONCURRENCY`, `_EXTRACTION_CONCURRENCY`, `_FEED_REQUEST_TIMEOUT_SECONDS`, `_ARTICLE_REQUEST_TIMEOUT_SECONDS`, `_MAX_FEED_RESPONSE_BYTES`, `_MAX_ARTICLE_RESPONSE_BYTES`, `_MAX_FEED_ENTRIES`, `_FETCH_MAX_ATTEMPTS`, `_FETCH_RETRY_INITIAL_SECONDS`, `_FETCH_RETRY_MAX_SECONDS` |
| Tests | `tests/integration_tests.rs` rewritten: 10 integration tests, 0 unit tests |
| Governance change | The commit **rewrote `AGENTS.md`** and removed several binding rules (QUA-009) |

The original Phase 2 specification is not in the repository. Traceability uses the review brief's requirement list, the `docs/roadmap.md` Phase 2 section, and the pre-Phase-2 `AGENTS.md`.

---

## 3. Requirements Traceability Matrix

| ID | Requirement | Implementation | Evidence | Status | Issues |
|---|---|---|---|---|---|
| R1 | RSS ingestion | `scheduler.rs:277`, `parser.rs:57`, `articles/mod.rs:137` | r27 (1,000 articles), crash probe | PARTIAL | QUA-001 |
| R2 | Atom ingestion | `parser.rs` (feed-rs) | existing `test_feed_parsing_rss_atom_rdf_and_edge_cases` | PASS | — |
| R3 | Conditional HTTP | `fetcher.rs:281-296, 389-400`; `feeds/mod.rs:562-571` | existing `/ok-conditional` test; validators kept only on success | PASS | — |
| R4 | Feed parsing robustness | `parser.rs:43-52` | r07, r08 fail | PARTIAL | QUA-005 |
| R5 | Metadata normalization | `parser.rs:138-200` | existing tests | PASS | — |
| R6 | Canonical URLs | `normalizer.rs:45-112` | r11: distinct `?id=` values preserved; bare keys rewritten | PARTIAL | QUA-016 |
| R7 | Deduplication | `UNIQUE(canonical_url)`, `articles/mod.rs:185` | r27: 0 duplicate URLs over 3 passes | PASS | — |
| R8 | Cross-feed identity | `article_feed_sources`, `articles/mod.rs:203-307` | r22 | PARTIAL | QUA-006 |
| R9 | Full-text extraction | `extractor.rs:33-66` | r27: 1,000/1,000 extracted | PASS | QUA-014 (P3) |
| R10 | HTML sanitization | `sanitizer.rs` (ammonia) + render-time re-sanitize `web/mod.rs:846` | r12 (20 vectors) | PASS | — |
| R11 | Extraction fallback | `extractor.rs:99-152` | r13, r14, r28, Docker | PARTIAL | QUA-002 |
| R12 | Content hashing | `normalizer.rs:136` (SHA-256 of normalized text) | existing test | PASS | — |
| R13 | Persistent extraction jobs | `article_extraction_jobs`, enqueue in same tx | SIGKILL probe, Docker recreate | PASS | — |
| R14 | Extraction retries | `articles/mod.rs:698-768` | Docker: summary-less job retried at +60 s, then failed at attempt 3; r14: summary job finalized | PARTIAL | QUA-002, QUA-011 |
| R15 | Automatic scheduling | `scheduler.rs:201-275` | r27, r29 | PARTIAL | QUA-001, QUA-008 |
| R16 | Manual fetching | `scheduler.rs:94-136` | r29: Fetch All lost 8–9 of 12 feeds | PARTIAL | QUA-001, QUA-003 |
| R17 | Duplicate execution prevention | `feeds/mod.rs:410-448` conditional `UPDATE ... WHERE fetch_in_progress = 0` | existing test; r23 | PASS | — |
| R18 | Concurrency limits | `drain_eligible_extractions` (bounded); manual fetch unbounded | r23: 10 concurrent with limit 2 | PARTIAL | QUA-003 |
| R19 | Crash recovery | `recover_interrupted_*` at startup | SIGKILL probe: 4 processing + 16 queued → 20 completed | PASS | QUA-004 (in-process) |
| R20 | Graceful shutdown | `main.rs:78-95` | probe: always 10 s; jobs left `processing` (recoverable) | PARTIAL | QUA-017 |
| R21 | Fetch history | `feed_fetch_runs`, retention 100 | Docker: interrupted run → `cancelled` | PASS | — |
| R22 | Browser controls | `web/mod.rs` routes | Docker curl: create/fetch/settings/404/405 | PASS | — |
| R23 | Config precedence | `config.rs:480+`, `web/mod.rs:1003-1080` | Docker: env-locked `fetch_enabled` refused; invalid values → 400 | PASS | QUA-021 (min-interval) |
| R24 | SQLite migrations | `0002_feed_ingestion.sql` | r26: real sqlx-migrated Phase 1 DB upgraded twice, all rows/FKs/timestamps intact | PASS | QUA-022 |
| R25 | SSRF protection | `validation.rs`, `fetcher.rs` | existing tests; Docker: 169.254.169.254 → 400 | PARTIAL | QUA-012 (P3) |
| R26 | DNS rebinding protection | `resolve_to_addrs` pinning per hop `fetcher.rs:252-261` | r04 shows connection goes to resolver-supplied address (pinning effective) | PASS | QUA-012 (proxy caveat) |
| R27 | Redirect validation | `fetcher.rs:309-375`, `Policy::none()` | r01: 9 restricted redirect targets blocked, 0 hits on internal endpoint | PASS | — |
| R28 | HTTP response limits | `fetcher.rs:440-497` | r05: 200 MB gzip bomb rejected in 7 ms; r06: drip body timed out at 2.0 s | PASS | — |
| R29 | Docker persistence | `Dockerfile`, `compose.yaml` | restart + `--force-recreate`: feeds, settings, runs, jobs persisted | PASS | — |
| R30 | Portainer compatibility | README stack snippet; `compose.yaml` `image: ${QUIRE_IMAGE:-quire:local}` | static inspection only | NOT VERIFIED | — |
| R31 | App health ≠ feed health | `web/mod.rs:109` (DB only) | Docker: healthy while all fetches/extractions failed | PASS | — |
| R32 | CSRF on state changes | `verify_csrf` on all POSTs | Docker: bad token 403, no cookie 403, missing field 422, GET 405 | PASS | — |
| R33 | XML security | quick-xml does not expand DTD entities | r09: billion-laughs + XXE after 64 KB not expanded | PASS | QUA-005 (filter harmful) |
| R34 | Safe feed deletion | `feeds/mod.rs:357-406` | r21, r22 | FAIL | QUA-006, QUA-007 |
| R35 | No repeated extraction on re-poll | `articles/mod.rs:251-306` | r27 repeat pass: 0 re-extractions | PASS | — |
| R36 | Transactions not held across network I/O | persist/claim/complete are separate from fetch | code inspection | PASS | — |
| R37 | Secrets kept out of DB/UI | `db/mod.rs:210` allow-list | code inspection (Phase 1 behavior unchanged) | PASS | — |

---

## 4. Critical Findings (P0 / P1)

### QUA-001 — Concurrent feed persistence fails with SQLITE_BUSY and drops whole batches

- **Severity:** P1
- **Confidence:** Confirmed
- **Component:** SQLite persistence / ingestion
- **Location:** `src/articles/mod.rs:148` (`pool.begin()`), `src/articles/mod.rs:185-201` (first statement is a `SELECT`); `src/feeds/scheduler.rs:122-136` (Fetch All), `src/feeds/scheduler.rs:220-223` (50 ms stagger)
- **Requirement:** R1, R15, R16. Automatic and manual ingestion must persist discovered articles reliably.
- **Expected:** Concurrent feed fetches (default `QUIRE_FETCH_CONCURRENCY=4`, manual fetches overlapping the scheduler, Fetch All) wait for the write lock (busy_timeout 10 s) and commit.
- **Actual:** `persist_discovered_entries` opens a deferred transaction (`BEGIN`), reads first, then tries to upgrade to a write. In WAL mode, if another connection holds or has just committed the write lock, SQLite returns `SQLITE_BUSY` immediately without invoking the busy handler (the read snapshot is stale). The whole batch rolls back and the run is recorded as `failed` / `database`.
- **Technical analysis:** Only `claim_next_extraction_job` and `complete_extraction_job` use `BEGIN IMMEDIATE`. The scheduled tick avoids collisions only through a 50 ms per-slot sleep, which is not a lock. On slower disks (typical homelab SBCs), larger feeds (max entries 200–5,000), or any overlap with a manual fetch or an extraction completion, collisions follow. Failed runs also increment `consecutive_failures`, which doubles the feed's next interval, up to 16×.
- **Reproduction:** reviewer tests (see §11).
  - `r24`: 4 concurrent persists × 30 entries × 10 rounds → **30/40 batches failed**, `SqliteError { code: 5, "database is locked" }`.
  - `r25`: 8 feeds through `execute_claimed_feed_fetch` without the stagger → 6/8 runs `failed:database`.
  - `r29`: `trigger_manual_fetch_all_enabled` with 12 feeds × 30 entries → **8–9 of 12 runs `failed:database`** on three consecutive runs. The same 12 feeds through the staggered scheduled path succeeded locally.
- **Impact:** Ingestion silently loses feed batches. Entries that leave the feed before the next successful poll are lost permanently. Repeated failures push affected feeds onto exponential backoff. The "Fetch All Enabled" control fails for most feeds.
- **Recommended fix:** Open write transactions with `pool.begin_with("BEGIN IMMEDIATE")` (available in sqlx 0.8.6) for `persist_discovered_entries`, `delete_feed`, `enqueue_manual_article_extraction`, `complete_feed_fetch_run`, and `try_claim_feed_fetch`. Optionally add one bounded retry on `SQLITE_BUSY`. Remove the stagger as a correctness mechanism.
- **Verification:** `r17`, `r24`, `r25`, and `r29` report 0 failed batches; also add a regression test with 8 concurrent 200-entry persists plus concurrent claim/complete loops.

No P0 findings.

---

## 5. Additional Findings (P2 / P3)

### QUA-002 — Transient extraction failures are finalized as summary fallback; automatic retries effectively never run

- **Severity:** P2 · **Confidence:** Confirmed
- **Component:** Article extraction / retry policy
- **Location:** `src/articles/extractor.rs:136-152` (fetch error → Fallback), `:99-116`; `src/articles/mod.rs:659-697` (Fallback → job `completed`)
- **Requirement:** R11, R14. Retry transient failures and keep retry possible.
- **Expected:** 5xx, 429, timeout, DNS, and connect errors schedule a persisted retry. Fallback is applied only after retries are exhausted or for permanent errors, and a fallback should remain eligible for a later full-text retry.
- **Actual:** If the entry has any summary, every fetch error (including `retryable: true`) returns `Fallback`. The job is marked `completed` and the article `skipped`/`summary_fallback`, with `next_extraction_retry_at = NULL`. Most RSS entries carry summaries, so the retry/backoff path is reached only for summary-less entries.
- **Reproduction:** `r13`: a 503 with a summary returns `Fallback`. `r14`: through the queue, job=`completed`, attempts=1, article=`skipped`. `r28`: 100 articles behind HTTP 500 → 100/100 `summary_fallback`, 0 queued. Docker: a seeded job with a summary was finalized as fallback on a DNS failure, while the summary-less job was retried at +60 s.
- **Impact:** A brief upstream outage permanently downgrades articles to summaries until someone clicks Retry per article or the entry changes.
- **Recommended fix:** In `fetch_and_extract_article`, return `Failed { retryable }` for retryable fetch errors while `attempts < max_attempts`, and apply the summary fallback only on the final attempt or for non-retryable errors. Either store the fallback as an intermediate state with a scheduled retry, or document the terminal semantics.
- **Verification:** `r13` and `r14` pass. An integration test with 503×2 then 200 ends `extracted`.

### QUA-003 — Manual fetches bypass `fetch_concurrency` and run as unsupervised detached tasks

- **Severity:** P2 · **Confidence:** Confirmed
- **Location:** `src/feeds/scheduler.rs:94-136` (`tokio::spawn` per feed, handle discarded); `src/web/mod.rs:639-647`
- **Requirement:** R18. Configured limits apply to feed requests; spawned tasks are owned and observable.
- **Expected:** Fetch All with `QUIRE_FETCH_CONCURRENCY=2` runs at most 2 feed requests at once, also counting scheduler work.
- **Actual:** One detached task per feed is spawned immediately, with no semaphore. Panics are unobserved, and shutdown doesn't wait for these tasks.
- **Reproduction:** `r23`: 10 feeds, limit 2 → **10 concurrent requests** at the fixture.
- **Impact:** Bursts of N outbound requests and N concurrent SQLite writers, which compounds QUA-001. A panicked task leaves the feed `fetch_in_progress = 1` until restart (QUA-004).
- **Recommended fix:** Use a shared `Arc<Semaphore>` sized from runtime config for every feed fetch (scheduled and manual), with tasks tracked in a `JoinSet` or `TaskTracker` owned by the coordinator. Alternatively, have manual triggers only mark the feed due (`next_scheduled_fetch_at = now`) and call `notify_workers()`.
- **Verification:** `r23` reports max in-flight ≤ limit.

### QUA-004 — Jobs and feeds can stay stuck in `processing` / `running` until process restart

- **Severity:** P2 · **Confidence:** Confirmed (jobs), Code-supported (feeds)
- **Location:** `src/feeds/scheduler.rs:104-116` (early `return` on config error leaves the feed claimed), `:318, :387, :425, :463, :502` (`let _ = complete_feed_fetch_run(...)`), `:593-600` (complete failure only logged); `src/articles/mod.rs:391` (enqueue refuses `processing`); recovery only at startup (`scheduler.rs:62`)
- **Requirement:** R19. In-process failures must not strand work.
- **Expected:** A worker panic, a DB error while persisting the outcome, or a config-load failure returns the job or feed to a recoverable state, or a lease expires.
- **Actual:** The job stays `processing` and the feed stays `fetch_in_progress = 1` (excluded from `list_due_feed_ids`) for the life of the process. Manual Retry shows "already in progress" and is refused.
- **Reproduction:** `r18`: claim a job without completing it → `enqueue_manual_article_extraction` returns `false` and `drain_eligible_extractions` returns 0.
- **Impact:** Silent per-article or per-feed stalls until the container restarts. The UI shows "processing" or "running" indefinitely.
- **Recommended fix:** Add a lease (`claimed_at` plus a timeout, recovered on each drain or tick rather than only at startup). On `complete_*` failure, retry with backoff before giving up. Treat `JoinError` (panic) by resetting the job.
- **Verification:** `r18` passes; a test that injects a panic in the extraction closure confirms the job is retried after the lease.

### QUA-005 — XML pre-filter rejects legitimate feeds and protects nothing

- **Severity:** P2 · **Confidence:** Confirmed
- **Location:** `src/feeds/parser.rs:43-52`
- **Requirement:** R4, R33
- **Expected:** Valid RSS/Atom parses; entity expansion and XXE are prevented.
- **Actual:** The first 64 KB is upper-cased and rejected if it contains `<!ENTITY`, `SYSTEM "FILE:`, or `PUBLIC "` anywhere, including in text content. This rejects (a) any feed whose text contains something like `went public "today"`, and (b) RSS 0.91 feeds with the standard Netscape `<!DOCTYPE rss PUBLIC ...>`. It also doesn't look past 64 KB, but that doesn't matter: quick-xml 0.42 (via feed-rs) does not expand custom or external entities at all.
- **Reproduction:** `r07` and `r08`: both are rejected with "external entity or DTD declarations are not permitted". `r09`: a billion-laughs + `SYSTEM "file:///etc/passwd"` payload placed after 64 KB parses with the title literally `&d;` and the entry title `&xxe;` (no expansion).
- **Impact:** Some feeds can never be ingested, with a misleading security error.
- **Recommended fix:** Delete the substring filter. Keep a regression test (like `r09`) asserting no expansion, so a future parser change is caught.
- **Verification:** `r07`, `r08`, and `r09` pass.

### QUA-006 — Deleting a feed fails with a UNIQUE violation when a shared article is reassigned

- **Severity:** P2 · **Confidence:** Confirmed
- **Location:** `src/feeds/mod.rs:360-391` (copies `afs.external_guid` into `articles.external_guid`); `src/articles/mod.rs:204-228` (nulls the GUID on `article_feed_sources` but leaves `articles.external_guid` when the owner differs)
- **Requirement:** R8, R34
- **Expected:** Deleting feed A reassigns shared articles to a surviving feed without error.
- **Actual:** Feed B has article X with GUID `g`. Feed A owns article Y. Feed B then publishes GUID `g` for Y's URL. Deleting A moves Y to B with GUID `g`, colliding with X on `idx_articles_feed_external_guid`. The delete fails with a generic error.
- **Reproduction:** `r22` → `UNIQUE constraint failed: articles.feed_id, articles.external_guid`.
- **Impact:** The user cannot delete the feed; no data is lost (the transaction rolls back).
- **Recommended fix:** During reassignment, null `articles.external_guid` whenever the target feed already uses that GUID, or stop mirroring GUIDs on `articles` and treat `article_feed_sources` as authoritative.
- **Verification:** `r22` passes.

### QUA-007 — Feed deletion silently removes articles from existing editions

- **Severity:** P2 · **Confidence:** Confirmed
- **Location:** `migrations/0001_initial_schema.sql` (`articles.feed_id ... ON DELETE CASCADE`, `edition_articles.article_id ... ON DELETE CASCADE`); `src/feeds/mod.rs:393`
- **Requirement:** R34. "A feed deletion must not silently corrupt edition references."
- **Expected:** Edition membership is preserved, or deletion is blocked or warned about.
- **Actual:** Articles owned only by the deleted feed cascade-delete, and their `edition_articles` rows go with them.
- **Reproduction:** `r21`: an edition with 1 article; deleting the feed leaves 0 `edition_articles` rows.
- **Impact:** Latent today because editions aren't generated until a later phase, but published editions would silently lose content. The schema inherited from Phase 1 should be fixed before editions ship.
- **Recommended fix:** A migration that rebuilds `articles` with `feed_id` nullable and `ON DELETE SET NULL` (or `RESTRICT`), or soft-deletes feeds, or blocks deletion while edition references exist.
- **Verification:** `r21` passes.

### QUA-008 — Feed polling is blocked while the extraction queue drains

- **Severity:** P2 · **Confidence:** Code-supported
- **Location:** `src/feeds/scheduler.rs:201-240` (tick awaits `drain_eligible_extractions`), `:166-196` (lockstep batches: claim N, wait for all N, repeat)
- **Requirement:** R15
- **Expected:** Due feeds are polled on schedule regardless of extraction backlog.
- **Actual:** Each tick fetches at most `fetch_concurrency` feeds, then drains the extraction queue until empty before the next tick. One slow article (45 s timeout × 3 attempts + backoff) holds its whole batch. Example: 40 unreachable articles at concurrency 4 ≈ 10 batches × ~2.5 min ≈ 25 min with no feed polling.
- **Impact:** Schedule drift and delayed ingestion under backlog. Also, at most `fetch_concurrency` feeds are polled per tick, so 100 feeds needed 25 ticks in r27.
- **Recommended fix:** Run feed scheduling and extraction as two independent loops. Use a continuous worker pool (a semaphore plus refill as each job finishes) instead of lockstep batches.
- **Verification:** A test with slow article fixtures shows feed runs starting on time during a drain.

### QUA-009 — Phase 2 commit removed binding rules from `AGENTS.md`

- **Severity:** P2 · **Confidence:** Confirmed
- **Location:** `AGENTS.md` (diff `f0ec769..66980ec`)
- **Requirement:** Project governance
- **Expected:** The implementation does not weaken its own governing constraints.
- **Actual:** Removed: the whole "Docker and Portainer Requirements" section (multi-stage, UID 10001, HEALTHCHECK, no privileged or socket mounts, `/data`); "Scope Management" (no stubbed or fake features, honest documentation of unimplemented work); "Minimal Dependencies"; "Appropriate Indexes"; "Non-Blocking Operations"; the security items "Secret Protection", "Parameterized Queries", "Safe Error Responses", and "Do not trust X-Forwarded-*"; the explicit SSRF host list; and "Inspect ./repos/ ... adapt only MIT code with attribution".
- **Impact:** Future agents lose constraints that the current code still relies on.
- **Recommended fix:** Restore the removed sections verbatim, and keep the new "Persistent SQLite Extraction Queue" section as an addition.
- **Verification:** `git diff f0ec769 -- AGENTS.md` shows only additions.

### QUA-010 — No claim token or lease: a stale worker can overwrite a reclaimed job's result

- **Severity:** P3 · **Confidence:** Confirmed (mechanism); low likelihood in the documented single-instance deployment
- **Location:** `src/articles/mod.rs:582-784` (`UPDATE ... WHERE id = ?` with no status or claim check); `:476-491` (blanket reset at startup)
- **Expected:** Completion is conditional on the worker still owning the claim.
- **Actual:** In `r19`, job claimed by A → recovery → reclaimed by B → B completes (`extracted`) → A completes `Failed` → the final state is `failed` / `failed`.
- **Impact:** Not reachable in one process, since recovery runs only at startup. It is reachable if two containers ever share `/data` (for example, overlapping Portainer redeploys). The single-instance assumption is not documented.
- **Fix:** Add a `claim_token` (or use `attempts`) and `WHERE id = ? AND status = 'processing' AND claim_token = ?` on completion. Document single-instance operation.

### QUA-011 — Crash-looping ("poison") jobs are reclaimed without limit

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/articles/mod.rs:504-533` (claim ignores `attempts >= max_attempts`); `:476-491`
- **Actual:** In `r20`, 10 claim → crash → recover cycles produced 10 claims with `max_attempts = 3`. An article that OOM-kills the process would crash-loop the container forever.
- **Fix:** Add `AND attempts < max_attempts` to the claim, and mark exhausted jobs `failed` during recovery.

### QUA-012 — SSRF defense-in-depth gaps

- **Severity:** P3 · **Confidence:** Confirmed (function level); Potential (exploitability)
- **Location:** `src/feeds/validation.rs:174-199`; `src/feeds/fetcher.rs:234-261`
- **Actual:**
  1. `is_disallowed_ip` allows NAT64 `64:ff9b::/96` (e.g., `64:ff9b::7f00:1`, `64:ff9b::a9fe:a9fe`), 6to4 `2002::/16` embedding private IPv4, and site-local `fec0::/10` (`r02`). This is exploitable only on hosts with NAT64 or 6to4 routing.
  2. The fetcher never re-checks addresses returned by `HostResolver`; all IP filtering lives in the resolver implementation (`r04`). A future resolver, or a test double reused in production, silently disables SSRF protection.
  3. The `reqwest` client doesn't call `.no_proxy()`, so `HTTP(S)_PROXY`/`ALL_PROXY` in the container environment would route requests through a proxy that resolves names itself, bypassing pinning (code-supported).
  4. `resolver.resolve()` isn't covered by `request_timeout`.
- **Fix:** Block the three prefixes (decode embedded IPv4 for NAT64 and 6to4). Call `is_disallowed_ip` on resolver output inside `execute_single_fetch`. Add `.no_proxy()`. Wrap resolution in `tokio::time::timeout(remaining_timeout, ...)`.

### QUA-013 — Public IPv6-literal URLs cannot be fetched

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/feeds/fetcher.rs:221,235`; `src/feeds/validation.rs:32`
- **Actual:** `Url::host_str()` returns `[2606:4700:4700::1111]` with brackets, and `lookup_host` fails ("nodename nor servname provided") (`r03`).
- **Fix:** Use `Url::host()` and match on `Host::Ipv6`, or strip the brackets before resolving.

### QUA-014 — Non-HTML responses are stored as "completed" full text

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/articles/extractor.rs:86-98`
- **Actual:** An `application/pdf` body was wrapped in `<div>` and marked `Completed` (`r15`).
- **Fix:** Require `text/html` or `application/xhtml+xml` (or sniff) before readability; otherwise fall back or fail as non-retryable.

### QUA-015 — Entry limit keeps the first N entries in document order, not the newest

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/feeds/parser.rs:116-127`
- **Actual:** In an oldest-first feed with limit 2, the two oldest were kept and the two newest dropped (`r10`).
- **Fix:** Sort by `published`/`updated` (descending, falling back to document order) before `take(max_entries)`.

### QUA-016 — Canonicalizer re-serializes every query string

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/articles/normalizer.rs:89-109`
- **Actual:** `?123` becomes `?123=` (`r11`), and percent-encoding is normalized via `form_urlencoded`. This can change the fetched URL on servers that distinguish these forms. Distinct `?id=` values are correctly preserved, and the tracking list is conservative and documented in code.
- **Fix:** Rewrite the query only when a tracking parameter was actually removed, and preserve raw key/value text for kept pairs.

### QUA-017 — Graceful shutdown always hits the 10 s timeout and leaves work `processing`

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `src/main.rs:78-95`; `src/feeds/scheduler.rs:166-196`
- **Actual:** In the probe, SIGINT during 4 in-flight extractions took exactly 10 s, left 4 jobs `processing` (recovered on the next start), and still logged "shut down cleanly". The drain loop keeps claiming new batches after shutdown is signaled. Docker's default stop grace period is also 10 s, so SIGKILL races the timeout.
- **Fix:** Check the shutdown flag before each claim, cancel in-flight fetches via a `CancellationToken`, and reset the cancelled jobs to `queued` before exit. Log accurately. Set `stop_grace_period: 20s` in compose.

### QUA-018 — CPU-heavy parsing runs on Tokio worker threads

- **Severity:** P3 · **Confidence:** Code-supported
- **Location:** `src/articles/extractor.rs:47-65`, `src/feeds/parser.rs:57-136`, `src/web/mod.rs:846-851`
- **Actual:** Readability, ammonia, and feed-rs run inline in async tasks, with up to 15 MiB per article and 32 concurrent extractions. This can stall HTTP handlers on small homelab CPUs. It contradicts the removed "Non-Blocking Operations" rule (QUA-009).
- **Fix:** Use `tokio::task::spawn_blocking` for parse, extract, and sanitize.

### QUA-019 — Raw `BEGIN IMMEDIATE` on pooled connections

- **Severity:** P3 · **Confidence:** Potential
- **Location:** `src/articles/mod.rs:500-501, 587-588`
- **Actual:** sqlx doesn't track transactions started with raw SQL. If the future is cancelled between `BEGIN` and `COMMIT`, or `COMMIT` errors (the `?` returns early without `ROLLBACK`), the connection goes back to the pool holding an open write transaction, which can block every writer.
- **Fix:** Use `pool.begin_with("BEGIN IMMEDIATE")` (sqlx 0.8.6 `Pool::begin_with`), which rolls back on drop.

### QUA-020 — The test suite missed the defects above

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `tests/integration_tests.rs`
- **Observations:**
  - 10 large integration tests and 0 unit tests.
  - The "second worker attempting to claim simultaneously" check (`:1015-1021`) is sequential.
  - The DNS-rebinding test (`:276-319`) exercises the IP check inside the test's own `FixtureResolver`, not `SystemDnsResolver` or the fetcher.
  - Crash recovery is simulated by calling `recover_*` directly, with no process termination.
  - No test runs two writers concurrently, which is why QUA-001 passed CI.
  - The migration test applies 0001 with `raw_sql`, so it doesn't reproduce a real `_sqlx_migrations` history (r26 does, and passes).
- **Fix:** Adopt reviewer tests r01–r29 (adjusted) and add real concurrency and crash tests.

### QUA-021 — Documentation inaccuracies

- **Severity:** P3 · **Confidence:** Confirmed
- **Items:**
  - README: `QUIRE_FETCH_CONCURRENCY` is described as "Maximum concurrent feed HTTP requests", which is false for manual triggers (QUA-003).
  - `docs/architecture.md:116` and `roadmap.md:33` claim an XXE/DTD guard; it is actually a text filter with false positives (QUA-005).
  - That fallback is terminal and skips retries (QUA-002) is undocumented.
  - The single-instance requirement (QUA-010) is undocumented.
  - `QUIRE_FETCH_MIN_INTERVAL_MINUTES` accepts 1–4 (`config.rs:975-985`), but scheduling clamps to 5 (`feeds/mod.rs:484`) and the DB `CHECK` requires ≥ 5.
  - Changing a feed's interval takes effect only after its next fetch (`update_feed` doesn't recompute `next_scheduled_fetch_at`).

### QUA-022 — Phase 1 `pending` articles get no extraction job after upgrade

- **Severity:** P3 · **Confidence:** Confirmed
- **Location:** `migrations/0002_feed_ingestion.sql` (no backfill into `article_extraction_jobs`)
- **Actual:** In `r26`, the legacy `pending` article had 0 jobs after the upgrade and stays pending until a manual retry. Phase 1 had no ingestion, so real impact is likely nil.
- **Fix:** In a new migration, `INSERT OR IGNORE INTO article_extraction_jobs (article_id) SELECT id FROM articles WHERE extraction_status = 'pending'`. Do not edit 0002.

---

## 6. SQLite Queue Assessment

| Property | Result | Evidence |
|---|---|---|
| Durability | **Pass** | Jobs live only in `article_extraction_jobs`. SIGKILL with 4 `processing` + 16 `queued` → all 20 `completed` after restart. Docker recreate with seeded `processing` jobs → recovered and processed. |
| Atomic job creation | **Pass** | `enqueue_extraction_job_in_tx` runs in the same transaction as the article insert (`articles/mod.rs:331-372`); a rollback discards both. |
| Atomic claims | **Pass** | `BEGIN IMMEDIATE` + `UPDATE ... WHERE status = 'queued'`; `r16`: 16 concurrent claimers, 300 jobs → 300 unique, 0 errors. |
| Duplicate jobs | **Pass** | `UNIQUE(article_id)`; enqueue skips `processing` and `queued`; r27 repeat pass created no new work. |
| Retry persistence | **Pass with caveats** | attempts, `last_error`, and `next_eligible_at` persist across recreation (Docker: attempt 2 → +60 s → attempt 3 → `failed`). Caveats: QUA-002 (fallback bypasses retries) and QUA-011 (crash loops are unbounded). |
| Lease / ownership | **Weak** | No lease or claim token; recovery is startup-only (QUA-004, QUA-010). |
| Backpressure | **Pass** | At most `extraction_concurrency` tasks in memory; the backlog stays in SQLite. |
| Concurrency with other writers | **Fail** | QUA-001: feed persistence fails against concurrent writers. |

Failure tests from the brief (§5.8):

- **Covered at runtime:** crash after claim, crash during HTTP retrieval (SIGKILL while the fixture delayed responses), duplicate creation, concurrent claims, retry exhaustion, recovery after container recreation.
- **Code-only:** crash after extraction but before persistence (same as "after claim" — the job stays `processing` and is recovered), DB write failure (QUA-004).

---

## 7. Security Assessment

| Area | Result | Notes |
|---|---|---|
| URL validation | Pass | Schemes, credentials, localhost, `.local`/`.internal`, RFC1918, link-local, CGNAT, metadata, and mapped IPv4 are rejected; numeric forms (`0x7f000001`, `2130706433`, `127.1`) are normalized by `url` and blocked. Applied at request time and on every hop. |
| DNS rebinding | Pass | Resolved per hop; addresses pinned with `resolve_to_addrs`. Caveats: proxy env and resolver trust (QUA-012). |
| Redirects | Pass | `Policy::none()` plus manual validation; 9 restricted targets blocked with 0 internal hits (r01). |
| Feed/article policy consistency | Pass | Both use `fetch_url_with_retries`; no other outbound path. |
| Response limits | Pass | Streaming cap on decompressed bytes: a 200 MB gzip bomb was rejected at 1 MiB in 7 ms; slow-drip bodies time out at `request_timeout`. |
| Timeouts | Pass / P3 gap | Connect and total timeouts work; DNS resolution is unbounded (QUA-012). |
| XML | Pass | No entity expansion or XXE (r09); the pre-filter is harmful (QUA-005). |
| HTML / stored XSS | Pass | ammonia allow-list; 20 vectors (script, event handlers, `javascript:`/`data:`, entity-obfuscated schemes, svg, math, iframe, form, style, base, meta, noscript mXSS) all removed (r12). Sanitized before storage and again at render; the only `|safe` is the re-sanitized body. Askama escapes everything else. |
| CSRF | Pass | Every POST verifies the double-submit cookie; bad token → 403, missing cookie → 403, missing field → 422; state changes are never GET (405). CSRF is not authentication — the network-exposure warning stands. |

---

## 8. Database Assessment

- **Migrations:** 0002 is additive (ALTER/CREATE only) and backfills `article_feed_sources`. A real sqlx-migrated Phase 1 DB upgraded cleanly: all rows, edition links, and timestamps preserved; 0 FK violations; restart idempotent (r26).
- **Pragmas:** WAL, `foreign_keys = ON`, and `busy_timeout = 10 s` are set through `SqliteConnectOptions`, so they apply to every pooled connection (`db/mod.rs:66-71`). `synchronous = NORMAL` can lose the last commits on power loss (not on a process crash); acceptable, but should be documented.
- **Transaction boundaries:** No transaction spans network I/O, DNS, parsing, or retry sleeps. Write transactions use deferred mode (QUA-001).
- **Deduplication:** Enforced by DB constraints; 0 duplicates under load.
- **Feed deletion:** QUA-006, QUA-007.
- **Growth:** `feed_fetch_runs` pruned to 100 per feed. Articles and jobs are never pruned (retention is Phase 3 scope). 1,000 articles ≈ 6.0 MB including WAL.
- **Queries:** The status filter evaluates a correlated `EXISTS` per row inside both `COUNT` and the page query, so it can't use an index. The dashboard issues 10 separate `COUNT`s. Fine at 1,000 rows; revisit in Phase 3.

## 9. Scheduler Assessment

- Startup recovery runs before the loop; scheduler tick errors are logged and the loop continues (a failure can't kill ingestion).
- Next fetch time = completion + interval, so drift equals fetch duration; failures back off ×2ⁿ, capped at 16× / 7 days, with no retry storm.
- After days offline, every feed is due at once and is processed `fetch_concurrency` per tick.
- Runtime config is reloaded every tick.
- Defects: QUA-001, QUA-003, QUA-004, QUA-008, QUA-017.
- `tokio::spawn` inventory:
  - The background loop handle is kept and awaited with a timeout.
  - Manual-fetch tasks are detached and unobserved.
  - `JoinSet` tasks have only `is_ok()` counted; panics are not logged.

## 10. Browser and Docker Assessment

**Browser** (exercised via curl against the container):

- Feed CRUD, toggle, fetch, settings, article detail, 404s, and invalid pagination (`page=0`, `page_size=100000` → clamped) all behave correctly.
- Settings: env-locked values are refused; invalid values (0, negative, non-numeric, overflow) → 400 with a clear message.
- Error responses don't leak internals.

**Docker:**

- `docker compose config` and `docker compose build` pass. Image: 175 MB on disk / 38.2 MB content; runs as `uid=10001(quire)`; healthy in about 9 s.
- `/health` checks only the DB and stayed healthy while every fetch failed.
- Restart and `--force-recreate` preserved feeds, settings, fetch history, and jobs.
- Crash recovery after recreation is verified (§6).
- Portainer was not deployed. `compose.yaml` defaults to `image: quire:local` with a `build:` section, so a Portainer stack must set `QUIRE_IMAGE` or use the README snippet. The README documents this.
- Testing used the isolated project `quirereview`, port 18080, and a dedicated volume, all removed afterwards.

---

## 11. Test Results

### Required commands (repository at `66980ec`)

| Command | Result |
|---|---|
| `cargo fmt --all -- --check` | Pass |
| `cargo clippy --all-targets --all-features -- -D warnings` | Pass, 0 warnings |
| `cargo test` | Pass: 10 integration tests passed, 0 failed, 0 ignored; 0 unit tests |
| `cargo build --release` | Pass |
| `docker compose config` | Pass |
| `docker compose build` | Pass |
| `cargo audit` | **Not run** — `cargo-audit` and `cargo-deny` aren't installed. The dependency vulnerability status is unverified. |

### Reviewer tests (run in a temporary worktree; sources kept in `docs/reviews/phase-2-artifacts/`)

The artifacts are not compiled as part of the crate. Full rerun instructions are in Appendix A.

| File | Purpose |
|---|---|
| `review_tests.rs` | 29 reviewer tests (r01–r29) |
| `crashprobe.rs` | Example binary that runs the real `IngestionCoordinator` against a local fixture, for SIGKILL/SIGINT tests |
| `fixture.py` | Python stdlib HTTP fixture: a feed with N items plus article pages with a configurable delay |
| `compose.review.yaml` | Isolated Compose override: container `quire-review`, port 18080, `QUIRE_FETCH_ENABLED=false` |
| `docker1.sh` | Docker runtime, CSRF, SSRF, settings, and restart checks |
| `docker2.sh` | Docker crash-recovery check after container recreation (run after `docker1.sh`) |

Each test asserts the correct behavior, so a failure documents a defect.

| Test | Result | Finding |
|---|---|---|
| r01 redirect to 9 restricted targets | pass | — |
| r02 IPv6 transition prefixes | **fail** | QUA-012 |
| r03 bracketed IPv6 literal | **fail** | QUA-013 |
| r04 fetcher re-checks resolver output | **fail** | QUA-012 |
| r05 gzip bomb | pass | — |
| r06 slow drip | pass | — |
| r07 `PUBLIC "` text in feed | **fail** | QUA-005 |
| r08 RSS 0.91 DOCTYPE | **fail** | QUA-005 |
| r09 entity / XXE past 64 KB | pass (not expanded) | — |
| r10 entry limit keeps newest | **fail** | QUA-015 |
| r11 canonicalization | **fail** (bare key only) | QUA-016 |
| r12 XSS vectors | pass | — |
| r13 / r14 transient failure retried | **fail** | QUA-002 |
| r15 PDF not marked full text | **fail** | QUA-014 |
| r16 concurrent claims | pass (300/300 unique) | — |
| r17 concurrent persists + queue writers | **fail** (17/18 batches) | QUA-001 |
| r18 stuck job recoverable in-process | **fail** | QUA-004 |
| r19 stale worker overwrite | **fail** | QUA-010 |
| r20 poison job bounded | **fail** (10 claims) | QUA-011 |
| r21 edition membership kept | **fail** | QUA-007 |
| r22 delete with GUID reassignment | **fail** | QUA-006 |
| r23 manual fetch-all concurrency | **fail** (10 vs 2) | QUA-003 |
| r24 4-way persists | **fail** (30/40) | QUA-001 |
| r25 production fetch path, 8 feeds | **fail** (6/8) | QUA-001 |
| r26 Phase 1 → 2 upgrade + restart | pass | — |
| r27 load: 100 feeds / 1,000 articles | pass (measurement) | — |
| r28 load: 100 failing articles | pass (measurement; all fallback) | QUA-002 |
| r29 contention via production paths | measurement: tick 12/12 ok; fetch-all 8–9/12 failed | QUA-001 |

**Totals:** 29 tests, 10 passed, 19 failed (each failure maps to a finding).

**Runtime experiments:**

- SIGKILL crash/restart probe: pass.
- SIGINT shutdown: 10 s, 4 jobs left recoverable.
- Docker: restart, recreate, recovery, retry, CSRF, SSRF, and settings checks: pass.

## 12. Performance Assessment

Debug build, Apple Silicon, local fixture:

| Scenario | Result |
|---|---|
| 100 feeds × 10 entries, first pass (scheduler ticks incl. extraction) | 5.3 s; 100/100 runs succeeded; 1,000/1,000 extracted; 0 duplicates; peak RSS ≈ 26 MB; DB + WAL ≈ 6.0 MB |
| Repeat ingestion of unchanged feeds (200 runs) | 4.0 s; 0 new articles; 0 re-extractions |
| 100 articles returning HTTP 500 (extraction concurrency 8) | 40.8 s; all 100 finalized as `summary_fallback` (QUA-002) |
| Concurrent persistence | see QUA-001 |

The extraction backlog stays bounded in memory, and fetch history is bounded. Each request hop builds a new `reqwest::Client` (no connection reuse). That's acceptable at homelab scale.

## 13. Recommended Corrections (priority order)

1. **QUA-001** — `BEGIN IMMEDIATE` for all write transactions (fold in QUA-019).
2. **QUA-003 + QUA-008** — one coordinator-owned semaphore for feed fetches; separate feed and extraction loops; continuous worker pool.
3. **QUA-002** — retry before fallback.
4. **QUA-004 + QUA-010 + QUA-011 + QUA-017** — lease/claim-token model, periodic lease recovery, bounded reclaims, cancellable shutdown.
5. **QUA-005** — remove the XML text filter.
6. **QUA-006 + QUA-007** — feed deletion integrity.
7. **QUA-009** — restore the removed `AGENTS.md` rules.
8. P3 items (QUA-012 through QUA-016, QUA-018, QUA-020 through QUA-022).

## 14. Acceptance Decision

**REJECT.**

QUA-001 is a confirmed P1. Under default configuration, concurrent ingestion paths fail with `SQLITE_BUSY` and drop whole feed batches; the UI's Fetch All loses most feeds on every use. Everything else is P2/P3:

- The queue's durability and claim atomicity are good and verified.
- The main security controls held under test.

After QUA-001 is fixed, with QUA-002 through QUA-004 corrected and covered by tests, Phase 2 should be re-reviewed for **ACCEPT WITH CONDITIONS**.

**Verification gaps:**

- `cargo audit` was not run.
- Portainer was not deployed.
- No multi-arch (arm64) image was tested.
- Behavior on slow storage (where the 50 ms stagger in scheduled ticks is least effective) was not measured.
