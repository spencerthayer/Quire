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

---

# Appendices

## Appendix A — Reproducing the Review

### A.1 Environment used

| Item | Value |
|---|---|
| Host | macOS 26.5.2, Apple M5 Pro (arm64) |
| Toolchain | rustc 1.97.1 (2026-07-14), cargo 1.97.1 |
| Docker Engine | 29.4.2 (Docker Desktop) |
| SQLite (via `libsqlite3-sys` 0.30.1, bundled) | crate-bundled; host `sqlite3` CLI 3.51.0 used only to inspect databases |
| Python | Homebrew `python3` (stdlib only), run with `-I` |
| Not available | `cargo-audit`, `cargo-deny` |

### A.2 Rerunning the reviewer tests

```bash
# 1. Disposable worktree; never run against your real data directory
git worktree add --detach /tmp/quire-review HEAD
cd /tmp/quire-review
cp docs/reviews/phase-2-artifacts/review_tests.rs tests/
mkdir -p examples && cp docs/reviews/phase-2-artifacts/crashprobe.rs examples/

# 2. Gzip-bomb fixture used by r05 (200 MB of zeros, ~190 KB compressed).
#    r05 reads $CARGO_MANIFEST_DIR/../bomb.gz, i.e. the worktree's parent directory.
head -c 200000000 /dev/zero | gzip -9 > ../bomb.gz

# 3. Run (each test asserts CORRECT behavior; failures document defects)
cargo test --test review_tests -- --test-threads=4 --nocapture

# 4. Measurement tests (long-running, print numbers rather than assert)
cargo test --test review_tests r27 r28 r29 -- --nocapture

# 5. Clean up
cd - && git worktree remove --force /tmp/quire-review
```

Reviewer tests inject a `HostResolver` (`AnyHost`) that maps every hostname to the local fixture without IP checks. This simulates a public DNS answer and lets the real fetcher, redirect logic, and limits run against `127.0.0.1` without touching third-party hosts. Tests that probe the production resolver (r03) use `SystemDnsResolver` directly.

### A.3 Rerunning the crash-recovery probe

```bash
# In the worktree from A.2 (crashprobe.rs already copied to examples/)
cargo build --example crashprobe
P=$PWD/target/debug/examples/crashprobe
C=$(mktemp -d); mkdir -p $C/data; echo 30 > $C/delay; PORT=18931
python3 -I docs/reviews/phase-2-artifacts/fixture.py $PORT $C/delay 20 &   # 20 articles, 30 s delay each
$P $C/data $PORT &  PR=$!
# wait until 4 jobs are 'processing'
sqlite3 $C/data/q.db "select status,count(*) from article_extraction_jobs group by status"
kill -9 $PR                                   # abrupt termination
echo 0 > $C/delay                             # make articles fast
$P $C/data $PORT &  PR=$!                     # restart; recovery runs at startup
sqlite3 $C/data/q.db "select status,count(*) from article_extraction_jobs group by status"
kill -INT $PR                                 # graceful path (see QUA-017)
```

### A.4 Rerunning the Docker checks

```bash
QUIRE_IMAGE=quire:review docker compose -p quirereview build
bash docs/reviews/phase-2-artifacts/docker1.sh   # up, health, uid, CSRF, SSRF, settings, restart
bash docs/reviews/phase-2-artifacts/docker2.sh   # stop, seed interrupted jobs, recreate, verify recovery
docker compose -p quirereview -f compose.yaml -f docs/reviews/phase-2-artifacts/compose.review.yaml down -v
docker rmi quire:review
```

The override renames the container to `quire-review`, publishes port 18080, and sets `QUIRE_FETCH_ENABLED=false` so the scheduler makes no outbound feed requests. Seeded article URLs use the reserved `.invalid` TLD (RFC 6761), which never resolves, so recovery and retry are exercised without contacting any real host. `docker2.sh` needs the host `sqlite3` CLI and BSD `tar` (for `--uid/--gid`).

---

## Appendix B — Methodology

1. **Inventory:** `git log`, `git diff --stat/--name-status f0ec769..HEAD`, plus a full read of every changed Rust module, the migration, templates touched by Phase 2, Dockerfile, compose, workflows, `.env.example`, README, and docs.
2. **Static analysis:** a line-by-line read of `src/feeds/{validation,fetcher,parser,scheduler,mod}.rs`, `src/articles/{mod,extractor,normalizer,sanitizer}.rs`, `src/db/mod.rs`, `src/main.rs`, and the security-relevant parts of `src/web/mod.rs` and `src/config.rs`.
3. **Required validation commands:** run unmodified against `66980ec`.
4. **Reviewer tests (r01–r29):** written to assert correct behavior, so a failing test is evidence of a defect. Every failure was re-read to confirm it failed for the intended reason, not a test bug. One reviewer test bug was found and corrected (the first r27 run used `https://` article links against a plain-HTTP fixture, so all extractions failed with TLS errors; the corrected run is reported).
5. **Runtime experiments:** a SIGKILL/SIGINT crash probe running the real `IngestionCoordinator`; a Docker container with an isolated project, port, and volume; and load and contention measurements.
6. **Confidence labels:**
   - *Confirmed*: reproduced by a test or runtime experiment.
   - *Code-supported*: established by source inspection, not reproduced.
   - *Potential*: plausible, needs further verification.
7. **Isolation:** no production source was modified, nothing was committed or pushed, and every container, volume, image, and worktree created for the review was removed. The only repository change is this `docs/reviews/` directory.

---

## Appendix C — Inventories

### C.1 HTTP routes (`src/web/mod.rs:68-91`)

| Method | Path | State change | CSRF verified | Verified at runtime |
|---|---|---|---|---|
| GET | `/` | no | n/a | yes |
| GET | `/health` | no | n/a | yes (DB-only health) |
| GET / POST | `/feeds` | POST creates feed | yes (`:346`) | 303 valid; 403 bad token; 403 no cookie; 422 missing field; 400 SSRF URL |
| POST | `/feeds/fetch-all` | spawns fetches | yes (`:644`) | via r29 (function level) |
| GET / POST | `/feeds/{id}` | POST updates feed | yes (`:560`) | — |
| GET | `/feeds/{id}/edit` | no | n/a | — |
| POST | `/feeds/{id}/fetch` | spawns fetch | yes (`:628`) | 404 for missing feed; GET → 405 |
| POST | `/feeds/{id}/toggle` | toggles enabled | yes (`:655`) | — |
| POST | `/feeds/{id}/delete` | deletes feed (requires `confirm=yes`) | yes (`:668`) | — |
| GET | `/articles` | no | n/a | `page=0`, `page_size=100000`, `feed_id=abc`, `status=<script>` → 200, clamped/escaped |
| GET | `/articles/{id}` | no | n/a | 404 for missing |
| POST | `/articles/{id}/extract` | enqueues job | yes (`:886`) | — |
| GET | `/editions` | no | n/a | — |
| GET / POST | `/settings` | POST writes settings | yes (`:1008`) | env-locked refused; invalid → 400; valid → 303 |
| GET | `/static/*` | no | n/a | — |

CSRF design: a double-submit cookie `quire_csrf` (32 random bytes, hex; `HttpOnly; SameSite=Strict; Path=/`) compared to the `csrf_token` form field. The comparison is not constant-time; this is immaterial for a double-submit token and not reported as a finding.

### C.2 `tokio::spawn` / task inventory

| Location | Spawned work | Owner / handle | Failure observation | Shutdown behavior |
|---|---|---|---|---|
| `scheduler.rs:247` | Background scheduler loop | `JoinHandle` held by `main` | errors per tick logged (`warn!`); panic is unobserved until the handle is awaited | `main` awaits with a 10 s timeout, then abandons it |
| `scheduler.rs:104` | Manual feed fetch (one per click / per feed in Fetch All) | **handle discarded** | none; an early `return` on config error leaves the feed claimed | not awaited; dropped at runtime shutdown |
| `scheduler.rs:179` | Extraction job (`JoinSet`, batch of `extraction_concurrency`) | local `JoinSet` | `JoinError` counted as "not processed", not logged | runs until the batch completes; the drain loop keeps claiming |
| `scheduler.rs:219` | Scheduled feed fetch (`JoinSet`, ≤ `fetch_concurrency`) | local `JoinSet` | same as above | same as above |

### C.3 SQLite write-transaction inventory

| Function | Location | Begin mode | First statement | Risk under concurrent writers |
|---|---|---|---|---|
| `persist_discovered_entries` | `articles/mod.rs:148` | deferred `BEGIN` | `SELECT` | **immediate `SQLITE_BUSY` (QUA-001)** |
| `enqueue_manual_article_extraction` | `articles/mod.rs:461` | deferred | `SELECT` | same pattern (lower volume) |
| `recover_interrupted_extractions` | `articles/mod.rs:477` | deferred | `UPDATE` | waits on busy_timeout (OK) |
| `claim_next_extraction_job` | `articles/mod.rs:500-501` | raw `BEGIN IMMEDIATE` | `SELECT` | OK; raw SQL is not tracked by sqlx (QUA-019) |
| `complete_extraction_job` | `articles/mod.rs:587-588` | raw `BEGIN IMMEDIATE` | `SELECT`/`UPDATE` | OK; QUA-019 |
| `create_feed` / `update_feed` / `set_feed_enabled` | `feeds/mod.rs:223/273/325` | deferred | `INSERT`/`UPDATE` | waits (OK) |
| `delete_feed` | `feeds/mod.rs:358` | deferred | `UPDATE` (with subquery reads) | waits (OK); QUA-006 constraint issue |
| `try_claim_feed_fetch` | `feeds/mod.rs:415` | deferred | `UPDATE` | waits (OK) |
| `complete_feed_fetch_run` | `feeds/mod.rs:503` | deferred (reads feed **before** the tx at `:480`) | `UPDATE` | waits (OK); read-modify-write race on `website_url`/`description` with a concurrent user edit (minor, not separately reported) |
| `recover_interrupted_fetches` | `feeds/mod.rs:640` | deferred | `UPDATE` | waits (OK) |
| `upsert_settings` | `db/mod.rs:228` | deferred | `DELETE`/`INSERT` | waits (OK) |

Rule of thumb: a deferred transaction whose first statement is a write takes the write lock immediately and honors `busy_timeout`. One that reads first and later writes cannot wait safely in WAL mode, and SQLite fails it with `SQLITE_BUSY`.

### C.4 Configuration matrix

| Env var | Default | Accepted range | DB-overridable | Invalid env value | Notes |
|---|---|---|---|---|---|
| `QUIRE_FETCH_ENABLED` | `true` | bool | yes | startup error | Docker: locked when set; UI override refused |
| `QUIRE_FETCH_INTERVAL_MINUTES` | 60 | min_interval..=10080 | yes | startup error | UI: −5, `abc`, 10²⁰ → 400 |
| `QUIRE_FETCH_MIN_INTERVAL_MINUTES` | 5 | 1..=10080 | no | startup error | values 1–4 not honored (QUA-021) |
| `QUIRE_FETCH_CONCURRENCY` | 4 | 1..=32 | yes | startup error | not applied to manual fetches (QUA-003) |
| `QUIRE_EXTRACTION_CONCURRENCY` | 4 | 1..=32 | yes | startup error | UI: 0 → 400 "must be between 1 and 32" |
| `QUIRE_FEED_REQUEST_TIMEOUT_SECONDS` | 30 | 5..=300 | yes | startup error | — |
| `QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS` | 45 | 5..=300 | yes | startup error | — |
| `QUIRE_MAX_FEED_RESPONSE_BYTES` | 10 MiB | 64 KiB..=100 MiB | yes | startup error | enforced on decompressed bytes (r05) |
| `QUIRE_MAX_ARTICLE_RESPONSE_BYTES` | 15 MiB | 64 KiB..=100 MiB | yes | startup error | — |
| `QUIRE_MAX_FEED_ENTRIES` | 200 | 1..=5000 | yes | startup error | keeps first N, not newest (QUA-015) |
| `QUIRE_FETCH_MAX_ATTEMPTS` | 3 | 1..=10 | yes | startup error | also used as job `max_attempts` |
| `QUIRE_FETCH_RETRY_INITIAL_SECONDS` | 2 | 1..=120 | no | startup error | — |
| `QUIRE_FETCH_RETRY_MAX_SECONDS` | 30 | 1..=600, ≥ initial | no | startup error | caps `Retry-After` |

Not configurable: connect timeout 10 s, max redirects 5, fetch-history retention 100 per feed, scheduler poll 30 s, pool size 8, busy_timeout 10 s.

### C.5 Dependencies added in Phase 2

| Crate | Locked version | Purpose | Assessment |
|---|---|---|---|
| `ammonia` | 4.2.4 | HTML sanitization | appropriate, widely used allow-list sanitizer |
| `dom_query` | 0.29 | DOM preprocessing / text extraction | also a `dom_smoothie` dependency; justified |
| `encoding_rs` | 0.8 | charset decoding | appropriate |
| `futures-util` | 0.3 | `StreamExt` for body streaming | appropriate |
| `reqwest` features `gzip`, `deflate`, `brotli`, `stream` | 0.13 | compression + streaming | limit applies to decompressed size (verified) |

Pre-existing (Phase 1), still unused in `src/`: `epub-builder` (Phase 5 scope). `cargo tree -d` shows duplicate major versions only in transitive crates (`base64` 0.22/0.23, `hashbrown` 0.15/0.17, `getrandom` 0.3/0.4, `phf` 0.12/0.14, `foldhash` 0.1/0.2); not a defect. No `unsafe` in `src/`. Advisory status is **unverified** (no `cargo audit`).

### C.6 CI / supply chain (`.github/workflows/`)

| Item | Observation |
|---|---|
| Permissions | `ci.yml`: `contents: read`; `publish-container.yml`: `contents: read`, `packages: write`. Least privilege, OK |
| Action pinning | tags only (`actions/checkout@v4`, `docker/*@v3/v5/v6`, `dtolnay/rust-toolchain@stable`, `Swatinem/rust-cache@v2`), no SHA pins. Recommended: pin to commit SHAs |
| Reproducibility | `cargo test --locked`, `cargo build --release --locked`; Dockerfile `--locked`; base image `rust:1.97-slim-bookworm` is tag-pinned, not digest-pinned |
| Secrets | only `GITHUB_TOKEN` |
| Publishing | `linux/amd64,linux/arm64` via QEMU; tags `sha-<long>`, `<short-sha>`, branch, semver, `latest` on default branch |
| Gaps | no `cargo audit`/`cargo deny` step; no SBOM or provenance attestation; publish runs on every push to `main` independently of CI success |

---

## Appendix D — Evidence for Each Defect

### D.1 QUA-001: deferred transaction that reads before writing

```rust
// src/articles/mod.rs:148 — deferred transaction
let mut tx = pool.begin().await?;
for entry in entries {
    let outcome = upsert_single_entry(&mut tx, feed_id, entry, i64::from(max_attempts)).await?;
    ...
}
tx.commit().await?;

// src/articles/mod.rs:185-201 — first statement inside the tx is a read
let existing = sqlx::query_as::<_, ExistingArticleRow>(
    "SELECT id, feed_id, external_guid, ... FROM articles WHERE canonical_url = ?",
)
```

```rust
// src/feeds/scheduler.rs:220-223 — the only thing separating concurrent scheduled persists
if idx > 0 {
    tokio::time::sleep(Duration::from_millis((idx as u64) * 50)).await;
}
```

Test output (unedited):

```
r17: persist batches failed: 17/18; completed jobs during run: 4;
     first: Some("Database(Database(SqliteError { code: 5, message: \"database is locked\" }))")
r24: first error: Database(Database(SqliteError { code: 5, message: "database is locked" }))
     4-way concurrent persists: 30/40 batches failed
r25: runs=8 failed=6 articles=80 sample=Some(("failed", Some("database"),
     Some("Database error while persisting discovered articles.")))
r29 (3 consecutive runs):
     CONTENTION tick/100-entry feeds: [("success:", 12)]
     CONTENTION tick/200-entry feeds: [("success:", 12)]
     CONTENTION fetch-all/30-entry feeds: [("failed:database", 8), ("success:", 4)]
     CONTENTION fetch-all/30-entry feeds: [("failed:database", 9), ("success:", 3)]
     CONTENTION fetch-all/30-entry feeds: [("failed:database", 9), ("success:", 3)]
```

The r24 test creates 4 feeds and runs `persist_discovered_entries` concurrently for 30 entries each, 10 rounds, with no other writers. This models four scheduled feeds completing together under default `QUIRE_FETCH_CONCURRENCY=4`, minus the 50 ms stagger.

What happens to the data: the run row is completed as `failed` / `error_category = 'database'`; `etag`/`last_modified` are not advanced; `consecutive_failures` increments, so the next poll is delayed by interval × 2ⁿ (up to 16×). No partial batch is written; everything rolls back.

### D.2 QUA-002: fallback on retryable fetch errors

```rust
// src/articles/extractor.rs:136-152
Err(fetch_err) => {
    if let Some(summary) = fallback_summary
        && let Some(fallback_html) = sanitize_summary_fallback(summary, base_url.as_ref())
    {
        ...
        ExtractionResult::Fallback { sanitized_html: fallback_html, content_hash,
                                     reason: fetch_err.message }   // retryable flag discarded
    } else {
        ExtractionResult::Failed { error_message: fetch_err.message,
                                   retryable: fetch_err.retryable }
    }
}
```

```
r13: Fallback { sanitized_html: "<p>Feed summary text</p>", ..., reason: "Upstream server returned HTTP 503." }
r14: job=completed attempts=1 article=skipped retry_at=None
r28: FAILLOAD elapsed=40.769436041s articles=[("skipped/summary_fallback", 100)] jobs=[("completed", 100)]
Docker (seeded job WITH summary, unresolvable host):
  article_extraction_completed article_id=1 ... status="fallback" reason=Validation error: DNS resolution failed ...
Docker (seeded job WITHOUT summary):
  02:11:49 article_extraction_failed article_id=2 attempt=2
  02:12:49 article_extraction_started article_id=2 attempt=3      (+60 s backoff = 15·2²)
  02:12:56 article_extraction_failed ...  → final state badge "failed"
```

The retry machinery works (as the summary-less job shows); it is bypassed whenever a summary exists.

Retry schedule as implemented, for reference:
- HTTP-level retries inside one job attempt: `fetch_max_attempts` (default 3), backoff `2s·2^(n−1)` with 0–25 % jitter, capped at 30 s; `Retry-After` honored up to the cap. Retryable: timeout, connect, 429, 5xx, DNS. Non-retryable: 4xx, TLS, SSRF, too large, redirect errors.
- Job-level retries: re-queue after `15·2^attempts` seconds (30 s, 60 s, …, capped at 1800 s) while `attempts < max_attempts`.
- Worst case for one unreachable article with default settings: 3 job attempts × 3 HTTP attempts × 45 s timeout, plus backoffs, ≈ 7 minutes of worker time.

### D.3 QUA-003: manual fetch bypasses the concurrency limit

```rust
// src/feeds/scheduler.rs:122-136 — one detached task per enabled feed, no semaphore
for feed in all_feeds.into_iter().filter(|f| f.enabled && !f.fetch_in_progress) {
    if let Ok(ManualTriggerOutcome::Started { .. }) = self.trigger_manual_feed_fetch(feed.id).await {
        started += 1;
    }
}
// src/feeds/scheduler.rs:104 — handle discarded
tokio::spawn(async move { ... coordinator.execute_claimed_feed_fetch(feed, run_id, &rt_cfg).await; ... });
```

```
r23: started=10 max concurrent feed requests=10 (limit 2) inflight_now=0
```

The fixture endpoint `/slowfeed` increments an in-flight counter, sleeps 800 ms, and records the maximum observed concurrency.

### D.4 QUA-004: no in-process recovery

```rust
// src/articles/mod.rs:390-393 — manual retry is refused while 'processing'
if status == "processing" || (status == "queued" && !force_reset) {
    return Ok(false);
}
// src/feeds/scheduler.rs:105-111 — feed already claimed (fetch_in_progress = 1); early return never completes the run
Err(e) => { error!(...); return; }
// src/feeds/scheduler.rs:318 etc. — completion errors discarded
let _ = complete_feed_fetch_run(&self.pool, CompleteFeedRunUpdate { ... }).await;
```

```
r18: manual requeue accepted=false drained=0
```

The UI then shows "Article extraction is already in progress." until the container restarts.

### D.5 QUA-005: XML text filter

```rust
// src/feeds/parser.rs:43-52
let head = String::from_utf8_lossy(&bytes[..prefix_len]).to_ascii_uppercase();
if head.contains("<!ENTITY") || head.contains("SYSTEM \"FILE:") || head.contains("PUBLIC \"") {
    return Err(AppError::Validation("Feed XML rejected: external entity or DTD declarations are not permitted.".to_string()));
}
```

```
r07 input:  <description>The company went PUBLIC "today" after years of delay.</description>
    result: legit feed rejected: Some(Validation("Feed XML rejected: external entity or DTD declarations are not permitted."))
r08 input:  <!DOCTYPE rss PUBLIC "-//Netscape Communications//DTD RSS 0.91//EN" "http://my.netscape.com/publish/formats/rss-0.91.dtd">
    result: RSS 0.91 feed rejected: (same error)
r09 input:  70 KB comment padding, then <!DOCTYPE rss [ billion-laughs entities a..d + <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
    result: entity parse elapsed 1.718375ms -> Ok((Some("&d;"), Some("&xxe;")))   ← no expansion, no file read
```

Because the comparison is case-insensitive, any occurrence of `public "` in the first 64 KB triggers the rejection, including article text, titles, and descriptions.

### D.6 QUA-006: GUID collision on reassignment

Sequence used by r22:

| Step | Action | Resulting rows |
|---|---|---|
| 1 | Feed B persists `/x` with GUID `g` | `articles(X, feed=B, guid=g)`, `afs(X,B,g)` |
| 2 | Feed A persists `/y` with GUID `h` | `articles(Y, feed=A, guid=h)`, `afs(Y,A,h)` |
| 3 | Feed B persists `/y` with GUID `g` (publisher moved GUID `g` to a new URL) | `afs(X,B,NULL)`, `afs(Y,B,g)`; `articles(X).guid` **stays `g`** because X's owner (B) ≠ Y's owner (A) at `articles/mod.rs:216` |
| 4 | Delete feed A | reassignment sets `articles(Y).feed=B, guid=g` → collides with X |

```
r22: delete feed A -> Err(Database(Database(SqliteError { code: 2067,
     message: "UNIQUE constraint failed: articles.feed_id, articles.external_guid" })))
```

### D.7 QUA-007: edition cascade

```sql
-- migrations/0001_initial_schema.sql
FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE CASCADE          -- articles
FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE    -- edition_articles
```

```
r21: edition_articles after feed delete: 0      (was 1)
```

### D.8 QUA-010 / QUA-011: queue ownership

```rust
// src/articles/mod.rs:645-657 (and the other completion branches) — no ownership predicate
"UPDATE article_extraction_jobs SET status = 'completed', ... WHERE id = ?"
// src/articles/mod.rs:504-510 — claim ignores attempts vs max_attempts
"SELECT id, article_id FROM article_extraction_jobs
 WHERE status = 'queued' AND next_eligible_at <= ... ORDER BY next_eligible_at ASC, id ASC LIMIT 1"
```

```
r19: job=failed article=failed            (newer 'completed' result overwritten by stale worker)
r20: claims across 10 crash/restart cycles: 10 (max_attempts=3)
```

### D.9 QUA-012 / QUA-013: SSRF edges

```
r02: not blocked: ["64:ff9b::7f00:1", "64:ff9b::a9fe:a9fe", "2002:7f00:1::", "2002:c0a8:101::", "fec0::1"]
r03: Err(Validation("DNS resolution failed for host '[2606:4700:4700::1111]': failed to lookup address
     information: nodename nor servname provided, or not known"))
r04: fetcher connected to 127.0.0.1 because the injected resolver did not filter
```

```rust
// src/feeds/fetcher.rs:245-249 — no .no_proxy(); env proxies (HTTP_PROXY/HTTPS_PROXY/ALL_PROXY) apply
let mut client_builder = reqwest::Client::builder()
    .user_agent(USER_AGENT)
    .redirect(Policy::none())
    .connect_timeout(options.connect_timeout.min(remaining_timeout))
    .timeout(remaining_timeout);
```

r04 also serves as positive evidence that DNS pinning works. The hostname `rebind.example.com` was connected to the address returned by the resolver, not one obtained from real DNS.

### D.10 QUA-014 / QUA-015 / QUA-016

```
r15: Completed { sanitized_html: "<div>%PDF-1.4\n1 0 obj &lt;&lt; /Type /Catalog &gt;&gt; endobj\nstream\nBT /F1 12 Tf (Hello binary ...", ... }
r10: kept ["old1", "old2"]       (feed order old1, old2, new3, new4; limit 2)
r11: bare-key query -> https://e.example.com/p?123=
     (passes: ?id=123 ≠ ?id=456; "?utm_source=x&id=1#frag" → "?id=1")
```

### D.11 QUA-017: shutdown

```
pre-SIGINT processing=4
graceful shutdown took 10s
probe exited cleanly                 ← printed although the scheduler task was abandoned at the timeout
after SIGINT: jobs=processing:4 queued:16      (recovered correctly on the next start)
```

---

## Appendix E — Crash-Recovery Transcript (SIGKILL)

Setup: fixture serving 20 articles, each delayed 30 s; probe with `QUIRE_EXTRACTION_CONCURRENCY=4`, a fresh database, and one feed.

```
before kill: articles=20 jobs=processing:4
                              queued:16
SIGKILLed probe
after kill:  jobs=processing:4 attempts=4
                  queued:16 attempts=0
             feed_in_progress=0 runs=success
(article delay set to 0, probe restarted)
INFO quire::feeds::scheduler: Recovered interrupted ingestion state on startup recovered_fetches=0 recovered_extractions=4
after restart: jobs=completed:20 articles=extracted:20 attempts=2,1
```

Interpretation:
- Discovery and job creation were committed atomically before the kill (20 articles, 20 jobs).
- The 4 in-flight jobs were reset to `queued` at startup and completed, with their interrupted attempt counted (`attempts=2`).
- No article was lost or duplicated.

## Appendix F — Docker Transcript

```
Container quire-review Started
health=healthy after ~9s
uid=10001(quire) gid=10001(quire) groups=10001(quire)
/data: quire.db, quire.db-shm, quire.db-wal (owner quire:quire)
{"status":"ok","application":"quire","version":"0.2.0","database":{"connected":true,"journal_mode":"wal","foreign_keys_enabled":true}}
create feed: 303 -> /feeds?msg=created
create feed (bad csrf): 403
create feed (no csrf cookie): 403
create feed (missing token field): 422
ssrf feed 169.254: 400
settings fetch_enabled=true (env-locked): "locked by an environment variable and cannot be modified."
settings extraction_concurrency=0: 400 "Max Concurrent Extractions must be between 1 and 32."
settings fetch_interval_minutes = -5 / abc / 99999999999999999999: 400 / 400 / 400
settings save valid (fetch_interval_minutes=120): 303 -> /settings?msg=settings_saved
GET /articles/999999: 404
GET /articles?page=0&page_size=100000: 200
GET /articles?feed_id=abc&status=<script>: 200
POST fetch nonexistent feed: 404
GET /feeds/1/fetch: 405
--- docker compose restart
Review Feed                                   (feed persisted)
name="fetch_interval_minutes" ... value="120" (setting persisted)
--- stop; copy DB out; seed 2 articles + 2 'processing' jobs + 1 'running' fetch; copy back as uid 10001
--- docker compose up -d --force-recreate   (same volume)
INFO Recovered interrupted ingestion state on startup recovered_fetches=1 recovered_extractions=2
INFO article_extraction_started article_id=1 job_id=1 attempt=2
INFO article_extraction_started article_id=2 job_id=2 attempt=2
INFO article_extraction_completed article_id=1 ... status="fallback" reason=... DNS resolution failed for host 'quire-review.invalid'
WARN article_extraction_failed article_id=2 ... DNS resolution failed ...
feed detail: run status "cancelled", "Interrupted by application restart or shutdown."
WARN/INFO (60 s later) article_id=2 attempt=3 → failed  (final)
--- down -v; image removed
```

The healthcheck stayed `healthy` throughout, while every outbound fetch and extraction failed, which confirms that application health is separate from feed health.

## Appendix G — Load-Test Output

```
r27 (debug build, AnyHost fixture, defaults: fetch 4, extraction 4)
LOAD first pass: ticks=25 elapsed=5.260718833s runs=[("success", 100)] articles=1000 dup_urls=0
                 article_status=[("extracted", 1000)] jobs=[("completed", 1000)] db+wal_bytes=6009032
LOAD repeat pass: elapsed=4.021803834s runs=[("success", 200)] articles=1000 re-extracted=0
maximum resident set size: 25,985,024 bytes

r28 (100 articles returning HTTP 500; fetch 1, extraction 8; retry backoff 1 s)
FAILLOAD elapsed=40.769436041s articles=[("skipped/summary_fallback", 100)] jobs=[("completed", 100)]
maximum resident set size: 25,591,808 bytes
```

Notes:
- 100 feeds needed 25 ticks because each tick claims at most `fetch_concurrency` feeds. With the 30 s production poll interval, polling 100 overdue feeds takes ≥ 12.5 minutes even with an empty extraction queue (see QUA-008).
- The repeat pass hit no `304`s because the fixture sends an `ETag` but ignores `If-None-Match`; all 1,000 entries were re-parsed and correctly classified as unchanged.

## Appendix H — Evidence for Controls That Passed

```
r01 (redirect targets, each via 302 from a "public" host):
/r-loop      http://127.0.0.1:PORT/secret        SsrfBlocked  "Redirect URL cannot target private, loopback, or link-local IP addresses."
/r-loop6     http://[::1]:PORT/secret            SsrfBlocked
/r-mapped    http://[::ffff:7f00:1]:PORT/secret  SsrfBlocked
/r-hex       http://0x7f000001:PORT/secret       SsrfBlocked
/r-int       http://2130706433:PORT/secret       SsrfBlocked
/r-short     http://127.1:PORT/secret            SsrfBlocked
/r-localhost http://localhost:PORT/secret        SsrfBlocked  "Redirect URL cannot target localhost or internal hostnames."
/r-lan       http://192.168.1.1/                 SsrfBlocked
/r-meta      http://169.254.169.254/latest/...   SsrfBlocked
internal /secret endpoint hits: 0

r05: 200 MB gzip bomb, limit 1 MiB → ResponseTooLarge in 6.7 ms
r06: 1 byte / 200 ms body, request_timeout 2 s → Timeout after 2.0016 s ("Response streaming timed out after 2s.")

r12 sanitizer output for 20 vectors (complete output):
<p>ok</p>
<a rel="noopener noreferrer">j</a><a rel="noopener noreferrer">j2</a>
<a rel="noopener noreferrer">d</a>
<div>s</div>
<a href="https://ok.example.com" rel="noopener noreferrer">fine</a>

r16: claimed=300 unique=300 errors=0 (16 concurrent claimers, multi-thread runtime)

r26 (real sqlx-migrated Phase 1 DB → Phase 2, then restart):
counts=(2, 4, 1, 1, 2, 4) migrations=[(1, true), (2, true)] fk_violations=0
feed1=("2025-01-01T00:00:00Z", Some("old err")) jobs_for_legacy_pending=0   ← QUA-022
(identical on the second start: no re-application)
```

## Appendix I — Rules Removed from `AGENTS.md` by `66980ec`

| Removed rule (pre-Phase-2 wording, abridged) | Still relied on by current code? |
|---|---|
| §6 Docker: multi-stage build, Debian slim, nonroot `quire` UID 10001, port 8080, state under `/data`, working `HEALTHCHECK` | yes (Dockerfile satisfies it today; the rule protects it) |
| §6 Compose must work locally and as a Portainer Stack from `ghcr.io/spencerthayer/quire` | yes |
| §6 Never require privileged containers, Docker socket, or host networking | yes |
| §9 Implement only the current phase; no stubbed or fake features; document unimplemented features honestly | yes |
| §3 Minimal Dependencies | yes |
| §5 Appropriate Indexes | yes |
| §5 Non-Blocking Operations (never block the Tokio runtime) | relevant to QUA-018 |
| §7 Secret Protection (`QUIRE_AI_API_KEY`, `QUIRE_ELEVENREADER_REFRESH_TOKEN` never in DB, logs, HTML, JSON) | yes (`db/mod.rs:210` allow-list) |
| §7 Parameterized Queries | yes |
| §7 Safe Error Responses (log internally, generic to clients) | yes |
| §7 Do not trust `X-Forwarded-*` unless a trusted proxy is configured | yes |
| §7 SSRF explicit list (`localhost`, `.local`, `.internal`, loopback, link-local, RFC1918) and HTTP timeouts on all outbound requests | partially replaced by a more general rule |
| §8 Inspect `./repos/` and `docs/references.md` before new subsystems; adapt only MIT code with attribution | yes |

## Appendix J — Status Model Reference

Phase 2 keeps Phase 1's `CHECK (extraction_status IN ('pending','extracted','failed','skipped'))` and derives a logical status (`articles/mod.rs:16-29`):

| `articles.extraction_status` | `extracted_content_type` | Job status | Logical status shown in UI |
|---|---|---|---|
| any | any | `processing` | `processing` |
| `pending` | — | `queued` | `pending` |
| `extracted` | `html` | `completed` | `completed` |
| `extracted` | `summary_fallback` | — | `fallback` |
| `skipped` | `summary_fallback` | `completed` | `fallback` |
| `failed` | — | `failed` | `failed` |

The dashboard counters (`db/mod.rs:116-174`) use the raw columns, not the logical status. They agree with the table above except that a `processing` article is counted under `pending` on the dashboard and shown as `processing` in the article list. This is cosmetic and not reported as a finding.

---

## Addendum — Finding identified during remediation-plan revision

### QUA-023 — New and legacy articles are labelled as having HTML content before extraction

- **Severity:** P3
- **Confidence:** Confirmed (by code)
- **Location:** `migrations/0002_feed_ingestion.sql:40` (`extracted_content_type TEXT NOT NULL DEFAULT 'html'`); `src/articles/mod.rs:331-356` (the `INSERT` does not set the column)
- **Expected:** An article with no extracted content has a content type of `none`.
- **Actual:** Every newly discovered article, and every Phase 1 row backfilled by `0002`, gets `extracted_content_type = 'html'` while `extracted_content IS NULL`. The UI masks this: `LOGICAL_STATUS_SQL` checks `extraction_status` first.
- **Impact:** Any consumer that trusts `extracted_content_type` to identify full-text articles (the planned publication engine) would treat pending or failed articles as full text.
- **Fix:** In the `INSERT`, set `'none'`. In a data migration, set `'none'` where the content is `NULL` or empty, and make `'none'` the column default in the next `articles` rebuild. Remediation plan Revision 2, §4.1 / Stage 6.
