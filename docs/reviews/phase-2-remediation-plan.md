# Quire Phase 2 — Remediation Plan

Source: `phase-2-independent-review.md` (commit `66980ec`). Finding IDs refer to that report.
Reviewer tests that reproduce each defect are in `docs/reviews/phase-2-artifacts/review_tests.rs` (rerun instructions: review report, Appendix A). Port the relevant tests into `tests/` before fixing. Each test asserts the correct behavior and currently fails.

Run the standard validation (`cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`) after every group. Do not edit `migrations/0001_*` or `0002_*`; add `0003_*` and later.

---

## Group A — SQLite write concurrency (do first)

### A1. QUA-001 + QUA-019: immediate write transactions

- **Severity:** P1 / P3
- **Files:** `src/articles/mod.rs`, `src/feeds/mod.rs`, `src/db/mod.rs`
- **Approach:**
  1. Add a helper in `src/db/mod.rs`: `pub async fn begin_write(pool: &SqlitePool) -> AppResult<Transaction<'static, Sqlite>>` returning `pool.begin_with("BEGIN IMMEDIATE")` (sqlx 0.8.6 `Pool::begin_with`).
  2. Use it in `persist_discovered_entries`, `enqueue_manual_article_extraction`, `recover_interrupted_extractions`, `try_claim_feed_fetch`, `complete_feed_fetch_run`, `recover_interrupted_fetches`, `delete_feed`, `create_feed`, `update_feed`, `set_feed_enabled`, and `upsert_settings`.
  3. Replace the raw `BEGIN IMMEDIATE`/`COMMIT`/`ROLLBACK` in `claim_next_extraction_job` and `complete_extraction_job` with the same helper (rollback on drop).
  4. Keep the 50 ms stagger only as load smoothing, or remove it; it must not be relied on.
- **Tests:** r17, r24, r25, and r29 (fetch-all branch) must report 0 failed batches. Add a regression test: 8 concurrent 200-entry persists plus 4 claim/complete loops.
- **Depends on:** none
- **Complexity:** Small

---

## Group B — Worker ownership and scheduling

### B1. QUA-003 + QUA-008: single bounded execution model

- **Severity:** P2
- **Files:** `src/feeds/scheduler.rs`, `src/web/mod.rs`
- **Approach:**
  1. Give `IngestionCoordinator` an `Arc<Semaphore>` for feed fetches and another for extractions, sized from `RuntimeIngestionConfig`. On config change, rebuild them or use `add_permits`/`forget_permits`.
  2. Make manual triggers mark the feed due (`next_scheduled_fetch_at = now`) and call `notify_workers()`. Alternatively, acquire the same feed semaphore inside a task tracked by a coordinator-owned `tokio_util::task::TaskTracker` or `JoinSet`. Never discard task handles.
  3. Split `spawn_background_loop` into two loops: a feed scheduler and an extraction worker pool. Replace the extraction lockstep batches with a refill loop: while a permit is available and a job can be claimed, spawn; on each completion, claim again.
  4. Log `JoinError` (panics) with the job or feed ID.
- **Tests:** r23 (max in-flight ≤ limit). New: with slow article fixtures (2 s each, 20 jobs, concurrency 2), a due feed's run must start within one poll interval.
- **Depends on:** A1
- **Complexity:** Medium

### B2. QUA-004 + QUA-010 + QUA-011: leases and claim tokens

- **Severity:** P2 / P3 / P3
- **Files:** new `migrations/0003_extraction_leases.sql`; `src/articles/mod.rs`; `src/feeds/mod.rs`; `src/feeds/scheduler.rs`
- **Approach:**
  1. Migration: add `claim_token TEXT` and `lease_expires_at TEXT` to `article_extraction_jobs`, and `fetch_lease_expires_at TEXT` to `feeds`.
  2. Claim sets a random `claim_token` and `lease_expires_at = now + (article_timeout × max_attempts + backoff margin)`. Add `AND attempts < max_attempts` to the eligible-job query.
  3. `complete_extraction_job`: `WHERE id = ? AND status = 'processing' AND claim_token = ?`. If 0 rows are affected, log and discard the stale result.
  4. Lease recovery on every extraction loop iteration and every scheduler tick, not only at startup: `processing` jobs with an expired lease → `queued` if `attempts < max_attempts`, else `failed`. The same applies to feeds with `fetch_in_progress = 1` and an expired lease (mark the run `cancelled`).
  5. In `scheduler.rs`, replace `let _ = complete_feed_fetch_run(...)` with a bounded retry (for example 3 tries, 1/2/4 s), then an error log. In `trigger_manual_feed_fetch`, if config loading fails, complete the run as `failed` instead of returning early.
  6. Startup recovery: also mark `processing` jobs with `attempts >= max_attempts` as `failed`.
  7. Document single-instance operation (README and `docs/architecture.md`). Optionally take an exclusive lock file on `/data/quire.lock` at startup.
- **Tests:** r18 (stuck job recovers after its lease without restart), r19 (stale completion rejected), r20 (≤ max_attempts claims across crash cycles). New: inject a panic in the extraction path and confirm the job is retried after its lease.
- **Depends on:** A1, B1
- **Complexity:** Medium

### B3. QUA-017: cancellable shutdown

- **Severity:** P3
- **Files:** `src/main.rs`, `src/feeds/scheduler.rs`, `compose.yaml`
- **Approach:**
  - Hold a `CancellationToken` in the coordinator.
  - Workers check it before claiming and `select!` on it during fetches.
  - Cancelled jobs are reset to `queued` without consuming an attempt; cancelled feed runs are marked `cancelled`.
  - Log "shutdown timed out" if the 10 s timeout fires.
  - Add `stop_grace_period: 20s` to compose and to the README Portainer snippet.
- **Tests:** Probe-style test: SIGINT with 4 in-flight jobs exits in under 2 s, and the jobs end up `queued`.
- **Depends on:** B1, B2
- **Complexity:** Small

---

## Group C — Extraction semantics

### C1. QUA-002: retry before fallback

- **Severity:** P2
- **Files:** `src/articles/extractor.rs`, `src/articles/mod.rs`, `src/feeds/scheduler.rs`, `docs/architecture.md`
- **Approach:**
  - Pass `attempts` and `max_attempts` into `fetch_and_extract_article`, or decide in `complete_extraction_job`.
  - Retryable fetch error and `attempts < max_attempts` → `Failed { retryable: true }`, which re-queues with backoff, stores the summary-fallback HTML as interim content, and sets `extracted_content_type = 'summary_fallback'` while keeping the job `queued`.
  - Final attempt, or a non-retryable error → `Fallback` (terminal), as today.
  - Readability failure on a fetched 200 page stays terminal fallback.
  - Document the policy.
- **Tests:** r13, r14. New: fixture returns 503, 503, then 200 → article ends `extracted` with 3 attempts.
- **Depends on:** B2 (attempt bookkeeping)
- **Complexity:** Small

### C2. QUA-014: content-type gate

- **Severity:** P3
- **Files:** `src/articles/extractor.rs`
- **Approach:** Run readability only for `text/html`, `application/xhtml+xml`, or a missing content type whose body starts with `<`. Otherwise use the summary fallback, or `Failed { retryable: false, "unsupported content type" }`.
- **Tests:** r15
- **Depends on:** none
- **Complexity:** Small

### C3. QUA-018: move CPU work off the async runtime

- **Severity:** P3
- **Files:** `src/articles/extractor.rs`, `src/feeds/scheduler.rs`, `src/web/mod.rs`
- **Approach:** Wrap `parse_feed_bytes`, `extract_readable_html_from_str`, and the summary sanitization in `tokio::task::spawn_blocking`. Handle `JoinError` as a non-retryable failure.
- **Tests:** Existing tests still pass. Optional: a 10 MB article extraction doesn't delay `/health` by more than 100 ms.
- **Depends on:** none
- **Complexity:** Small

---

## Group D — Parsing and identity

### D1. QUA-005: remove the XML text filter

- **Severity:** P2
- **Files:** `src/feeds/parser.rs`, `docs/architecture.md`, `docs/roadmap.md`
- **Approach:** Delete `reject_dangerous_xml_declarations` and its call. Keep a test proving quick-xml does not expand entities (r09) so a parser upgrade can't regress silently. Update the docs to say entities are not expanded by the parser.
- **Tests:** r07, r08, r09
- **Depends on:** none
- **Complexity:** Small

### D2. QUA-015: keep the newest entries

- **Severity:** P3
- **Files:** `src/feeds/parser.rs`
- **Approach:** Stable-sort entries by `updated.or(published)` descending (undated entries keep document order, placed after dated ones) before `take(max_entries)`.
- **Tests:** r10
- **Depends on:** none
- **Complexity:** Small

### D3. QUA-016: query rewriting only when needed

- **Severity:** P3
- **Files:** `src/articles/normalizer.rs`
- **Approach:** Split the raw query on `&`. Drop a segment only if its decoded key matches the tracking list. Rejoin the remaining raw segments unchanged. If nothing was removed, leave the query untouched.
- **Tests:** r11, plus the existing canonicalization test
- **Depends on:** none
- **Complexity:** Small

### D4. QUA-013: IPv6 literal hosts

- **Severity:** P3
- **Files:** `src/feeds/fetcher.rs`, `src/feeds/validation.rs`
- **Approach:** Use `Url::host()`. For `Host::Ipv4`/`Host::Ipv6`, skip DNS, check `is_disallowed_ip`, and connect directly. For `Host::Domain`, resolve and pin.
- **Tests:** r03 (change it to assert the fetch path accepts a public v6 literal using a fixture resolver)
- **Depends on:** E1 (touches the same code)
- **Complexity:** Small

---

## Group E — SSRF hardening

### E1. QUA-012

- **Severity:** P3
- **Files:** `src/feeds/validation.rs`, `src/feeds/fetcher.rs`
- **Approach:**
  1. `is_disallowed_ipv6`: block `64:ff9b::/96` and `64:ff9b:1::/48` (and check the embedded IPv4), `2002::/16` (check the embedded IPv4 in segments 1–2), `fec0::/10`, `100::/64`, and `2001::/32` (Teredo).
  2. In `execute_single_fetch`, after `resolver.resolve`, reject if any address satisfies `is_disallowed_ip`, regardless of resolver implementation.
  3. Add `.no_proxy()` to the client builder.
  4. Wrap `resolver.resolve` in `tokio::time::timeout(remaining_timeout, ...)`.
- **Tests:** r02, r04 (expect an `SsrfBlocked` error), and a DNS-timeout test with a resolver that never returns.
- **Depends on:** none
- **Complexity:** Small

---

## Group F — Data integrity on feed deletion

### F1. QUA-006: GUID collision during reassignment

- **Severity:** P2
- **Files:** `src/feeds/mod.rs`
- **Approach:** Before the reassignment `UPDATE`, compute the target feed per article. Set `external_guid = NULL` when `(target_feed_id, guid)` already exists in `articles` for another row. A two-statement approach inside the immediate transaction is fine.
- **Tests:** r22
- **Depends on:** A1
- **Complexity:** Small

### F2. QUA-007: preserve edition membership

- **Severity:** P2
- **Files:** new migration; `src/feeds/mod.rs`; `src/articles/mod.rs` (queries that `INNER JOIN feeds` must tolerate orphaned articles); templates
- **Approach (pick one; recommended first):**
  - (a) Block deleting a feed whose sole-owned articles appear in any `edition_articles`, with a clear validation error. No schema change. Small.
  - (b) Table-rebuild migration making `articles.feed_id` nullable with `ON DELETE SET NULL`; switch the listing queries to `LEFT JOIN`. Medium.
- **Tests:** r21 (adjust the assertion to the chosen policy)
- **Depends on:** A1, F1
- **Complexity:** Small (a) / Medium (b)

### F3. QUA-022: backfill jobs for legacy pending articles

- **Severity:** P3
- **Files:** new `migrations/0003_*` (or fold into B2's migration)
- **Approach:** `INSERT OR IGNORE INTO article_extraction_jobs (article_id) SELECT id FROM articles WHERE extraction_status = 'pending';`
- **Tests:** r26, extended to assert one job for the legacy pending article
- **Depends on:** B2 if folded together
- **Complexity:** Small

---

## Group G — Governance, tests, and documentation

### G1. QUA-009: restore `AGENTS.md`

- **Severity:** P2
- **Files:** `AGENTS.md`
- **Approach:** Restore every section and bullet removed in `66980ec` (Docker/Portainer requirements, Scope Management, Minimal Dependencies, Appropriate Indexes, Non-Blocking Operations, Secret Protection, Parameterized Queries, Safe Error Responses, X-Forwarded-* rule, the SSRF host list, and the `./repos` inspection/attribution rules). Keep the added queue section. Show the diff to the project owner before committing.
- **Tests:** `git diff f0ec769 -- AGENTS.md` shows only additions.
- **Depends on:** none
- **Complexity:** Small

### G2. QUA-020: test-suite improvements

- **Severity:** P3
- **Files:** `tests/`
- **Approach:**
  - Port r01–r29 (drop the measurement-only r27–r29, or mark them `#[ignore]`).
  - Make the claim test actually concurrent (r16).
  - Add a crash test that spawns the binary, or an example, as a child process and kills it with SIGKILL.
  - Make the migration test use `sqlx::migrate::Migrator` for 0001 (r26).
  - Split the 10 large tests into focused ones.
- **Depends on:** the relevant fixes
- **Complexity:** Medium

### G3. QUA-021: documentation corrections

- **Severity:** P3
- **Files:** `README.md`, `docs/architecture.md`, `docs/roadmap.md`, `src/config.rs`
- **Approach:**
  - Correct the concurrency description after B1.
  - Document the fallback and retry policy after C1, the single-instance requirement after B2, the XML handling after D1, and `synchronous = NORMAL` durability.
  - Make `QUIRE_FETCH_MIN_INTERVAL_MINUTES` validation range `5..=10080`, or make the clamp and DB CHECK honor it.
  - Recompute `next_scheduled_fetch_at` when a feed's interval changes.
- **Depends on:** B1, B2, C1, D1
- **Complexity:** Small

---

## Suggested order

A1 → G1 → D1 → B1 → B2 → C1 → F1 → F2 → B3 → E1 → D4 → remaining P3s → G2 → G3. Then request a re-review.

Also install `cargo-audit` (`cargo install cargo-audit --locked`), run it, and record the result. It was not run in this review.
