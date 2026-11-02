# Quire Phase 2 — Remediation Plan (Revision 2)

- **Source findings:** `docs/reviews/phase-2-independent-review.md` (QUA-001 … QUA-022, commit `66980ec`).
- **Revision 2:** incorporates the technical review of Revision 1 (TRP-001 … TRP-016). It also adds one defect found while revising (QUA-023).
- **Status:** Ready for implementation, one stage at a time. Stage 2 (QUA-001) may ship alone.

---

## 0. How to use this plan

1. **Work in stage order (§6).** Do not start a stage until the previous stage's **gate** passes. The original Phase 2 passed fmt, clippy, and tests while carrying a P1 concurrency defect, so passing the standard checks is necessary but not sufficient.
2. **Treat §2 to §5 as binding design decisions.** Where the plan says "must", deviate only after updating this document and getting owner approval.
3. **Adapt reviewer tests per §7.** Do not copy them blindly. Keep each original reproduction case, then adapt the assertion to the decided behavior.
4. **Respect migration rules.** Never edit `0001_*` or `0002_*`. New migrations start at `0003_*`. Run every new migration against a copy of a real, populated database before merging (procedure in §5.3).
5. **Run these checks after every stage:**
   ```bash
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test
   ```
   Then run that stage's gate tests (§6), which include concurrency and crash tests that standard CI does not exercise.

### Facts verified for this revision

| Fact | Evidence |
|---|---|
| `Pool::begin_with(statement)` exists in the pinned sqlx (0.8.6). It goes through the managed `Transaction`, so it rolls back on drop or cancellation. | `sqlx-core-0.8.6/src/pool/mod.rs:390-399` |
| sqlx migrations support a `-- no-transaction` first line (needed for SQLite table rebuilds). | `sqlx-core-0.8.6/src/migrate/source.rs:127` |
| `reqwest::Response::remote_addr()` is available (0.13). | `reqwest-0.13/src/async_impl/response.rs:116` |
| `std::fs::File::try_lock` is in stable Rust (≥ 1.89; toolchain here is 1.97.1). No crate is needed for an instance lock. | rustc 1.97.1 |
| **New defect QUA-023:** `0002` adds `extracted_content_type TEXT NOT NULL DEFAULT 'html'`, and `INSERT INTO articles` does not set it. Every newly discovered article, and every Phase 1 row, claims HTML content while `extracted_content` is `NULL`. | `migrations/0002_feed_ingestion.sql:40`; `src/articles/mod.rs:331-356` |

---

## 1. Disposition of the Revision 1 review

| ID | Topic | Disposition | Where addressed |
|---|---|---|---|
| TRP-001 | Semaphore resizing exceeds limits | **Accepted.** Semaphores removed; one dispatcher per worker class owns an `active` count and reads the limit before every claim. | §2.3, Stage 3 |
| TRP-002 | Lease expiry duplicates live work | **Accepted in substance, different mechanism.** No time-based lease expiry at all. Single-instance is *enforced* (lock file), and in-process work is reclaimed only after its future is known to be finished or dropped, so a live worker can never be reclaimed. Per-job heartbeats would only matter for multiple instances, which are prohibited. | §2.1, §2.4, Stage 4 |
| TRP-003 | Feed claims lack ownership | **Accepted.** `feeds.current_run_id` plus run-ID ownership predicates on every completion write. Article persistence and run completion become one ownership-checked transaction. | §2.4, Stage 4 |
| TRP-004 | `BEGIN IMMEDIATE` everywhere is too broad | **Accepted.** Transactions classified: immediate only for read-then-write; all raw `BEGIN` removed. | §3.1, Stage 2 |
| TRP-005 | No bounded busy policy | **Accepted.** Retryable codes, whole-transaction retry, attempts and backoff, a duration budget, and failure semantics are all defined. | §3.2, Stage 2 |
| TRP-006 | Worker model ambiguous | **Accepted.** One model chosen: SQLite-backed work state plus coordinator-owned dispatchers. Manual actions only mark work eligible. Manual requests survive restarts; repeats are coalesced. | §2.2, §2.3, Stage 3 |
| TRP-007 | Cancellation vs attempt accounting | **Accepted.** Semantics defined. A graceful-shutdown release reverts the claim's increment in the same ownership-checked statement. No extra counter (nothing needs it). | §2.5, Stage 9 |
| TRP-008 | Backlog visibility | **Accepted.** Dashboard backlog metrics; discovery is never throttled by the extraction backlog. | §2.6, Stage 5 |
| TRP-009 | Interim fallback creates contradictory states | **Accepted.** Content state and execution state are separated and derived from exactly two columns. The legacy column is written by one helper. Also fixes QUA-023. | §4.1, Stage 6 |
| TRP-010 | Two retry layers multiply requests | **Accepted.** One retry budget with distinct layer responsibilities; ≤ 6 requests per article; `Retry-After` honored at the job layer. | §4.2, Stage 6 |
| TRP-011 | Blocking deletion leaves ownership unresolved | **Accepted.** Articles become independent of a required feed FK; edition members survive feed deletion. | §5.1, Stage 7 |
| TRP-012 | GUID identity duplicated | **Accepted.** `article_feed_sources` is authoritative; `articles.external_guid` becomes inert legacy data and its unique index is dropped. | §5.2, Stage 7 |
| TRP-013 | SSRF should use a policy, not a list | **Accepted.** A public-unicast allow-list policy, applied at four points plus a post-connect peer check. | §5.4, Stage 8 |
| TRP-014 | XML invariant must stay tested | **Accepted.** The test runs the production fetch-and-parse path with external DTD, XXE, and billion-laughs payloads. | Stage 8 |
| TRP-015 | No gates between groups | **Accepted.** Every stage has a gate. | §6 |
| TRP-016 | Tests must be adapted, not copied | **Accepted.** Per-test invariant and adaptation table. | §7 |

The reviewer noted that `begin_with` had not been independently verified. It is now verified from source (table above), and Stage 2's gate requires it to compile and pass the contention tests.

---

## 2. Target execution architecture (binding)

### 2.1 Deployment invariant: exactly one Quire process per data directory

- At startup, before migrations, open `/data/quire.lock` (`{QUIRE_DATA_DIR}/quire.lock`) and call `File::try_lock()`. If the lock is held, exit non-zero with: `Another Quire instance is using <dir>`. Hold the `File` for the process lifetime; the OS releases it on exit, including on SIGKILL.
- This is valid for local Docker volumes and bind mounts, including two containers on one host sharing a volume. It is **not** reliable on NFS/SMB; document that `/data` must be local storage.
- Everything below relies on this invariant. Multi-instance operation is explicitly unsupported (README + `docs/architecture.md`).

### 2.2 Work state lives in SQLite; manual actions only mark work eligible

| Action | Effect (single `UPDATE`, then `notify_workers()`) |
|---|---|
| **Fetch now** (one feed) | `UPDATE feeds SET next_scheduled_fetch_at = now, manual_requested_at = now WHERE id = ? AND enabled = 1 AND fetch_in_progress = 0` |
| **Fetch all enabled** | the same `UPDATE` without the `id` predicate |
| **Retry extraction** | unchanged semantics: reset the job to `queued` (ownership rules in §2.4) |

- **Durable:** a request made just before a restart is still due after the restart.
- **Coalesced:** repeated clicks are idempotent. A request against a feed that is already running is a no-op (0 rows); the UI says "already running". A request is not queued behind a running fetch.
- **Trigger type:** at claim, `trigger_type = 'manual'` if `manual_requested_at IS NOT NULL`, otherwise `'scheduled'`. The claim clears `manual_requested_at`.
- **Fetch-all ordering:** feeds are claimed by `next_scheduled_fetch_at`, then `id`, under the same concurrency limit as scheduled work.
- **Disabled global fetching:** manual requests still run when `fetch_enabled = false`, because the user asked explicitly. Implement this by having the feed dispatcher claim only feeds with `manual_requested_at IS NOT NULL` while `fetch_enabled = false`.

Migration `0003_work_ownership.sql` adds `feeds.manual_requested_at TEXT` (with other columns; see §2.4).

### 2.3 Dispatchers: one per worker class, no semaphores

The coordinator owns exactly two long-lived tasks: a **feed dispatcher** and an **extraction dispatcher**. Both are spawned at startup, their handles are kept, and both are joined on shutdown. Nothing else in the codebase calls `tokio::spawn` for ingestion work.

Each dispatcher runs this loop (pseudocode):

```text
active = JoinSet::new()
loop:
    if shutdown requested: break                      # see §2.5
    limit = runtime_config().<class>_concurrency      # re-read every iteration
    while active.len() < limit:
        match claim_one().await:                      # one short IMMEDIATE tx
            Some(item) => active.spawn(run_with_deadline(item))
            None       => break
    select! {
        Some(res) = active.join_next() => handle_finished(res)   # §2.4 release rules
        _ = notify.notified()          => {}
        _ = sleep(poll_interval)       => {}                     # 5 s
        _ = shutdown.cancelled()       => {}
    }
```

- **A decrease in concurrency** (8 → 2 while 8 are active) starts no new work until `active.len() < 2`. Running work finishes normally. **An increase** takes effect on the next iteration.
- There is one authority (`active.len()` against the freshly read limit) and no replaceable permit objects.
- The feed and extraction dispatchers are independent: a large extraction backlog never delays feed claims (fixes QUA-008).
- The poll interval drops from 30 s to 5 s. That is cheap, because the eligibility queries are indexed. `notify` makes manual actions immediate.
- **Panics:** `join_next()` returns `Err(JoinError)`. `handle_finished` logs `error!` with the item ID and runs the release rule from §2.4.

### 2.4 Ownership, completion, and in-process recovery

**Extraction jobs.** Migration `0003` adds `article_extraction_jobs.claim_token TEXT`.

- **Claim**, inside `BEGIN IMMEDIATE`:
  ```sql
  UPDATE article_extraction_jobs
  SET status = 'processing',
      attempts = attempts + 1,
      claim_token = :tok,
      claimed_at = now
  WHERE id = (
      SELECT id FROM article_extraction_jobs
      WHERE status = 'queued' AND next_eligible_at <= now AND attempts < max_attempts
      ORDER BY next_eligible_at, id LIMIT 1
  )
  RETURNING ...
  ```
  `:tok` is 128 random bits, hex-encoded.
- **Every completion or release** includes `WHERE id = :id AND claim_token = :tok AND status = 'processing'`. If 0 rows are affected, the worker is stale: roll back the whole transaction, log `warn!`, and discard the result.
- **Manual retry while processing** stays refused. The UI says "in progress".

**Feed fetches.** Migration `0003` adds `feeds.current_run_id INTEGER`.

- **Claim** (`BEGIN IMMEDIATE`):
  ```sql
  UPDATE feeds SET fetch_in_progress = 1, current_run_id = :run ...
  WHERE id = ? AND fetch_in_progress = 0
  ```
  It is issued together with the `INSERT` of the `feed_fetch_runs` row that produced `:run`.
- **Completion is one transaction** (`BEGIN IMMEDIATE`), replacing the separate `persist_discovered_entries` and `complete_feed_fetch_run` calls:
  1. `UPDATE feeds SET fetch_in_progress = 0, current_run_id = NULL, <validators, counters, schedule> WHERE id = :feed AND current_run_id = :run AND fetch_in_progress = 1`. If 0 rows are affected, roll back and discard; the worker is stale.
  2. Upsert all entries and enqueue jobs.
  3. `UPDATE feed_fetch_runs SET ... WHERE id = :run AND status = 'running'`.
  4. Prune history.
  5. `COMMIT`.

  Consequences:
  - Articles from a stale fetch are never persisted.
  - HTTP validators advance only when entries were persisted (TRP-005).
  - The read-before-write in the current `complete_feed_fetch_run` (`feeds/mod.rs:480`) moves inside the transaction.
- **Failure paths** (fetch error or parse error) use the same transaction shape without step 2.

**Bounded execution (no lease expiry).**

- Each item runs inside `tokio::time::timeout(deadline, work)`:
  - Articles: `deadline = article_timeout × http_attempts + 30 s`.
  - Feeds: `deadline = feed_timeout × fetch_max_attempts + retry_max × (fetch_max_attempts − 1) + 30 s`.
- On timeout the future is **dropped** (all its I/O is cancelled), and only then is the item released. Blocking-pool work started by the item (Stage 8's `spawn_blocking`) may run to completion, but it performs no database writes. All writes happen in the async part, which has already been dropped.
- No other component can reclaim an item whose future may still be running. This removes TRP-002's slow-but-healthy duplicate-execution case without heartbeats.

**Release rules** (`handle_finished`):

| Outcome | Extraction job | Feed |
|---|---|---|
| Normal completion | written by the completion transaction | written by the completion transaction |
| Completion transaction failed after the §3.2 retries | conditional release: `status = 'queued'`, `next_eligible_at = now + 60 s`, `claim_token = NULL` (attempt counted) | conditional release: `fetch_in_progress = 0`, `current_run_id = NULL`, run → `failed`/`database`, validators untouched |
| Panic (`JoinError`) | same as the row above | same as the row above |
| Deadline exceeded | conditional release, attempt counted, `last_error = 'deadline exceeded'` | conditional release, run → `failed`/`timeout` |
| Release itself fails (DB unavailable) | item stays `processing`; `error!`; counted on the dashboard as "stuck"; fixed by startup recovery | same as the cell to the left |

**Startup recovery** is safe because the instance lock guarantees no live owner exists:
- `processing` → `queued`, or → `failed` if `attempts >= max_attempts` (fixes QUA-011).
- Feeds with `fetch_in_progress = 1` → released; runs → `cancelled`.

**Delivery semantics (documented in `docs/architecture.md`):**
- Database state transitions are exactly-once per claim, because every transition is an ownership-conditional `UPDATE`.
- External HTTP requests are **at-least-once**. A crash or deadline after the request but before commit repeats the request on the next attempt. That is acceptable for idempotent GETs.

### 2.5 Attempt and cancellation semantics

- **What an attempt is:** `attempts` counts executions that started (it is incremented at claim).
- **Unexpected termination** (crash, SIGKILL, panic, deadline) counts as an attempt. This bounds poison jobs.
- **Graceful shutdown** does not count:
  1. On shutdown, dispatchers stop claiming and cancel in-flight futures through a shared `CancellationToken` passed into the HTTP layer (`select!` on it).
  2. Each cancelled job is released with a single statement:
     ```sql
     UPDATE article_extraction_jobs
     SET status = 'queued', attempts = attempts - 1, claim_token = NULL
     WHERE id = :id AND claim_token = :tok AND status = 'processing'
     ```
  3. Cancelled feeds are released with their run marked `cancelled`.

  The decrement is safe because it is conditional on the same token that performed the increment, and it happens in one statement. `last_extraction_attempt_at` keeps the execution history visible.
- **No separate cancellation counter.** Nothing in the UI or operations needs one (YAGNI). The TRP-007 reviewer suggested one only as an option.
- **Shutdown budget:**
  - Dispatchers get 5 s after cancellation to release items, then `main` exits.
  - Compose sets `stop_grace_period: 20s`.
  - Log `shutdown complete` only when both dispatchers joined; otherwise log `shutdown timed out; N items left for startup recovery`.

### 2.6 Backlog visibility

Add one dashboard query (indexed by `idx_extraction_jobs_eligible`) and show:

- queued
- processing
- failed (terminal)
- oldest eligible queued job age: `now − MIN(next_eligible_at) WHERE status = 'queued' AND next_eligible_at <= now`
- completed in the last 24 h (throughput)
- stuck: processing items whose `claimed_at` is older than their deadline. This should always be 0 when §2.4 works.

Also show feeds due but not yet claimed, and feeds currently running.

Feed discovery is never throttled by the extraction backlog. The backlog is unbounded on disk by design and visible in the UI.

---

## 3. SQLite transaction policy (binding)

### 3.1 Classification

| Category | Rule | Functions |
|---|---|---|
| **Read-then-write** (outcome depends on a prior read in the same transaction) | `pool.begin_with("BEGIN IMMEDIATE")` | feed completion transaction (§2.4), extraction claim, extraction completion and release, `enqueue_manual_article_extraction`, `delete_feed` (new form, §5.1), feed claim (it pairs `UPDATE` + `INSERT ... RETURNING`; immediate keeps it simple) |
| **Write-first** (first statement writes; nothing earlier is read) | `pool.begin()` is acceptable | `create_feed`, `update_feed`, `set_feed_enabled`, `upsert_settings`, startup recovery, manual "mark due" updates |
| **Raw `BEGIN` / `COMMIT` / `ROLLBACK` strings** | **forbidden**; remove all (QUA-019) | currently `articles/mod.rs:500-501, 587-588` |

Add a helper in `src/db/mod.rs`:

```rust
pub async fn begin_immediate(pool: &SqlitePool) -> AppResult<Transaction<'static, Sqlite>> {
    Ok(pool.begin_with("BEGIN IMMEDIATE").await?)
}
```

### 3.2 Busy and failure policy

| Parameter | Value |
|---|---|
| `busy_timeout` | 10 s (unchanged) |
| Retryable errors | primary code `SQLITE_BUSY` (5) including extended `BUSY_SNAPSHOT` (517) and `BUSY_RECOVERY` (261); `SQLITE_LOCKED` (6). Check with `DatabaseError::code()`. |
| Non-retryable | constraint violations, I/O errors, everything else |
| Retry unit | the **whole transaction closure**, re-run from `begin` (no partial continuation) |
| Attempts / backoff | 3 attempts total; sleep 250 ms, then 1 s (±20 % jitter) |
| Duration budget | target < 500 ms per write transaction; `warn!` with `tx_ms` above 2 s. Every immediate transaction logs `tx_ms` at `debug`. |
| Large batches | measure a 5,000-entry feed completion transaction (the max `max_entries_override`). If it exceeds 2 s on the CI runner, cap per-transaction entries at 500 and process a feed in chunks. Each chunk is ownership-checked (an ownership check without clearing `fetch_in_progress`); the final chunk completes the run. Validators advance only in the final chunk. |
| Final failure | the transaction rolled back as a unit. The feed run is released per §2.4 (`failed`/`database`, validators not advanced, `consecutive_failures + 1`); the extraction job is released per §2.4. |

Implement this as `db::with_write_retry(pool, |tx| async { ... })`. Use it for every read-then-write function.

---

## 4. Extraction semantics (binding)

### 4.1 Content state vs execution state

There are exactly two sources of truth:

- **Content state:** `articles.extracted_content_type ∈ {'none', 'summary_fallback', 'html'}`, together with `extracted_content`.
- **Execution state:** `article_extraction_jobs.status ∈ {'queued', 'processing', 'completed', 'failed', 'cancelled'}`. `cancelled` is unused; leave it out.

Displayed state is derived as follows (replacing `LOGICAL_STATUS_SQL`):

| Content | Job | Display | Meaning |
|---|---|---|---|
| `none` | `queued` / `processing` | **Pending** / **Extracting** | no content yet |
| `summary_fallback` | `queued` / `processing` | **Summary (full text pending)** | summary available; full-text retry scheduled or running |
| `html` | `queued` / `processing` | **Full text (refreshing)** | a manual refresh of an extracted article |
| `html` | `completed` | **Full text** | extraction succeeded |
| `summary_fallback` | `completed` / `failed` | **Summary only** | full text exhausted or permanently failed; summary retained |
| `none` | `failed` | **Failed** | no usable content |

Rules:

- `articles.extraction_status` (the Phase 1 column with its `CHECK`) becomes **legacy, write-only, derived**. One function, `sync_legacy_status(tx, article_id)`, is called at the end of every transaction that changes either column. The mapping is:
  - job `queued` or `processing` → `pending`
  - `html` + `completed` → `extracted`
  - `summary_fallback` + (`completed` or `failed`) → `skipped`
  - `none` + `failed` → `failed`
- No code may read `extraction_status` for decisions. Update the dashboard counters to use the derived state.
- **QUA-023 fix:** `INSERT INTO articles` must set `extracted_content_type = 'none'`. Migration `0003` runs `UPDATE articles SET extracted_content_type = 'none' WHERE extracted_content IS NULL OR extracted_content = ''`. The column default becomes `'none'` in Stage 7's rebuild.
- **The publication engine (Phases 3+) must select full text with `extracted_content_type = 'html'`** and must never infer it from `extraction_status`. Record this in `docs/architecture.md`.

### 4.2 One retry budget

| Layer | Handles | Policy |
|---|---|---|
| HTTP layer, **articles** | connection refused or reset before any response | at most 2 tries, fixed 1 s delay. Timeouts, 5xx, 429, and DNS errors are **not** retried here. |
| HTTP layer, **feeds** | as today (timeouts, connect, 5xx, 429) | `QUIRE_FETCH_MAX_ATTEMPTS`, existing backoff + jitter, `Retry-After` capped at `QUIRE_FETCH_RETRY_MAX_SECONDS` |
| Job layer, **articles** | timeout, 5xx, 429, DNS, connect (after the HTTP layer gives up) | `max_attempts = QUIRE_FETCH_MAX_ATTEMPTS` (default 3); `next_eligible_at = now + max(15 s · 2^attempts, Retry-After)`, capped at 6 h |
| Schedule layer, **feeds** | any failed run | existing `consecutive_failures` backoff (×2ⁿ, up to 16×, max 7 days); `next_scheduled_fetch_at ≥ now + Retry-After` when present, capped at 24 h |
| Permanent (no retry anywhere) | 4xx except 408/429, TLS errors, SSRF blocks, response too large, unsupported content type (QUA-014), readability failure on a 200 response | terminal immediately |

Bounds: at most 2 × 3 = **6 requests per article**. Worst-case worker time per article is roughly 3 × article timeout plus 3 s.

On a retryable job failure, if a summary exists and the content is `none`, store the sanitized summary now (content → `summary_fallback`) and keep the job `queued`. On terminal failure, keep whatever content exists. This fixes QUA-002 without contradictory state: the content column says what is available, and the job column says whether work remains.

`FetchError` gains `retry_after: Option<Duration>` so the job layer can honor it.

The UI article detail shows the display state, attempts used / max, next retry time, last error, and Retry-After when applicable.

---

## 5. Data model, deletion, and security (binding)

### 5.1 Articles are independent of feed subscriptions

Migration `0004_article_independence.sql` is a full table rebuild:

- `articles.feed_id` becomes **nullable**, `REFERENCES feeds(id) ON DELETE SET NULL`. Its meaning becomes "primary display source", and it is informational only.
- The `extracted_content_type` default becomes `'none'`.
- The partial unique index `idx_articles_feed_external_guid` is **dropped**. `external_guid` is kept as an inert legacy column (§5.2).
- All other columns, `CHECK`s, and indexes are kept exactly as they are.

**Deletion policy** (`delete_feed`, one `BEGIN IMMEDIATE` transaction):

1. `DELETE FROM feeds WHERE id = ?`. This cascades to `article_feed_sources`, `feed_fetch_runs`, and (via SET NULL) to `articles.feed_id`.
2. Repoint each affected article to its earliest remaining source, if one exists:
   ```sql
   UPDATE articles
   SET feed_id = (SELECT feed_id FROM article_feed_sources s
                  WHERE s.article_id = articles.id
                  ORDER BY discovered_at, feed_id LIMIT 1)
   WHERE feed_id IS NULL
   ```
3. Delete orphans that are **not** in any edition:
   ```sql
   DELETE FROM articles
   WHERE feed_id IS NULL
     AND NOT EXISTS (SELECT 1 FROM article_feed_sources s WHERE s.article_id = articles.id)
     AND NOT EXISTS (SELECT 1 FROM edition_articles e WHERE e.article_id = articles.id)
   ```
   Jobs cascade with the article.
4. Articles that are in editions survive with `feed_id = NULL` and their content intact. The UI shows the source as "(removed feed)".

Queries that `INNER JOIN feeds` (6 in `src/articles/mod.rs`) become `LEFT JOIN feeds` with `COALESCE(f.name, '(removed feed)')`.

### 5.2 GUID identity

- `article_feed_sources(feed_id, external_guid)` is the only GUID authority. Its existing partial unique index stays.
- Remove every read and write of `articles.external_guid` from application code (`articles/mod.rs:204-228, 252-259, 309-318`; `feeds/mod.rs:370-376`). New rows insert `NULL`.
- Article identity is `articles.id`, deduplicated by `canonical_url`. A GUID identifies an entry only within one feed.
- QUA-006 cannot recur, because there is no per-feed GUID uniqueness on `articles` any more.

### 5.3 Table-rebuild migration procedure (Stage 7)

SQLite cannot `ALTER` a foreign key. Follow SQLite's documented 12-step rebuild, with these Quire-specific traps:

1. The first line of the file must be `-- no-transaction`. `PRAGMA foreign_keys` is a no-op inside a transaction, and sqlx wraps migrations in one unless told otherwise.
2. `PRAGMA foreign_keys = OFF;` must come **before** `BEGIN;`.
   **Critical:** with foreign keys ON, `DROP TABLE articles` performs an implicit `DELETE` that **cascades into `edition_articles`, `article_feed_sources`, and `article_extraction_jobs`**, destroying exactly the data this migration exists to protect.
3. `BEGIN;` → `CREATE TABLE articles_new (...)` → `INSERT INTO articles_new (<explicit column list>) SELECT <same list> FROM articles;` → `DROP TABLE articles;` → `ALTER TABLE articles_new RENAME TO articles;` → recreate **every** index on `articles` from `0001` and `0002` (except the dropped GUID index) → `UPDATE articles SET feed_id = NULL WHERE feed_id NOT IN (SELECT id FROM feeds);` (defensive) → `COMMIT;` → `PRAGMA foreign_keys = ON;`.
4. Immediately after the migrator runs, `db::init_pool` must run `PRAGMA foreign_key_check` and `PRAGMA integrity_check`. If either reports a problem, refuse to start with a clear error. Pool connections are created with `foreign_keys = true` by `SqliteConnectOptions`, so the setting is restored for normal operation; the migration's `ON` also restores it on its own connection.
5. **Required tests** before merging:
   - A real sqlx-migrated Phase 1 database (method of reviewer test r26) with feeds, articles, sources, jobs, an edition with 2 members, and settings. Run all migrations, then assert:
     - row counts per table are unchanged
     - `edition_articles` is unchanged, including `position`
     - `PRAGMA foreign_key_check` returns no rows
     - every expected index exists (`pragma_index_list('articles')`)
     - restart is idempotent
   - A failure-injection copy: break the `INSERT ... SELECT` (for example, add a column that doesn't exist) and verify the database is byte-for-byte unchanged and the app refuses to start.

### 5.4 Outbound network policy (Stage 8)

**Policy:** Quire fetches only **public unicast** destinations. Implement one classifier, `fn is_public_unicast(ip: IpAddr) -> bool`, as an allow-list:

- **IPv4:** allowed unless the address is in an IANA IPv4 special-purpose block that is not globally reachable:
  `0.0.0.0/8`, `10/8`, `100.64/10`, `127/8`, `169.254/16`, `172.16/12`, `192.0.0.0/24`, `192.0.2.0/24`, `192.88.99.0/24`, `192.168/16`, `198.18/15`, `198.51.100/24`, `203.0.113/24`, `224/4`, `240/4` (includes `255.255.255.255`).
- **IPv6:** allowed **only** inside `2000::/3`, and then not inside:
  - `2001::/23`: IETF protocol assignments, including Teredo `2001::/32` and benchmarking. Deny the whole block; the few globally reachable sub-blocks are irrelevant to feeds.
  - `2001:db8::/32`
  - `2002::/16` (6to4)
  - `3fff::/20` (documentation, RFC 9637)

  Everything outside `2000::/3` is denied by construction: IPv4-mapped and compatible, NAT64 `64:ff9b::/96`, ULA, link-local, site-local, multicast, loopback, unspecified.

**Enforcement points** (all mandatory):

1. URL literals: in `parse_and_validate_http_url`, using `Url::host()` with `Host::Ipv4`/`Host::Ipv6` (also fixes QUA-013).
2. Every DNS result, checked **inside the fetcher** whatever `HostResolver` is in use (fixes the r04 trust gap). All results must pass; any restricted result fails the request.
3. Every redirect hop (existing behavior; reuse the classifier).
4. After connecting: compare `response.remote_addr()` with the validated set for this hop, and abort with `SsrfBlocked` on mismatch or `None`. This is the "immediately before or at connection" check, and it catches any future change that bypasses pinning.

**Client configuration:**
- `.no_proxy()`, so environment proxies cannot bypass pinning.
- `resolve_to_addrs` pinning stays.
- DNS resolution is wrapped in `tokio::time::timeout(remaining_timeout, …)`.

**Ports:** any port is allowed. This is a documented decision; restricting ports is not required for this phase.

---

## 6. Stages and gates

Each stage lists its scope, findings, files, the tests that must pass (**gate**), and complexity. Gate tests run in CI. Docker and SIGKILL gates run locally with the scripts in `docs/reviews/phase-2-artifacts/`.

### Stage 1 — Governance and baseline reproduction

- **Findings:** QUA-009, QUA-020 (part).
- **Work:**
  - Restore every `AGENTS.md` section removed in `66980ec` (list in review Appendix I), keeping the new queue section. Update that section to reference the revised model in §2.
  - Port the reviewer tests listed as "baseline" in §7 into `tests/review_regressions.rs` with `#[ignore = "QUA-xxx: fixed in stage N"]`, so they are tracked and fail on demand.
- **Gate:**
  - `git diff f0ec769 -- AGENTS.md` shows only additions or intentional rewording approved by the owner.
  - `cargo test -- --ignored` reproduces the baseline failures listed in §7.
- **Complexity:** Small.

### Stage 2 — SQLite transactions (QUA-001, QUA-019; shippable alone)

- **Work:**
  - §3.1 classification and helper.
  - §3.2 `with_write_retry`.
  - Remove raw `BEGIN`.
  - Remove reliance on the 50 ms stagger (keep it only as smoothing or delete it).
  - Not yet merged with completion; that is Stage 4.
- **Files:** `src/db/mod.rs`, `src/articles/mod.rs`, `src/feeds/mod.rs`, `src/feeds/scheduler.rs`.
- **Gate:**
  - r17, r24, r25, and the r29 fetch-all branch report **0** failed batches over 5 consecutive runs.
  - New: 8 concurrent 200-entry persists plus 4 claim/complete loops → 0 failures, and every `tx_ms` below 2 s.
  - New: hold `BEGIN IMMEDIATE` on a separate connection for 12 s while a feed persists → the persist fails after the policy's retries, no articles are written, validators are unchanged, and the next run succeeds.
  - New: cancel (`drop`) a claim future between begin and commit → the next write on the pool succeeds (no leaked transaction).
- **Complexity:** Small.

### Stage 3 — Unified execution model (§2.1–§2.3)

- **Findings:** QUA-003, QUA-008, TRP-001, TRP-006.
- **Work:**
  - Instance lock.
  - `manual_requested_at`.
  - Manual and fetch-all become "mark due + notify".
  - Two dispatchers with `active < limit` admission.
  - Remove `trigger_manual_feed_fetch`'s `tokio::spawn`, `run_scheduler_tick`'s inline drain, and `drain_eligible_extractions`'s lockstep batches.
  - Panics are logged.
- **Files:** `src/main.rs`, `src/feeds/scheduler.rs`, `src/feeds/mod.rs`, `src/web/mod.rs`, migration `0003_work_ownership.sql`.
- **Gate:**
  - r23: max in-flight ≤ limit for fetch-all.
  - New: 8 active fetches, then limit set to 2 → the fixture sees 0 new requests until in-flight < 2, then never more than 2.
  - New: 20 slow extraction jobs (5 s each, concurrency 2) plus a due feed → the feed run starts within 6 s.
  - New: a second process on the same data directory exits non-zero with the lock message.
  - New: click Fetch now, SIGKILL, restart → the feed is fetched after restart with `trigger_type = 'manual'`.
- **Complexity:** Medium.

### Stage 4 — Ownership and recovery (§2.4)

- **Findings:** QUA-004, QUA-010, QUA-011, TRP-002, TRP-003.
- **Work:**
  - Claim tokens.
  - `current_run_id`.
  - Merged ownership-checked feed completion transaction.
  - Deadlines, release rules, and startup recovery with exhausted-attempt handling.
- **Gate:**
  - r18 (adapted): a panic injected in the extraction path is released and retried without restart.
  - r19 (adapted): a stale token completion is rejected and the newer result is intact.
  - r20 (adapted): ≤ `max_attempts` claims across crash cycles, then `failed`.
  - New feed equivalent of r19: run A is superseded by run B → A's completion changes 0 rows and persists 0 articles; feed validators equal B's.
  - New: deadline exceeded → the item is released only after the future has been dropped (assert the fixture connection was closed before the job became `queued`).
  - New: SIGKILL during the feed completion transaction → no partial articles, the run is `cancelled` after restart, and the feed is due again.
  - New: SIGKILL after extraction but before completion → reclaimed and completed after restart.
- **Complexity:** Medium.

### Stage 5 — Independence and backlog visibility (§2.6)

- **Findings:** QUA-008 (verification), TRP-008.
- **Work:** dashboard backlog metrics and the due/running feed counts.
- **Gate:**
  - New: 500 queued jobs with the extraction fixture blocked → feeds keep polling on schedule, and the dashboard shows queued = 500, processing = limit, and an increasing oldest age.
  - Dashboard counts match direct SQL.
- **Complexity:** Small.

### Stage 6 — Extraction semantics (§4)

- **Findings:** QUA-002, QUA-014, QUA-023, TRP-009, TRP-010.
- **Work:**
  - Content/execution state.
  - `sync_legacy_status`.
  - The `'none'` fix and data migration (in `0003` if not yet released, otherwise `0005`).
  - The retry budget, `Retry-After` at the job layer, and the content-type gate.
- **Gate:**
  - r13 and r14 (adapted to §4.2).
  - New: 503, 503, then 200 → `html` + `completed`, 3 attempts; the summary is shown in between as "Summary (full text pending)".
  - New: 503 ×3 → `summary_fallback` + `failed` → "Summary only".
  - New: 429 with `Retry-After: 120` → `next_eligible_at ≥ now + 120 s`.
  - New: a request counter proves ≤ 6 requests per article in the worst case.
  - r15: PDF → not `html`.
  - Every combination in the §4.1 table maps to exactly one display state, and `sync_legacy_status` matches.
- **Complexity:** Medium.

### Stage 7 — Article independence and identity (§5.1–§5.3)

- **Findings:** QUA-006, QUA-007, QUA-022, TRP-011, TRP-012.
- **Work:**
  - Rebuild migration `0004`.
  - Deletion policy.
  - LEFT JOINs.
  - Remove `articles.external_guid` usage.
  - Backfill jobs for legacy pending articles: `INSERT OR IGNORE INTO article_extraction_jobs (article_id) SELECT id FROM articles WHERE extracted_content_type = 'none' AND id NOT IN (SELECT article_id FROM article_extraction_jobs)`.
- **Gate:**
  - All §5.3 migration tests.
  - r21 (adapted): the edition member survives with its content, the source shows as "(removed feed)", and `edition_articles` is unchanged.
  - r22: deletion succeeds.
  - New: the orphan not in an edition is deleted, and the shared article is repointed to the surviving feed.
  - r26 extended: the legacy pending article gets exactly one job.
- **Complexity:** Large.

### Stage 8 — Parsing and network security (§5.4)

- **Findings:** QUA-005, QUA-012, QUA-013, QUA-015, QUA-016, QUA-018, TRP-013, TRP-014.
- **Work:**
  - Remove the XML text filter.
  - Network policy and the four enforcement points.
  - `.no_proxy()` and the DNS timeout.
  - IPv6 literals.
  - Newest-first entry limit.
  - Minimal query rewriting.
  - `spawn_blocking` for parse, extract, and sanitize.
- **Gate:**
  - r02 and r04 (adapted: expect `SsrfBlocked`), r03 (adapted: a public v6 literal is accepted by validation and dialed directly), r07, r08, r10, r11, and r01 extended with NAT64, 6to4, Teredo, `fec0::/10`, `3fff::/20`, and IPv4-mapped targets.
  - New XML test through the **production path** (`execute_claimed_feed_fetch` against a fixture) with:
    - billion-laughs
    - `SYSTEM "file:///etc/passwd"`
    - an external DTD `SYSTEM "http://<fixture>/evil.dtd"`

    Assert: the run completes or fails within the feed timeout; no field contains expanded text or file content; the fixture's `/evil.dtd` receives **0** requests; peak response bytes ≤ the configured limit.
  - New: a resolver returning `[public, 10.0.0.1]` → blocked inside the fetcher.
  - New: `HTTPS_PROXY` set in the process env → the request still goes direct (fixture proxy receives 0 connections).
  - New: a resolver that never returns → the request fails at the deadline.
  - New: a 10 MB extraction does not delay `/health` by more than 100 ms.
- **Complexity:** Medium.

### Stage 9 — Graceful shutdown (§2.5)

- **Findings:** QUA-017, TRP-007.
- **Work:**
  - `CancellationToken` through the dispatchers and HTTP layer.
  - Ownership-checked release with the attempt revert.
  - Accurate shutdown logs.
  - `stop_grace_period: 20s` in compose and the README snippet.
- **Gate:**
  - New (crash probe): SIGINT with 4 in-flight jobs → exits within 6 s, jobs `queued` with their attempts unchanged from before the claim, and the log says `shutdown complete`.
  - New: SIGKILL with 4 in-flight jobs → attempts incremented; startup recovery requeues them.
  - Docker: `docker compose stop` during extraction → the container exits before the grace period; recovery is not needed.
- **Complexity:** Small.

### Stage 10 — Remaining P3s and documentation

- **Findings:** QUA-020, QUA-021.
- **Work:**
  - Split the 10 large integration tests into focused ones.
  - Make the migration test use the real migrator.
  - README:
    - concurrency semantics
    - single instance and local storage only
    - manual-request durability and coalescing
  - `docs/architecture.md`:
    - the §2 execution model
    - the §4.1 state table
    - the §4.2 retry budget
    - the §5.4 network policy
    - delivery semantics
    - the `synchronous = NORMAL` durability note
  - Config and scheduling:
    - make `QUIRE_FETCH_MIN_INTERVAL_MINUTES` accept `5..=10080`
    - recompute `next_scheduled_fetch_at` when a feed's interval changes
  - Optional: add `cargo audit` to CI.
- **Gate:**
  - Docs reviewed against the code.
  - `cargo audit` result recorded (install with `cargo install cargo-audit --locked`).
  - The full suite, including previously ignored regressions, passes with no `#[ignore]` left.
- **Complexity:** Medium.

### Stage 11 — Full validation for re-review

- **Work:** re-run review Appendix A end to end:
  - reviewer tests (adapted)
  - SIGKILL probe
  - `docker1.sh` and `docker2.sh`
  - load tests r27, r28, r29
- Additionally, with a Portainer-style deployment:
  1. `QUIRE_IMAGE` set to a locally built tag.
  2. Seed feeds, articles, settings, an edition, and queued jobs.
  3. `docker compose up -d --force-recreate`.
  4. Assert nothing is lost and queued jobs resume.
- **Gate:** all green; then request an independent re-review.
- **Complexity:** Small (execution only).
- **Docker tooling conflict (owner decision required before Stage 11):**
  - `AGENTS.md` §9 (added in `73aca0d`) requires `./start`/`./stop` for every local Docker run.
  - Those scripts always operate on the developer's `quire` container and its default volume, and they load `./.env`.
  - The Docker gates in this plan seed, SIGKILL, and recreate containers, so they must not run against that container or its data. That is why `docker1.sh`/`docker2.sh` use raw `docker compose` with a separate project (`quirereview`), container, port, and volume.
  - Pick one:
    - **(a) Recommended.** Extend `./start`/`./stop` to accept an isolated profile, for example `QUIRE_COMPOSE_PROJECT`, `QUIRE_ENV_FILE`, and a container-name override. Then port the two scripts onto them.
    - **(b)** Approve the isolated harness as a documented exception in `AGENTS.md` §9.

  Until one is chosen, run the Stage 11 Docker steps only with explicit owner approval.

---

## 7. Test adaptation table (TRP-016)

"Baseline" means the test is ported in Stage 1 as an ignored regression that reproduces the original defect.

| Test | Invariant it protects | Still applies? | Adaptation | Stage |
|---|---|---|---|---|
| r01 | redirects to restricted destinations are never followed | yes | add NAT64, 6to4, Teredo, site-local, `3fff::/20`, mapped | 8 |
| r02 | non-public IPv6 is rejected | yes | assert via `!is_public_unicast` | 8 (baseline) |
| r03 | public IPv6 literals are fetchable | yes | assert validation accepts and the fetcher dials without DNS | 8 (baseline) |
| r04 | the fetcher enforces policy independent of the resolver | yes | expect `SsrfBlocked` | 8 (baseline) |
| r05, r06 | size and time bounds | yes | none | already passing |
| r07, r08 | legitimate feeds parse | yes | none | 8 (baseline) |
| r09 | entities are inert | yes | move to the production path (Stage 8 gate) | 8 |
| r10 | the newest entries are kept | yes | none | 8 (baseline) |
| r11 | URL identity is preserved | yes | none | 8 (baseline) |
| r12 | stored XSS is impossible | yes | none | already passing |
| r13, r14 | transient failures are retried | yes | assert the §4.1 display states, not a raw `Failed` variant | 6 (baseline) |
| r15 | non-HTML is never "full text" | yes | assert content type ≠ `html` | 6 (baseline) |
| r16 | claims are unique under concurrency | yes | use the new claim function | already passing |
| r17, r24, r25, r29 | concurrent ingestion loses nothing | yes | r25 and r29 use the dispatcher instead of direct `execute_claimed_feed_fetch` after Stage 3 | 2 (baseline) |
| r18 | in-process failures are recoverable without restart | yes | **rewrite**: inject a panic; do not `drop` a claim (dropping a claim has no meaning under dispatcher ownership) | 4 (baseline) |
| r19 | a stale owner cannot overwrite | yes | **rewrite** using claim tokens; add a feed equivalent | 4 (baseline) |
| r20 | poison jobs are bounded | yes | assert terminal `failed` after `max_attempts` | 4 (baseline) |
| r21 | feed deletion preserves edition content | yes | **assertion changes**: the article survives with `feed_id NULL` (not "deletion blocked") | 7 (baseline) |
| r22 | feed deletion succeeds with shared GUIDs | yes | none | 7 (baseline) |
| r23 | the concurrency limit applies to manual work | yes | measure through dispatcher admission | 3 (baseline) |
| r26 | Phase 1 → current upgrade is lossless | yes | extend to migrations 0003 and 0004 and the §5.3 assertions | 7 |
| r27, r28 | throughput and failure load | measurement | keep as `#[ignore]` benchmarks; r28 expectations follow §4.2 | 11 |

New tests introduced by this plan are listed in each stage's gate. They cover the reviewer's §9 list:

| Scenario | Covered by |
|---|---|
| Lower concurrency while active | Stage 3 |
| Stale owner (extraction and feed) | Stage 4 |
| Slow but healthy work is not reclaimed (deadline-only release) | Stage 4 |
| SIGKILL during persistence and after extraction | Stage 4 |
| Repeated crashes | r20 |
| 503 then 200 | Stage 6 |
| Edition preservation | Stage 7 |
| Contention beyond the busy timeout | Stage 2 |
| Polling during backlog | Stages 3 and 5 |
| Container recreation | Stage 11 |

The reviewer's "heartbeat renews lease" test does not apply: this design has no time-based lease expiry (§2.4). Its intent (healthy slow work is never reclaimed) is covered by the Stage 4 deadline test.

---

## 8. Decisions summary (previously open)

| Decision | Choice |
|---|---|
| Instances per data directory | exactly one, enforced by `File::try_lock` on `/data/quire.lock`; local storage only |
| Manual fetch model | mark due + notify; durable across restart; coalesced; no-op while running |
| Concurrency control | per-class dispatcher, `active < limit` re-read every iteration; no semaphores |
| Recovering abandoned work | in process: release after the future finishes or is dropped at its deadline; across processes: startup recovery |
| Ownership | `claim_token` (jobs), `current_run_id` (feeds); every completion is conditional |
| Feed completion | one immediate transaction: ownership check + article upsert + run/feed update |
| Attempts | counted at claim; graceful-shutdown release reverts the increment atomically; crashes count |
| Content vs execution state | `extracted_content_type` vs job `status`; `extraction_status` is legacy and derived |
| Retry budget | ≤ 6 requests per article; HTTP layer handles resets only; job layer handles outages with `Retry-After` |
| Feed deletion | articles survive if in an edition; orphans not in an edition are deleted; shared articles are repointed |
| GUID authority | `article_feed_sources` only |
| Network policy | public-unicast allow-list at 4 points + post-connect peer check; no proxy; DNS timeout |
| Delivery semantics | DB transitions exactly-once per claim; HTTP at-least-once |

## 9. Suggested commit granularity

One commit per stage (Stage 7 may be two: the migration, then the code). Each commit:
- includes its gate tests
- passes the standard checks
- un-ignores the regressions fixed by that stage
