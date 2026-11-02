---
name: Finish Phase 2 remediation
overview: Fix migration 0004 and the broken test build, push that fix first so the published `:latest` image starts again, then prove the remaining Phase 2 remediation gates (tests, review-profile Docker checks, docs, audit) and push them as new commits. Never rewrite the commits already on origin/main.
todos:
  - id: unblock
    content: Run migrations on a dedicated foreign_keys=OFF connection, strip BEGIN/COMMIT/PRAGMA from 0004, add the 0004 data-preservation test, fill ExtractionResult fields, add the FixtureHost resolver, fix the XXE assertion, fmt
    status: pending
  - id: push-unblock
    content: Run the four standard checks, commit the unblock fix, push origin/main, and confirm CI and Publish Container Image succeed for that SHA
    status: pending
  - id: gates-2-5
    content: Add the Stage 2-5 gate tests and un-ignore r17, r18, r19, r20, r23, r24, r25, r29 when they pass; fix code only where a gate fails
    status: pending
  - id: gates-6-9
    content: Add the Stage 6-9 gate tests, un-ignore r01-r04, r07, r08, r10, r11, r13-r15, r21, r22, and run the host crash probe
    status: pending
  - id: docs-docker
    content: Update docs, install and run cargo audit, split oversized tests, port r27/r28 as ignored benchmarks, and run the review-profile Docker checks
    status: pending
  - id: publish
    content: Run fmt, clippy, test, and release build; commit forward and push origin/main; write the execution log and final report
    status: pending
isProject: false
---

# Finish Phase 2 remediation

Library code for Stages 2–9 is on `origin/main` at `39a5ded` (`c61cf34` plus the review-profile scripts). Migration `0004` cannot apply, so the app cannot start. The test suite also does not compile. Do not rewrite those commits. Fix forward, push the startup fix first, then prove each remaining gate.

**Why the fix ships first:** the `Publish Container Image` workflow does not depend on CI. It succeeded for `39a5ded` while CI failed. So `ghcr.io/spencerthayer/quire:latest` currently ships the broken `0004`. A stack that pulls it applies `0001`–`0003`, then exits at migration 4.

**Environment for every command below:**
- Working directory: `/Users/spenceratgraybox/Work/_Personal/Quire`.
- `cargo` is not on this shell's default `PATH`. Run `export PATH="$HOME/.cargo/bin:$PATH"` first.

## Acceptance criteria

- **AC1:** On a fresh database and on a database at `0003`, `quire` starts and every migration applies. Migration `0004` keeps every row in `edition_articles`, `article_feed_sources`, and `article_extraction_jobs`, including `position`.
- **AC2:** After the unblock push, CI and the container publish both succeed for the pushed SHA.
- **AC3:** Every gate in `docs/reviews/phase-2-remediation-plan.md` §6 for Stages 2–9 passes. Each test that needs the local fixture proves it reached the fixture.
- **AC4:** Only the benchmarks `r27` and `r28` stay `#[ignore]`.
- **AC5:** Docs match the code for §2, §4.1, §4.2, §5.1, and §5.4. A `cargo audit` result is recorded.
- **AC6:** The review-profile Docker checks pass using only `./start --profile review` and `./stop --profile review`.
- **AC7:** `docs/reviews/phase-2-remediation-execution.md` records each gate's actual result.
- **AC8:** New commits only; no amend and no force-push. No PR. No Phase 3 work.

## Evidence (reviewed 2026-11-01)

**Baseline:**
- HEAD is `f6fdc31`, which adds only this plan file. It is 1 commit ahead of `origin/main` at `39a5ded`.
- The worktree was clean at review start.
- This review changes only this plan file.

| Claim | Status | Evidence | Consequence |
| --- | --- | --- | --- |
| sqlx SQLite ignores `-- no-transaction` | verified | `sqlx-sqlite-0.8.6/src/migrate.rs` `apply` always calls `self.begin()`. `no_tx` is parsed (`sqlx-core-0.8.6/src/migrate/source.rs:127`) but this driver never reads it. | The `0004` header and its `BEGIN`/`COMMIT` must go. |
| `0004` fails and is never recorded | verified (ran) | `cargo test --test integration_tests` panics 4 times with `ExecuteMigration(... "cannot start a transaction within a transaction", 4)`. The `_sqlx_migrations` insert shares the failed transaction. | Editing `0004` in place is safe: no database has a checksum for it. |
| Removing `BEGIN`/`COMMIT` alone is enough | **contradicted (ran)** | Scratch check: the stripped `0004` run on a normal pool connection (foreign keys ON) returned Ok, but cascaded deletes emptied `edition_articles` (2→0), `article_feed_sources` (3→0), and `article_extraction_jobs` (2→0). | The connection must have foreign keys OFF before sqlx opens its transaction. The new test asserts row counts, so this trap can't pass silently. |
| A dedicated `foreign_keys(false)` connection preserves data | verified (ran) | Scratch check on a database seeded through `0003`, using `Migrator::run(&mut conn)`: all counts were unchanged and `edition_articles` kept `[(1,1,0),(1,3,1)]`. `foreign_key_check` returned no rows and `integrity_check` returned `ok`. `feed_id` became nullable, all 7 expected indexes exist, and `idx_articles_feed_external_guid` is gone. A rerun was a no-op, and a fresh database also migrated cleanly. | Use this design (`unblock`). |
| A failing `0004` leaves the database untouched | verified (ran) | Scratch check with a `0004` that inserts into a nonexistent column: the migrator returned an error, and counts, edition members, and applied versions `[1,2,3]` were unchanged. | The failure-injection gate compares the logical state. Byte equality is unreliable under WAL. |
| `review_regressions.rs` does not compile | verified (ran) | `cargo build --tests`: E0063 at `tests/review_regressions.rs:625:26` and `:731:10`. The fields `cancelled`, `interim_fallback`, and `retry_after` are missing (`src/articles/extractor.rs:30-36`). | Fill the fields. |
| The XXE test expects an error | verified (ran) | `tests/integration_tests.rs:457` fails. Scratch check: `parse_feed_bytes` returns Ok with `metadata.title == Some("&xxe;")` and 0 entries. In an item, `title` and `summary` are both literally `&xxe;`. | Assert the literal, unexpanded text. |
| Regression tests can reach the 127.0.0.1 fixture | **contradicted** | The fetcher blocks non-public resolver output and non-public peers unless `HostResolver::allows_test_fixture_addr` returns true (`src/feeds/fetcher.rs:392`, `:479`; `src/feeds/validation.rs:25`). The `AnyHost` resolver in `tests/review_regressions.rs:42` keeps the default `false`. | **r05 fails, and r06 passes falsely:** it gets a fast `SsrfBlocked` instead of a timeout. **r13–r15, r23, r25, and r29 would never reach the fixture.** In particular, r23's "in-flight ≤ limit" check passes trivially with 0 requests. Add `FixtureHost` (`unblock`). |
| There are 22 `#[ignore]` markers | contradicted (minor) | There are 21 attributes; the 22nd grep hit is the doc comment on line 2. | The counts below use 21. |
| r27 and r28 can "stay ignored" | contradicted | They exist only in `docs/reviews/phase-2-artifacts/review_tests.rs`, not in `tests/`. | Port them as ignored benchmarks (`docs-docker`). |
| Stage 2 gate: "hold `BEGIN IMMEDIATE` 12 s and the persist fails" | contradicted | The worst case of `with_write_retry` (`src/db/mod.rs:81-137`) with a 10 s busy timeout is about 31 s: 3 attempts plus 250 ms and 1 s backoff. A 12 s hold succeeds on attempt 2. | The test uses a pool with a 1 s busy timeout and an 8 s hold (`gates-2-5`). |
| The `QUIRE_IMAGE` recreate is the Stage 11 check | contradicted | The review profile uses `QUIRE_REVIEW_IMAGE` (default `quire:review`; `start:157`). `docker2.sh` already recreates with `./start --profile review --recreate --no-build`. | Use `docker2.sh`. Do not set `QUIRE_IMAGE`. |
| Docker can show in-flight extraction cancelled at stop | contradicted | The container enforces the public-unicast policy, so it cannot reach a host fixture. Seeded `.invalid` URLs fail immediately. | In-flight cancellation is proven by the host SIGINT probe. Docker proves a clean `./stop` under 20 s with `shutdown complete`. |
| `main.rs` logs `shutdown complete`, the lock exits non-zero, and `stop_grace_period` is 20 s | verified | `src/main.rs:31-40` and `:112`; `src/feeds/scheduler.rs:47-75` (message `Another Quire instance is using <dir>`); `compose.yaml` | No code change. |
| Stage 10 interval items | verified done | `src/config.rs:20-21`, `:975-985` (`5..=10_080`); `src/feeds/mod.rs:282` | Nothing left beyond the docs. |
| `cargo-audit` is installed | contradicted | It is not in `~/.cargo/bin`. | The owner chose to install it (`docs-docker`). |

The scratch checks ran from `/private/tmp/claude-502/-Users-spenceratgraybox-Work--Personal-Quire/ff650437-9990-41e1-866e-740e32b80b0d/scratchpad/migcheck/src/main.rs`, a separate crate that uses `quire` as a path dependency. The `unblock` code below follows the same API calls. It was inspected, not compiled in-repo.

## Decisions

- **Push timing (owner, 2026-11-01).** Asked: "When should the fixed migration be pushed?" Answer: **"Push the unblock fix first."** Result: new task `push-unblock` sits between `unblock` and the gate work. `publish` becomes the final push.
- **cargo audit (owner, 2026-11-01).** Asked how to handle the missing `cargo-audit`. Answer: **"Install it and run."** Result: `docs-docker` installs it with `cargo install cargo-audit --locked`. That is a user-level tool, not a project dependency.
- **Engineering choices:**
  - **Migration connection:** use a dedicated non-pool connection instead of toggling the pragma on a pooled connection. If a migration fails, a toggled pooled connection could go back into the pool with foreign keys OFF. A dedicated connection is also what lets the failure-injection test pass in a broken `Migrator`.
  - **Crash probes:** run in a disposable worktree, per review Appendix A.3, so `examples/` is not committed.
  - **SIGKILL during feed completion:** this can't be timed deterministically. It is covered two ways: an in-process dropped-future test that is deterministic, and a 10-kill invariant loop that is probabilistic and labeled as such.

## Tasks

### unblock

**Purpose:** AC1 and findings R1–R5. **Depends on:** nothing.

**1. `migrations/0004_article_independence.sql`** (edit in place; it has never applied anywhere):
- Delete line 1 (`-- no-transaction`).
- Delete `PRAGMA foreign_keys = OFF;`, `BEGIN;`, `COMMIT;`, and `PRAGMA foreign_keys = ON;`.
- Replace the comment block in lines 11–13 (`-- CRITICAL: ...`) with:

```sql
-- sqlx 0.8 SQLite runs every migration inside its own transaction and ignores
-- `-- no-transaction`; `PRAGMA foreign_keys` is a no-op inside a transaction.
-- `db::run_migrations` therefore applies migrations on a dedicated connection
-- opened with `foreign_keys = OFF`, so `DROP TABLE articles` cannot cascade into
-- `edition_articles`, `article_feed_sources`, or `article_extraction_jobs`.
-- Never apply this file on a connection with foreign keys ON.
```

Leave the rebuild statements unchanged.

**2. `src/db/mod.rs`:** change the imports on lines 9–10 to:

```rust
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, Connection, FromRow, Sqlite, SqlitePool, Transaction};
```

Add this function above `init_pool` (new symbol):

```rust
/// Apply `migrator` on a dedicated connection opened with `foreign_keys = OFF`.
///
/// sqlx 0.8 SQLite wraps every migration in a transaction and ignores
/// `-- no-transaction`, and `PRAGMA foreign_keys` is a no-op inside a
/// transaction. Table rebuilds (migration 0004) would otherwise cascade-delete
/// child rows. This connection never enters the pool.
pub async fn run_migrations(migrator: &Migrator, options: &SqliteConnectOptions) -> AppResult<()> {
    let mut conn = options.clone().foreign_keys(false).connect().await?;
    let migrate_result = migrator.run(&mut conn).await;
    let close_result = conn.close().await;
    migrate_result?;
    close_result?;
    Ok(())
}
```

In `init_pool`, run migrations before the pool is built. Replace lines 177–185 with:

```rust
    run_migrations(&sqlx::migrate!("./migrations"), &connect_options).await?;

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    verify_database_integrity(&pool).await?;
```

`AppError` already converts `sqlx::Error` and `MigrateError` (`src/error.rs:27`). Pool connections stay `foreign_keys(true)` from `connect_options`.

**3. `tests/review_regressions.rs` lines 625 and 731:** each `ExtractionResult::Failed { ... }` becomes:

```rust
&ExtractionResult::Failed {
    error_message: "x".into(), // line 731 keeps "stale".into()
    retryable: false,
    retry_after: None,
    interim_fallback: None,
    cancelled: false,
},
```

**4. `tests/review_regressions.rs`: fixture resolver.**

Add the following after `AnyHost` (it ends at line 52). Keep `AnyHost` unchanged: it models an untrusted resolver for r04.

```rust
/// Resolver that maps every hostname to the local fixture and permits exactly
/// that fixture address through the fetcher's test hook (models a public DNS
/// answer for tests that must reach the fixture).
struct FixtureHost(SocketAddr);
impl HostResolver for FixtureHost {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let a = self.0;
        Box::pin(async move { Ok(vec![a]) })
    }

    fn allows_test_fixture_addr(&self, addr: &SocketAddr) -> bool {
        *addr == self.0
    }
}
```

Change `AnyHost(` to `FixtureHost(` in r01 (line 250), r05 (323), r06 (340), r13 (484), r14 (503), r15 (536), r23 (843), r25 (926), and r29 (1066). Leave r04 (line 309) on `AnyHost`.

In r06, after `assert!(res.is_err());`, add an assertion that the error is a timeout and not an SSRF block:

```rust
    assert_eq!(res.unwrap_err().category, FetchErrorCategory::Timeout);
```

Literal redirect targets in r01 still go through the literal check, which never consults the hook (`src/feeds/fetcher.rs:316-345`).

**5. `tests/integration_tests.rs` lines 452–457:** replace the XXE block with:

```rust
    // 6. Reject invalid XML; external entities are never expanded
    assert!(parse_feed_bytes(b"not xml at all", None, &base_url, 200).is_err());
    let xxe_xml = br#"<?xml version="1.0"?>
    <!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
    <rss version="2.0"><channel><title>&xxe;</title></channel></rss>"#;
    let parsed_xxe =
        parse_feed_bytes(xxe_xml, None, &base_url, 200).expect("XXE feed parses inertly");
    assert_eq!(parsed_xxe.metadata.title.as_deref(), Some("&xxe;"));
    assert!(parsed_xxe.entries.is_empty());
```

The production-path XML test stays a Stage 8 gate.

**6. `tests/integration_tests.rs`: new migration test** (the §5.3 tests, moved forward so the push is safe). It uses only items the file already imports (`PathBuf`, `SqliteConnectOptions`, `SqlitePoolOptions`, `TempDir`, `db`).

```rust
async fn migration_snapshot(pool: &sqlx::SqlitePool) -> (Vec<i64>, Vec<(i64, i64, i64)>) {
    let mut counts = Vec::new();
    for table in [
        "feeds",
        "articles",
        "article_feed_sources",
        "article_extraction_jobs",
        "editions",
        "edition_articles",
        "settings",
    ] {
        let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap();
        counts.push(n);
    }
    let members = sqlx::query_as(
        "SELECT edition_id, article_id, position FROM edition_articles ORDER BY edition_id, article_id",
    )
    .fetch_all(pool)
    .await
    .unwrap();
    (counts, members)
}

#[tokio::test]
async fn test_migration_0004_preserves_child_rows_and_rolls_back_on_failure() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let tmp = TempDir::new().unwrap();
    let upto3 = tmp.path().join("upto3");
    let broken = tmp.path().join("broken");
    std::fs::create_dir_all(&upto3).unwrap();
    std::fs::create_dir_all(&broken).unwrap();
    for name in [
        "0001_initial_schema.sql",
        "0002_feed_ingestion.sql",
        "0003_work_ownership.sql",
    ] {
        std::fs::copy(src.join(name), upto3.join(name)).unwrap();
        std::fs::copy(src.join(name), broken.join(name)).unwrap();
    }
    std::fs::write(
        broken.join("0004_article_independence.sql"),
        "CREATE TABLE articles_new (id INTEGER PRIMARY KEY);\n\
         INSERT INTO articles_new (id, no_such_column) SELECT id, id FROM articles;\n\
         DROP TABLE articles;\n",
    )
    .unwrap();

    let db_path = tmp.path().join("quire.db");
    let url = format!("sqlite://{}", db_path.display());
    let options = SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .foreign_keys(true);

    let upto3_migrator = sqlx::migrate::Migrator::new(upto3.as_path()).await.unwrap();
    db::run_migrations(&upto3_migrator, &options).await.unwrap();
    let seed_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    for sql in [
        "INSERT INTO feeds (id, name, feed_url) VALUES (1, 'A', 'https://a.example/f'), (2, 'B', 'https://b.example/f')",
        "INSERT INTO articles (id, feed_id, canonical_url, title, extracted_content, extraction_status, extracted_content_type) VALUES (1, 1, 'https://a.example/1', 'one', '<p>x</p>', 'extracted', 'html'), (2, 1, 'https://a.example/2', 'two', NULL, 'pending', 'none'), (3, 2, 'https://b.example/3', 'three', NULL, 'pending', 'none')",
        "INSERT INTO article_feed_sources (article_id, feed_id, external_guid) VALUES (1, 1, 'g1'), (2, 1, 'g2'), (3, 2, 'g3')",
        "INSERT OR IGNORE INTO article_extraction_jobs (article_id, status) VALUES (2, 'queued'), (3, 'queued')",
        "INSERT INTO editions (id, title, publication_date) VALUES (1, 'E', '2026-11-01')",
        "INSERT INTO edition_articles (edition_id, article_id, position) VALUES (1, 1, 0), (1, 3, 1)",
        "INSERT INTO settings (key, value) VALUES ('fetch_interval_minutes', '60')",
    ] {
        sqlx::query(sql).execute(&seed_pool).await.unwrap();
    }
    let before = migration_snapshot(&seed_pool).await;
    assert_eq!(before.0, vec![2, 3, 3, 2, 1, 2, 1]);
    seed_pool.close().await;

    // A failing 0004 rolls back completely and is not recorded.
    let broken_migrator = sqlx::migrate::Migrator::new(broken.as_path()).await.unwrap();
    assert!(db::run_migrations(&broken_migrator, &options).await.is_err());
    let check_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    assert_eq!(migration_snapshot(&check_pool).await, before);
    let (max_version,): (i64,) = sqlx::query_as("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&check_pool)
        .await
        .unwrap();
    assert_eq!(max_version, 3);
    check_pool.close().await;

    // The real 0004 through init_pool keeps every child row.
    let pool = db::init_pool(&url, tmp.path()).await.expect("0004 applies");
    assert_eq!(migration_snapshot(&pool).await, before);
    let (fk_on,): (i64,) = sqlx::query_as("PRAGMA foreign_keys").fetch_one(&pool).await.unwrap();
    assert_eq!(fk_on, 1);
    let (feed_id_notnull,): (i64,) =
        sqlx::query_as("SELECT \"notnull\" FROM pragma_table_info('articles') WHERE name = 'feed_id'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(feed_id_notnull, 0);
    let indexes: Vec<(String,)> =
        sqlx::query_as("SELECT name FROM pragma_index_list('articles') ORDER BY name")
            .fetch_all(&pool)
            .await
            .unwrap();
    let indexes: Vec<String> = indexes.into_iter().map(|r| r.0).collect();
    assert_eq!(
        indexes,
        vec![
            "idx_articles_canonical_url",
            "idx_articles_content_hash",
            "idx_articles_discovered_at",
            "idx_articles_extraction_retry",
            "idx_articles_extraction_status",
            "idx_articles_feed_id",
            "idx_articles_published_at",
        ]
    );
    pool.close().await;

    // Restart is idempotent.
    let pool = db::init_pool(&url, tmp.path()).await.expect("restart");
    assert_eq!(migration_snapshot(&pool).await, before);
}
```

**7. Existing test `test_phase1_to_phase2_migration_preserves_existing_data`** (`tests/integration_tests.rs:886`):
- If, after steps 1–6, it fails only on assertions that `0004` intentionally changed (`feed_id NOT NULL`, the GUID index, the `'html'` default), update those assertions to the §5.1 schema.
- Any other failure is a code defect. Fix it and record it in the execution log.

**8.** Run `cargo fmt --all`.

**Verification (future execution):**

```bash
cargo build --tests --locked
cargo test --locked --lib --test integration_tests
cargo test --locked --test review_regressions
```

Expected results:
- The build has no errors.
- `integration_tests`: 11 passed, 0 failed (the 10 existing tests plus the new one).
- `review_regressions`: r05, r06, r09, r12, r16, and r26 pass, with 21 ignored.
- Before step 2, the new test fails at `init_pool` with "cannot start a transaction within a transaction". Confirm that once by stashing step 2.

### push-unblock

**Purpose:** AC2. **Depends on:** `unblock`.

1. Run the four standard checks. Each must exit 0 before anything is pushed. A red push still publishes `:latest`.

   ```bash
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test --locked
   cargo build --release --locked
   ```

2. Commit the revised plan and the fix separately:

   ```bash
   git add .agents/plans/finish_phase_2_remediation_47a5ff61.plan.md
   git commit -m "Revise finish-phase-2 plan after review"
   git add migrations/0004_article_independence.sql src/db/mod.rs tests/integration_tests.rs tests/review_regressions.rs
   git commit -m "Apply migrations with foreign keys off and unblock the test build"
   ```

3. Push: `git push origin main`. Fast-forward only. If it is rejected, run `git pull --rebase origin main`. Never force-push.

4. Watch both workflows for the pushed SHA:

   ```bash
   SHA=$(git rev-parse HEAD)
   gh run list --commit "$SHA" --json databaseId,workflowName,status,conclusion
   gh run watch <CI id> --exit-status
   gh run watch <Publish id> --exit-status
   ```

   Both must report `success`. If CI fails, fix forward with a new commit and push again.

5. Record the SHA, both run IDs, and their conclusions in the execution log.

   A database that ran the `39a5ded` image is at `0003` and needs no manual step. This is verified by the scratch check above.

### gates-2-5

**Purpose:** AC3 for Stages 2–5. **Depends on:** `push-unblock`.

Put new tests in `tests/review_regressions.rs`. Use `FixtureHost` for any test that needs the fixture. Each fixture test also asserts the fixture's hit counter is greater than 0, so a run blocked by the SSRF policy cannot pass. Remove a stage's `#[ignore]` only after its gate passes. If a gate fails, fix the code it names and record that fix.

| Stage | Test | Setup and action | Pass condition |
| --- | --- | --- | --- |
| 2 | r17, r24, r25, r29 | Un-ignore. Run `for i in 1 2 3 4 5; do cargo test --locked --test review_regressions -- r17 r24 r25 r29 || exit 1; done` | All 5 runs pass; each test's failed-batch counter is 0 |
| 2 | `s2_concurrent_persists_and_claims_stay_under_budget` | Run 8 tasks, each calling `articles::persist_discovered_entries` with 200 entries for its own feed. In parallel, run 4 loops of `claim_next_extraction_job` / `complete_extraction_job` until the persists finish. Time every call. | 0 errors, and every call takes under 2 s. Call duration is an upper bound on `tx_ms`. |
| 2 | `s2_persist_fails_cleanly_when_writer_lock_held` | Build a pool via `db::run_migrations` and `SqlitePoolOptions` with `busy_timeout(1s)`. A second connection runs `BEGIN IMMEDIATE` and holds it for 8 s. Persist 5 entries in the meantime. | The persist returns Err, and `db::is_retryable_sqlite_busy` is true. It writes 0 articles, and the feed's `etag`/`last_modified` are unchanged. After the hold is released, the same persist succeeds. The worst case is about 4.5 s, below the 8 s hold. |
| 2 | `s2_dropped_claim_future_releases_connection` | Run `tokio::time::timeout(Duration::from_micros(1), claim_next_extraction_job(&pool))` 50 times. | Afterwards a write (`persist_discovered_entries`) succeeds within 1 s |
| 3 | r23 | Un-ignore (now on `FixtureHost`) | Max in-flight ≤ limit, and fixture hits > 0 |
| 3 | `s3_lowering_fetch_concurrency_admits_no_new_work` | Use 8 feeds on a fixture that holds responses for 3 s. Start the coordinator with `QUIRE_FETCH_CONCURRENCY` unset and fetch concurrency at 8 via settings. Once 8 are in flight, save setting `fetch_concurrency=2`. | The fixture's in-flight gauge never rises again until it drops below 2, and after that never exceeds 2 |
| 3 | `s3_due_feed_starts_during_extraction_backlog` | Queue 20 extraction jobs against a fixture with a 5 s delay, with extraction concurrency 2. Then add a due feed. | A `feed_fetch_runs` row for that feed exists within 6 s |
| 3 | `s3_second_process_exits_nonzero` | Spawn `env!("CARGO_BIN_EXE_quire")` twice with the same `QUIRE_DATA_DIR`/`QUIRE_DATABASE_URL`, `QUIRE_FETCH_ENABLED=false`, and different `QUIRE_PORT`. Wait for the first `/health` before starting the second. | The second exits non-zero within 10 s, and stderr contains `Another Quire instance is using`. Kill the first afterwards. |
| 3 | `s3_manual_request_survives_restart` | `mark_feed_manual_fetch_due`, then drop the state without running the coordinator, then `init_pool` again and run one coordinator tick against the fixture | The new run has `trigger_type = 'manual'`. Durability is SQLite's commit, so this in-process restart stands in for SIGKILL. |
| 4 | r18, r19, r20 | Un-ignore and adapt per remediation plan §7. r18 injects a panic, not a dropped claim. r19 uses claim tokens. r20 asserts `failed` after `max_attempts`. | Pass |
| 4 | `s4_stale_feed_run_writes_nothing` | Claim run A, force a new run B with `try_claim_feed_fetch` after clearing A's ownership, then complete A with 3 entries and B with 1 | A changes 0 rows and adds 0 articles. The feed's validators equal B's. |
| 4 | `s4_deadline_release_after_future_dropped` | Use a fixture that never finishes the body and an article timeout of 2 s. Record the time the fixture sees the connection close, and the time the job becomes `queued`. | Connection close ≤ requeue time |
| 4 | `s4_dropped_feed_completion_is_atomic` | Wrap `complete_feed_fetch_run` with 200 entries in `timeout` at 1, 5, 20, and 50 ms | After each attempt, every article has an `article_feed_sources` row, and either all 200 or 0 are present |
| 5 | `s5_backlog_does_not_stall_polling` | Queue 500 jobs against a blocked fixture, with extraction concurrency 2, and add 1 feed due every 2 s | The feed gets ≥ 3 runs in 7 s. The dashboard shows queued ≥ 498 and processing = 2, and oldest-queued age increases between two reads. |
| 5 | `s5_dashboard_counts_match_sql` | Use mixed job states | Every `DashboardCounts` field equals its direct SQL count |

The table names the tests and fixes their pass conditions. The test bodies are written during execution against the fixture helpers in `tests/review_regressions.rs:100-240`.

### gates-6-9

**Purpose:** AC3 for Stages 6–9. **Depends on:** `gates-2-5`. The same `FixtureHost` and hit-count rule applies.

| Stage | Test | Pass condition |
| --- | --- | --- |
| 6 | r13, r14, r15 (adapted per §7) | Pass. r15's PDF result has `extracted_content_type != 'html'`. |
| 6 | `s6_503_503_200` | `html` + `completed`, 3 attempts. In between, `derive_display_state` returns the "summary, full text pending" state. |
| 6 | `s6_503_x3` | `summary_fallback` + `failed`, and the display state is "Summary only" |
| 6 | `s6_retry_after_120` | `next_eligible_at ≥ now + 120 s` |
| 6 | `s6_request_budget` | The fixture counter shows ≤ 6 requests for one article across all attempts |
| 6 | `s6_display_state_table` | Every pair in remediation plan §4.1 maps to exactly one `derive_display_state` value, and `derive_legacy_extraction_status` matches |
| 7 | r21, r22 (adapted) | r21: the edition member survives with `feed_id NULL` and shows "(removed feed)". r22: deletion succeeds. |
| 7 | `s7_delete_feed_orphans_and_repoints` | An orphan that is in no edition is deleted. A shared article is repointed to the surviving feed. |
| 7 | r26 extended | Covers `0003` and `0004`, and the legacy pending article gets exactly one job. The other §5.3 assertions are already in `unblock` step 6. |
| 8 | r01–r04, r07, r08, r10, r11 | Un-ignore. Extend r01 with NAT64, 6to4, Teredo, `fec0::/10`, `3fff::/20`, and IPv4-mapped redirect targets. |
| 8 | `s8_production_path_xml` | Run billion-laughs, `SYSTEM "file:///etc/passwd"`, and an external DTD at `http://<fixture>/evil.dtd` through the claimed feed fetch. Each completes or fails within the feed timeout. No field contains `root:` or expanded text, and `/evil.dtd` gets 0 hits. |
| 8 | `s8_resolver_mixed_answer_blocked` | A resolver returning `[93.184.216.34, 10.0.0.1]` with the default hook is blocked with `SsrfBlocked`, and the fixture gets 0 hits |
| 8 | `s8_https_proxy_ignored` | With `HTTPS_PROXY`/`HTTP_PROXY` set to a local proxy fixture (set with `unsafe { std::env::set_var }` in a single-threaded test), the proxy sees 0 connections and the target fixture sees ≥ 1 |
| 8 | `s8_resolver_hang_hits_deadline` | A resolver that never returns gives `Timeout` within `request_timeout + 1 s` |
| 8 | `s8_large_extraction_keeps_health_fast` | During a 10 MB extraction, `/health` p95 over 20 requests is ≤ 100 ms |

**Stage 9, host crash probe** (owner-run evidence; it does not run in CI):

1. Set up a disposable worktree:
   ```bash
   git worktree add --detach /tmp/quire-probe HEAD
   cd /tmp/quire-probe
   mkdir -p examples && cp docs/reviews/phase-2-artifacts/crashprobe.rs examples/
   cargo build --example crashprobe
   ```
2. Start the fixture: `python3 -I docs/reviews/phase-2-artifacts/fixture.py 18931 $C/delay 20` with a 30 s delay.
3. **SIGINT:** start the probe and wait for 4 `processing` jobs, then `kill -INT`.
   Pass: the process exits within 6 s, the jobs are `queued` with `attempts` as they were before the claim, and stderr contains `shutdown complete`.
4. **SIGKILL:** repeat with `kill -9`. Pass: attempts went up by 1. After restart, recovery requeues the jobs and they complete with a 0 s delay.
5. **SIGKILL during ingestion:** use a 200-item feed with a 0 s delay. Kill at a random 0.2–2.0 s offset, 10 times.
   After each kill, run these queries:
   ```sql
   SELECT count(*) FROM articles a WHERE NOT EXISTS (SELECT 1 FROM article_feed_sources s WHERE s.article_id = a.id);
   SELECT count(*) FROM articles a WHERE a.extracted_content_type = 'none' AND NOT EXISTS (SELECT 1 FROM article_extraction_jobs j WHERE j.article_id = a.id);
   ```
   Pass: both return 0. After a restart, no run is `running`, and the feed is due again or completes. Label this as probabilistic evidence; `s4_dropped_feed_completion_is_atomic` is the deterministic check.
6. Clean up: `cd - && git worktree remove --force /tmp/quire-probe`.

### docs-docker

**Purpose:** AC4–AC6. **Depends on:** `gates-6-9`.

1. **Docs.** Update:
   - `README.md`: concurrency semantics; single instance, local storage only; manual-request durability and coalescing.
   - `docs/architecture.md`:
     - §2 execution model, §4.1 state table, §4.2 retry budget, and §5.4 network policy.
     - `synchronous = NORMAL`.
     - Remove "partial unique index on `(feed_id, external_guid)`" from line 82; `0004` drops it.
     - State that publication must select `extracted_content_type = 'html'`.
   - `docs/roadmap.md`: the Phase 2 bullets match the shipped behavior.
   - Also record in the execution log that sqlx 0.8 SQLite ignores `-- no-transaction`.
2. **Audit.** Run `cargo install cargo-audit --locked`, then `cargo audit`. Record the full advisory list, or "0 vulnerabilities", plus the tool version. If an advisory affects a runtime path, add it as an open item; do not upgrade dependencies under this plan.
3. **Test split (Stage 10).** Split each `tests/integration_tests.rs` test over 150 lines into focused tests with the same assertions. Before and after, the summed count of `assert` calls (`grep -c 'assert'`) must match.
4. **Benchmarks.** Port `r27_load_100_feeds_1000_articles` and `r28_load_concurrent_extraction_failures` from `docs/reviews/phase-2-artifacts/review_tests.rs:849-930` into `tests/review_regressions.rs`:
   - mark both `#[ignore = "benchmark: Stage 11"]`
   - use `FixtureHost`
   - use `http://` article links
   - make r28 expectations follow §4.2

   Run `cargo test --locked --test review_regressions -- --ignored r27 r28 --nocapture`, and record throughput and RSS.

   Then `grep -c '#\[ignore' tests/review_regressions.rs` must print 3: two attributes plus the doc comment.
5. **Docker** (review profile only; requires the host `sqlite3` and BSD `tar`):
   ```bash
   bash docs/reviews/phase-2-artifacts/docker1.sh   # build, health, uid, CSRF, SSRF, settings, stop/start
   bash docs/reviews/phase-2-artifacts/docker2.sh   # timed stop, seed interrupted jobs, --recreate, recovery
   docker logs quire-review 2>&1 | grep -c "shutdown complete"   # read-only
   ./stop --profile review --purge
   docker rmi quire:review
   ```
   - Pass: health is `healthy`, and the uid is `10001`.
   - CSRF and SSRF codes match the review's recorded run (`docs/reviews/phase-2-independent-review.md:948-951`): bad token 403, no cookie 403, missing token field 422, `169.254.169.254` feed 400.
   - The feed and settings survive the restart.
   - `docker2.sh` prints `stop took` under 20 s. The seeded `processing` jobs show as recovered after `--recreate`.
   - `shutdown complete` appears at least once.

   This is a local Compose recreate check, not a Portainer or ARM64 check. Never use `--purge` on the default profile, never use raw `docker compose` lifecycle commands, and do not set `QUIRE_IMAGE`.

### publish

**Purpose:** AC7–AC8. **Depends on:** `docs-docker`.

1. Run the four standard checks from `push-unblock` step 1. All must exit 0.
2. Fill `docs/reviews/phase-2-remediation-execution.md`. For each stage, record:
   - its status
   - the exact commands and their pass/fail counts
   - the probe and Docker transcripts, summarized
   - the `cargo audit` output and the unblock push SHA with its run IDs

   A stage is marked passed only if its tests ran and passed. Gates that did not run are listed as not run.
3. Make new commits only, in this order:
   1. gate tests plus any code fixes, one commit per stage group (2–5, 6–9);
   2. test split and benchmarks;
   3. docs and the execution log.
4. Push to `origin/main`, then confirm CI and Publish as in `push-unblock` step 4. Do not open or merge a PR. Do not start Phase 3.
5. Close with a report that uses the §20 structure from the remediation request. That document is not in the repo; if it can't be found, use: summary, changes, gate results, checks not run, risks, re-review scope. Recommend re-review only for what actually ran.

## Coverage

| Requirement or finding | Tasks | Evidence |
| --- | --- | --- |
| AC1, R1, R2 | unblock | New migration test; `integration_tests` 11/11 |
| AC2 | push-unblock | Both `gh run watch` results are `success` |
| AC3, R3, R6 | unblock (FixtureHost), gates-2-5, gates-6-9 | Gate tables, with fixture hit counts > 0 |
| AC4, R4 | docs-docker step 4 | `grep -c` prints 3 |
| AC5, R8 | docs-docker steps 1–2 | Docs diff; audit output |
| AC6, R7 | docs-docker step 5 | Script output |
| AC7, AC8 | publish | Execution log; `git log origin/main` shows no rewritten SHAs |

## Review log

### 2026-11-01: plan review (Claude)

Inspected `f6fdc31` (HEAD) and `origin/main` at `39a5ded`. Checks run during review:
- `cargo build --tests`.
- `cargo test --lib --test integration_tests`: 5 passed, 5 failed.
- A scratch crate exercised the migrations and the XXE parse against `quire` as a path dependency.
- `gh run list`.

Nothing in the repo changed except this file.

| Finding | Severity | Evidence and impact | Disposition | Tasks |
| --- | --- | --- | --- | --- |
| R1 | blocker | Removing `BEGIN`/`COMMIT` alone wipes `edition_articles`, sources, and jobs on a foreign-keys-ON connection, and still returns Ok (scratch check). | Kept the plan's foreign-keys-OFF requirement. Specified a dedicated non-pool connection and a test that asserts child-row counts. | unblock |
| R2 | major | The original plan toggled the pragma on a pooled connection; a failed migration could return it to the pool with foreign keys OFF. | Replaced with `db::run_migrations` on a dedicated connection. That also enables failure injection. | unblock |
| R3 | blocker | `AnyHost` lacks the `allows_test_fixture_addr` hook. r05 fails, r06 passes falsely, and the r13–r15, r23, r25, and r29 gates could pass without any request reaching the fixture. CI would fail on r05. | Added `FixtureHost`, a timeout-category assertion in r06, and a hit-count rule for every fixture gate. | unblock, gates-2-5, gates-6-9 |
| R4 | major | r27 and r28 don't exist in `tests/`. | Port them as ignored benchmarks. | docs-docker |
| R5 | minor | The XXE replacement was vague. | Exact assertion, from observed parser output. | unblock |
| R6 | major | The Stage 2 "12 s hold → fails" gate cannot fail with a 10 s busy timeout × 3 attempts. | Use a test pool with a 1 s timeout and an 8 s hold. | gates-2-5 |
| R7 | major | The review profile ignores `QUIRE_IMAGE`, and the container cannot have in-flight extraction against a local fixture. | Use `docker2.sh` for recreate. Docker proves a clean stop; the host probe proves in-flight cancellation. | docs-docker, gates-6-9 |
| R8 | major | `:latest` was published from `39a5ded` while CI failed, so it is broken now. | Owner chose to push the fix first; added `push-unblock`. | push-unblock |
| R9 | minor | "22 ignores" is really 21. `cargo` is not on `PATH`. `cargo-audit` is not installed (the owner chose to install it). Stage 10 interval work is already done. | Corrected the counts, added the environment line, and made the install step explicit. | unblock, docs-docker |
| R10 | minor | The SIGKILL-in-completion gate can't be timed deterministically. | A deterministic dropped-future test plus a probabilistic 10-kill loop, labeled as such. | gates-2-5, gates-6-9 |

## Handoff

The plan is ready to execute. Next task: `unblock`. Implementation has not started. These checks were not run: clippy, the release build, the Stage 2–9 gates, the probes, Docker, and `cargo audit`. The `run_migrations` and test code was checked in a scratch crate, not compiled inside the repo.
