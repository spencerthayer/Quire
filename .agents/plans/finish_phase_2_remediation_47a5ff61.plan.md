---
name: Finish Phase 2 remediation
overview: Unblock the failing migration and tests, then finish the remaining Phase 2 remediation gates (tests, Docker review profile, docs, and a push to main) without rewriting the two commits already on origin/main.
todos:
  - id: unblock
    content: Fix migration 0004 via connection-level foreign_keys off, fill ExtractionResult fields, fmt, and correct the XXE assertion
    status: pending
  - id: gates-2-5
    content: Un-ignore and add Stage 2-5 gate tests; fix code only where a gate fails
    status: pending
  - id: gates-6-9
    content: Un-ignore and add Stage 6-9 gate tests, including the crash probe
    status: pending
  - id: docs-docker
    content: Update docs, run cargo audit, split oversized tests, run review-profile Docker checks
    status: pending
  - id: publish
    content: Run fmt, clippy, test, and release build; commit forward and push origin/main; write the execution log and final report
    status: pending
isProject: false
---

# Finish Phase 2 remediation

Library code for Stages 2–9 is already on `origin/main` at `39a5ded` (`c61cf34` plus the review-profile scripts). It does not build a green suite, and migration `0004` cannot apply, so the app cannot start. Do not rewrite those commits. Fix forward, then prove each remaining gate.

## 1. Unblock startup and the compiler

**Migration 0004.** [`sqlx-sqlite` 0.8.6 `apply`](file:///Users/spenceratgraybox/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/sqlx-sqlite-0.8.6/src/migrate.rs) always calls `begin()` and ignores `Migration.no_tx`. The file's own `BEGIN` then fails with "cannot start a transaction within a transaction". `-- no-transaction` cannot do what [§5.3](docs/reviews/phase-2-remediation-plan.md) assumed. `0004` has never applied (the insert into `_sqlx_migrations` happens only after the script succeeds), so edit [migrations/0004_article_independence.sql](migrations/0004_article_independence.sql) in place. Do not add `0005`.

- Remove `BEGIN`, `COMMIT`, and both `PRAGMA foreign_keys` lines. Leave the 12-step rebuild (create, copy, drop, rename, indexes, dangling `feed_id` cleanup) as one script inside sqlx's transaction.
- In [src/db/mod.rs](src/db/mod.rs) `init_pool`, acquire one connection, run `PRAGMA foreign_keys = OFF` before `sqlx::migrate!().run` on that same connection, then `PRAGMA foreign_keys = ON` before the connection returns to the pool. `PRAGMA foreign_keys` is a no-op inside a transaction, so it has to be set before sqlx begins one. Other pool connections stay `foreign_keys(true)` via `SqliteConnectOptions`.
- Keep the existing post-migrate `foreign_key_check` and `integrity_check`.

**Compile errors.** [tests/review_regressions.rs](tests/review_regressions.rs) lines 625 and 731 build `ExtractionResult::Failed` without `cancelled`, `interim_fallback`, and `retry_after`. Fill those fields.

**Format.** `cargo fmt --all`.

**XXE assertion.** [tests/integration_tests.rs](tests/integration_tests.rs) expects `parse_feed_bytes` to reject an external-entity feed. That text filter was removed because feed-rs does not expand entities. Change this test so a successful parse contains no expanded or file content. The production-path XML test stays a Stage 8 gate.

## 2. Prove the gates that the code already claims

Work in [tests/review_regressions.rs](tests/review_regressions.rs) and [tests/integration_tests.rs](tests/integration_tests.rs). Un-ignore a regression only when its stage's gate passes. There are 22 `#[ignore]` markers. Add the new gate tests named in [docs/reviews/phase-2-remediation-plan.md](docs/reviews/phase-2-remediation-plan.md) §6. Fix code only when a gate fails.

- **Stage 2:** r17, r24, r25, r29 at 0 failed batches over 5 runs. New: 8×200-entry persists plus 4 claim/complete loops with every `tx_ms` under 2 s. Hold `BEGIN IMMEDIATE` for 12 s and assert the persist fails cleanly, then the next run succeeds. Drop a claim future between begin and commit and assert the next write succeeds.
- **Stage 3:** r23. Concurrency lowered from 8 to 2 while fetches are active. A due feed starts within 6 s despite 20 slow extractions. A second process on the same data dir exits non-zero. Fetch now, SIGKILL, restart, and the run is `manual`.
- **Stage 4:** adapted r18, r19, r20. Stale feed completion writes 0 rows. Deadline release happens only after the future is dropped. SIGKILL during feed completion leaves no partial articles. SIGKILL after extraction but before completion is finished on restart.
- **Stage 5:** 500 queued jobs do not stall feed polling. Dashboard counts match SQL.
- **Stage 6:** adapted r13, r14, r15. 503, 503, 200. 503 three times. `Retry-After: 120`. At most 6 HTTP requests per article. Every §4.1 display state.
- **Stage 7:** §5.3 on a real Phase 1 database (row counts, `edition_articles.position`, `foreign_key_check`, index list, idempotent restart). A separate test runs a broken rebuild inside a transaction on a copy and checks the file is unchanged. Adapted r21, r22, orphan deletion, shared-article repoint, r26 with exactly one backfilled job.
- **Stage 8:** adapted r01–r04, r07, r08, r10, r11. Production-path XML (billion laughs, `file:///etc/passwd`, external DTD with 0 fixture hits). Resolver `[public, 10.0.0.1]` blocked. `HTTPS_PROXY` still goes direct. A resolver that never returns hits the deadline. A 10 MB extraction does not delay `/health` by more than 100 ms.
- **Stage 9:** crash probe. SIGINT with 4 in-flight jobs exits within 6 s, attempts unchanged, log contains `shutdown complete` ([src/main.rs](src/main.rs) already logs that). SIGKILL increments attempts and startup recovery requeues. `./stop --profile review` during extraction exits inside the 20 s grace period.

Leave r27 and r28 as ignored benchmarks.

## 3. Docs, audit, and local Docker

- Update [README.md](README.md), [docs/architecture.md](docs/architecture.md), and [docs/roadmap.md](docs/roadmap.md) to match §2, §4.1, §4.2, and §5.4, including single-instance local storage, manual-request durability, and `synchronous = NORMAL`. Record that publication must select `extracted_content_type = 'html'`.
- Note in the migration comment and the execution log that sqlx 0.8 SQLite ignores `-- no-transaction`.
- Run `cargo audit` and record the result. Do not claim it passed if it was not run.
- After the suite is green, split the oversized integration tests into focused cases (Stage 10). Behavior stays the same.
- Stage 11 uses only `./start --profile review` and `./stop --profile review`. [docs/reviews/phase-2-artifacts/docker1.sh](docs/reviews/phase-2-artifacts/docker1.sh) and `docker2.sh` already do this. Run them, plus the SIGINT and SIGKILL probes. A compose recreate with `QUIRE_IMAGE` set to a local tag is a Compose check, not a Portainer or ARM64 check. Never `--purge` the default profile and never use raw `docker compose` lifecycle commands.
- Fill [docs/reviews/phase-2-remediation-execution.md](docs/reviews/phase-2-remediation-execution.md) with each gate result. Do not mark a stage passed until its tests passed.

## 4. Close

Standard checks, in order: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --locked`, `cargo build --release --locked`.

New commits only, no amend and no force-push. Suggested split: migration and pool pragma, regression and gate tests, docs and execution log. Push to `origin/main`. Do not open or merge a PR. Do not start Phase 3. The closing report uses the §20 structure from the remediation request and recommends re-review only for what actually ran.
