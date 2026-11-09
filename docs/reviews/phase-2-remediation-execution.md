# Quire Phase 2 — Remediation Execution Log

## 1. Baseline Record

* **Starting HEAD Commit**: `77fac0d` (`77fac0d915be8c80a7f5f245423107db8f8b7561` — "Update Phase 2 independent review and remediation plan documentation to include a new finding (QUA-023)...")
* **Original Phase 2 Implementation Commit**: `66980ec076cb8e5db536498d77395339efbff438`
* **Phase 1 Baseline Commit**: `f0ec769e6dc5675bcf21e93c0a9b643e2772c3c9`
* **Current Branch**: `main` (tracking `origin/main` at `https://github.com/spencerthayer/Quire.git`)
* **Working Tree Status at Inspection**: Clean (`git status --short` empty; Revision 2 of `docs/reviews/phase-2-remediation-plan.md` and `QUA-023` in `docs/reviews/phase-2-independent-review.md` were committed in `77fac0d`).
* **Existing Migrations**:
  * `migrations/0001_initial_schema.sql`
  * `migrations/0002_feed_ingestion.sql`
* **Existing Test Results (Pre-Remediation)**:
  * `cargo test`: 10 integration tests in `tests/integration_tests.rs` pass.
  * Reviewer suite (`docs/reviews/phase-2-artifacts/review_tests.rs`): 29 tests total (10 pass, 19 fail, documenting QUA-001 through QUA-023).
* **Existing Docker Configuration & Scripts**:
  * `Dockerfile`: Multi-stage build (`rust:1.97-slim-bookworm` -> `debian:bookworm-slim`), runs as nonroot user `quire` (`10001:10001`), healthcheck `/app/quire healthcheck`.
  * `compose.yaml`: Service `quire`, container `quire`, volume `quire_data:/data`, port `${QUIRE_PORT:-8080}:8080`.
  * `./start` & `./stop`: Bash lifecycle scripts that load `./.env`, invoke `docker compose`, and capture container logs into `./_logs/`.
* **Toolchain & Key Dependencies**:
  * `rustc`: `1.97.1 (8bab26f4f 2026-07-14)`
  * `cargo`: `1.97.1 (c980f4866 2026-06-30)`
  * `sqlx`: `0.8.6` (`libsqlite3-sys 0.30.1`)
  * `tokio`: `1.54.1` (declared `1.52`)
  * `axum`: `0.8.9`
  * `reqwest`: `0.13.5`
  * `feed-rs`: `3.0.0` (`quick-xml 0.42.0`)
  * `dom_smoothie`: `0.19.0`
  * `ammonia`: `4.2.4`

---

## 2. Stage Execution Progress

### Stage 1 — Governance and Baseline Reproduction
* **Status**: Completed (Gate passed)
* **Actions**:
  * Restored all binding rules removed from `AGENTS.md` in `66980ec` while retaining the standing SQLite extraction queue rules and extending `§9` for isolated `--profile review` Docker lifecycle execution.
  * Confirmed via `git diff f0ec769 -- AGENTS.md` that all Phase 1 requirements are preserved and only additive or updated Phase 2 rules are present.
  * Ported the reviewer regression suite into `tests/review_regressions.rs` with baseline defects marked `#[ignore = "QUA-xxx: fixed in Stage N"]`.
  * Verified `cargo test --test review_regressions` passes all 6 already-passing tests (`r05`, `r06`, `r09`, `r12`, `r16`, `r26`), and `cargo test --test review_regressions -- --ignored` reproduces all 20 baseline defect failures (`r02`, `r03`, `r04`, `r07`, `r08`, `r10`, `r11`, `r13`, `r14`, `r15`, `r17`, `r18`, `r19`, `r20`, `r21`, `r22`, `r23`, `r24`, `r25`, `r29`).

### Stage 2 — SQLite Transactions (QUA-001, QUA-019)
* **Status**: Passed
* **Code**: `SqlitePoolOptions::after_release` proves a pooled connection can `BEGIN IMMEDIATE`. If it cannot, the pool closes that connection. A cancelled sqlx transaction only queues `ROLLBACK`, and `SELECT 1` does not clear sqlx's transaction depth, so the next `BEGIN IMMEDIATE` was returning `InvalidSavePointStatement`.
* **Tests**: `s2_concurrent_persists_and_claims_stay_under_budget`, `s2_persist_fails_cleanly_when_writer_lock_held`, `s2_dropped_claim_future_releases_connection`, plus `r17`, `r24`, `r25`, `r29`.
* **Five quiet runs** of `cargo test --locked --test review_regressions -- r17 r24 r25 r29`: each run `4 passed; 0 failed`.
* **Timing**: the concurrent persist test's own run finished in 0.74s. In a fully parallel `cargo test` on a busy machine one persist was measured at 2.09s, so the assertion allows 3s. A held writer lock still fails the persist through the retry policy (`s2_persist_fails_cleanly_when_writer_lock_held`). The dropped-claim test passed 10/10 after the connection-close fix.
* **Full suite**: `cargo test --locked --test review_regressions` → `50 passed; 0 failed; 2 ignored`.

### Stage 3 — Unified execution model
* **Status**: Passed
* **Tests**: `r23_manual_fetch_all_respects_fetch_concurrency`, `s3_lowering_fetch_concurrency_admits_no_new_work`, `s3_due_feed_starts_during_extraction_backlog`, `s3_second_process_exits_nonzero`, `s3_manual_request_survives_restart`. Included in the 50 passing review tests.

### Stage 4 — Ownership and deadlines
* **Status**: Passed
* **Tests**: `s4_stale_feed_run_writes_nothing`, `s4_deadline_release_after_future_dropped`, `s4_dropped_feed_completion_is_atomic`. Included in the 50 passing review tests. The host SIGKILL loop below is the probabilistic pair of the dropped-completion test.

### Stage 5 — Backlog and dashboard
* **Status**: Passed
* **Tests**: `s5_backlog_does_not_stall_polling`, `s5_dashboard_counts_match_sql`. Included in the 50 passing review tests.

### Stage 6 — Display state and one retry budget
* **Status**: Passed
* **Tests**: `r13`, `r14`, `r15`, `s6_display_state_table`, `s6_503_503_200`, `s6_503_x3`, `s6_retry_after_120`, `s6_request_budget`.
* `s6_retry_after_120` uses a raw HTTP/1.1 429 with `Retry-After: 120`. The stored `next_eligible_at` is whole seconds, so the assertion allows a 1 second truncation (`delta >= 119`).

### Stage 7 — Article independence and deletion
* **Status**: Passed
* **Tests**: `r21`, `r22`, `r26` (job backfill count is 1), `s7_delete_feed_orphans_and_repoints`. Migration `0004` row preservation is `test_migration_0004_preserves_child_rows_and_rolls_back_on_failure` in `tests/integration_tests.rs`.
* **sqlx 0.8**: the SQLite driver ignores a `-- no-transaction` marker. `PRAGMA foreign_keys` is a no-op inside the transaction sqlx always opens. `db::run_migrations` applies migrations on a dedicated connection with foreign keys off. That connection is not in the pool. Pool connections keep foreign keys on.

### Stage 8 — Public-unicast network policy
* **Status**: Passed
* **Tests**: `r01` through `r04`, `r07`, `r08`, `r10`, `r11`, `s8_production_path_xml`, `s8_resolver_mixed_answer_blocked`, `s8_https_proxy_ignored`, `s8_resolver_hang_hits_deadline`, `s8_large_extraction_keeps_health_fast`.
* `s8_https_proxy_ignored` sets `HTTP_PROXY` and `HTTPS_PROXY` for the duration of that test and then removes them. The proxy saw 0 connections.

### Stage 9 — Crash probe
* **Status**: Passed (host probe, not CI). Labeled probabilistic for the SIGKILL-during-ingest loop. The deterministic pair is `s4_dropped_feed_completion_is_atomic`.
* Disposable worktree: `git worktree add --detach /tmp/quire-probe HEAD` at `83730e4`, with the uncommitted `src/db/mod.rs` connection-release fix copied in, and `docs/reviews/phase-2-artifacts/crashprobe.rs` built as `cargo build --example crashprobe`. The worktree was removed with `git worktree remove --force /tmp/quire-probe`. `examples/` was not committed.
* Fixture: `python3 -I docs/reviews/phase-2-artifacts/fixture.py 18931`.
* **SIGINT**: 4 processing jobs, attempts `1|1`. After `kill -INT` the process exited in 0.13s. stderr contained `shutdown complete`. All 20 jobs were `queued` with `attempts=0`.
* **SIGKILL**: attempts stayed `1` (one above the pre-claim value of 0). After restart with a 0s delay, 20 jobs completed.
* **SIGKILL during a 200-item ingest**, 10 times, kill delay uniform between 0.2s and 2.0s. Every iteration: orphan articles `0`, articles with `extracted_content_type='none'` and no job `0`, and after restart no run was `running` (`idle success=1`). Delays were 1.306, 0.326, 1.08, 1.602, 1.839, 0.992, 0.65, 1.133, 0.372, 1.178 seconds.

### Stage 10 — Interval bounds and test split
* **Status**: Passed. The `QUIRE_FETCH_MIN_INTERVAL_MINUTES` range `5..=10080` and `next_scheduled` recompute were already in the tree before this pass.
* `tests/integration_tests.rs` previously had one test over 150 lines (`test_end_to_end_ingestion_extraction_web_routes_and_restart`). It is now `test_end_to_end_feed_create_and_extraction`, `test_end_to_end_article_and_feed_pages`, and `test_end_to_end_settings_lock_and_restart`, sharing `e2e_after_first_tick`.
* `grep -c 'assert' tests/integration_tests.rs` is 150 before and after the split.

### Stage 11 — Benchmarks
* **Status**: Passed as ignored benchmarks. `grep -c '#\[ignore' tests/review_regressions.rs` prints 3 (the doc comment plus `r27` and `r28`).
* Command: `/usr/bin/time -l ./target/debug/deps/review_regressions-ad50b014b51f3dd7 --ignored r27 r28 --nocapture --test-threads=1`
* `r27`: first pass 25 ticks, 2.025s, 100 success runs, 1000 articles, 0 duplicate URLs, 1000 extracted, database plus WAL 6,082,712 bytes. Repeat pass 326ms, 1000 articles, 0 re-extracted.
* `r28`: 213ms. 100 rows of `summary_fallback` / `queued` / `attempts=1`. HTTP 500 stays retryable at the job layer.
* Process maximum resident set size for that run: 31,227,904 bytes. Peak memory footprint: 12,321,224 bytes.

### Stage 12 — Docs, audit, Docker
* **Status**: Passed for the checks below. This was a local Compose run, not Portainer and not ARM64.
* **cargo-audit**: installed with `cargo install cargo-audit --locked`. Tool version `cargo-audit 0.22.2`. Advisory database: 1422 advisories. `cargo audit` exit 1 with 5 vulnerabilities and 1 warning. Dependencies were not upgraded.
  * `rsa` 0.9.10, RUSTSEC-2023-0071 (Marvin Attack). `cargo tree -i rsa --target all` prints nothing, so this crate is in `Cargo.lock` and not in the compiled tree. Not a Quire runtime path.
  * `zip` 6.0.0 via `epub-builder` 0.8.3: RUSTSEC-2026-0429, RUSTSEC-2026-0367, RUSTSEC-2026-0368, RUSTSEC-2026-0369. The vulnerable functions are zip extraction. Quire does not call `epub-builder` or `ZipArchive::extract` in `src/`. Open item for Phase 5. Not upgraded here.
  * `basic-toml` 0.1.10, RUSTSEC-2026-0374, unmaintained warning, pulled by the `askama_derive` proc macro. Build time only.
* **Docker** (`docs/reviews/phase-2-artifacts/docker1.sh` then `docker2.sh`, review profile only):
  * `health=healthy` in about 8s. `uid=10001`. `/health` reported `journal_mode=wal` and `foreign_keys_enabled=true`.
  * Create feed `303`. Bad CSRF `403`. No CSRF cookie `403`. Missing token field `422`. `169.254.169.254` feed `400`.
  * `QUIRE_FETCH_ENABLED` post reports `locked by an environment variable`.
  * `extraction_concurrency=0` returns 400 with `Max Concurrent Extractions must be between 1 and 32.` The artifact grep looked for `concurrency must`, which does not match that sentence. The script now greps the review's recorded sentence.
  * After `./stop` and `./start --profile review --no-build`, the feed name `Review Feed` was present and `fetch_interval_minutes` was `120`.
  * `docker2.sh`: `stop took 0s` (one-second clock). Seeded jobs were `processing` with `attempts=1`. After `./start --profile review --recreate --no-build`: `Recovered interrupted ingestion state on startup recovered_fetches=1 recovered_extractions=2`. The feed page showed `Interrupted by application restart or shutdown.` and `cancelled`.
  * `docker logs quire-review | grep -c "shutdown complete"` immediately after `docker2.sh` printed `0`, because `--recreate` replaced the container that had stopped. A follow-up `./start --profile review` and `./stop --profile review --keep-container` logged `shutdown complete` once, about 300ms after the server started listening.
  * Cleanup: `./stop --profile review --purge` and `docker rmi quire:review`. The default profile was not purged.

### Unblock push
* **SHA**: `83730e4a460d7c3c76894b9339a47a199052dc5d` (`83730e4`), pushed to `origin/main`.
* **CI** run `42565825253`: success. https://github.com/spencerthayer/Quire/actions/runs/42565825253
* **Publish Container Image** run `42565825302`: still `in_progress` when this log was written. https://github.com/spencerthayer/Quire/actions/runs/42565825302

### Standard checks after the gate work
All four exited 0:
* `cargo fmt --all -- --check`
* `cargo clippy --all-targets --all-features -- -D warnings`
* `cargo test --locked` (integration tests `13 passed`; `review_regressions` `50 passed`, `2 ignored`)
* `cargo build --release --locked`

