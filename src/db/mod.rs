use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use rand::Rng;
use serde::Serialize;
use sqlx::migrate::Migrator;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{ConnectOptions, Connection, FromRow, Sqlite, SqlitePool, Transaction};
use tracing::{debug, info, warn};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Serialize)]
pub struct DashboardCounts {
    pub total_feeds: i64,
    pub enabled_feeds: i64,
    pub due_feeds: i64,
    pub total_articles: i64,
    pub extracted_articles: i64,
    pub fallback_articles: i64,
    pub pending_extractions: i64,
    pub processing_extractions: i64,
    pub failed_extractions: i64,
    pub completed_extractions_24h: i64,
    pub stuck_extractions: i64,
    pub oldest_queued_age_seconds: Option<i64>,
    pub running_fetches: i64,
    pub total_editions: i64,
    pub last_ingestion_activity_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct RecentFailureRow {
    pub feed_id: i64,
    pub feed_name: String,
    pub started_at: String,
    pub error_category: Option<String>,
    pub error_message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DbHealth {
    pub ok: bool,
    pub journal_mode: String,
    pub foreign_keys_enabled: bool,
}

/// Begin an immediate SQLite write transaction (`BEGIN IMMEDIATE`) managed by SQLx
/// so dropping or cancelling the `Transaction` automatically rolls it back.
pub async fn begin_immediate(pool: &SqlitePool) -> AppResult<Transaction<'static, Sqlite>> {
    Ok(pool.begin_with("BEGIN IMMEDIATE").await?)
}

/// Returns true if an `AppError` wraps a transient SQLite lock/busy error
/// (`SQLITE_BUSY` = 5, `SQLITE_LOCKED` = 6, `SQLITE_BUSY_RECOVERY` = 261,
/// `SQLITE_BUSY_SNAPSHOT` = 517).
pub fn is_retryable_sqlite_busy(err: &AppError) -> bool {
    if let AppError::Database(sqlx::Error::Database(db_err)) = err {
        if let Some(code_cow) = db_err.code()
            && let Ok(code_num) = code_cow.parse::<i64>()
        {
            let primary = code_num & 0xFF;
            if primary == 5 || primary == 6 {
                return true;
            }
        }
        let msg = db_err.message().to_ascii_lowercase();
        if msg.contains("database is locked") || msg.contains("database table is locked") {
            return true;
        }
    }
    false
}

/// Execute a whole immediate SQLite transaction closure with bounded retries
/// on `SQLITE_BUSY` / `SQLITE_LOCKED` (3 attempts total: initial + 250ms + 1000ms
/// with ±20% jitter). Logs transaction duration (`tx_ms`) at `debug!` and `warn!`
/// if a write transaction exceeds 2,000 ms.
pub async fn with_write_retry<T, F>(pool: &SqlitePool, mut op: F) -> AppResult<T>
where
    F: for<'c> FnMut(
        &'c mut Transaction<'static, Sqlite>,
    ) -> std::pin::Pin<Box<dyn Future<Output = AppResult<T>> + Send + 'c>>,
{
    const MAX_ATTEMPTS: u32 = 3;
    const BASE_DELAYS_MS: [u64; 2] = [250, 1000];

    for attempt in 1..=MAX_ATTEMPTS {
        let started = Instant::now();
        let begin_res = begin_immediate(pool).await;
        let mut tx = match begin_res {
            Ok(tx) => tx,
            Err(err) => {
                if attempt < MAX_ATTEMPTS && is_retryable_sqlite_busy(&err) {
                    sleep_with_jitter(BASE_DELAYS_MS[(attempt - 1) as usize]).await;
                    continue;
                }
                return Err(err);
            }
        };

        let body_res = op(&mut tx).await;
        match body_res {
            Ok(val) => match tx.commit().await.map_err(AppError::Database) {
                Ok(()) => {
                    let tx_ms = started.elapsed().as_millis() as u64;
                    if tx_ms > 2_000 {
                        warn!(
                            tx_ms,
                            attempt, "SQLite immediate transaction exceeded 2000ms budget"
                        );
                    } else {
                        debug!(tx_ms, attempt, "SQLite immediate transaction committed");
                    }
                    return Ok(val);
                }
                Err(commit_err) => {
                    if attempt < MAX_ATTEMPTS && is_retryable_sqlite_busy(&commit_err) {
                        sleep_with_jitter(BASE_DELAYS_MS[(attempt - 1) as usize]).await;
                        continue;
                    }
                    return Err(commit_err);
                }
            },
            Err(err) => {
                // Dropping `tx` rolls back automatically, or we can roll back explicitly
                let _ = tx.rollback().await;
                if attempt < MAX_ATTEMPTS && is_retryable_sqlite_busy(&err) {
                    sleep_with_jitter(BASE_DELAYS_MS[(attempt - 1) as usize]).await;
                    continue;
                }
                return Err(err);
            }
        }
    }

    unreachable!("with_write_retry loop always returns within MAX_ATTEMPTS")
}

async fn sleep_with_jitter(base_ms: u64) {
    let min_ms = (base_ms * 80) / 100;
    let max_ms = (base_ms * 120) / 100;
    let delay_ms = rand::rng().random_range(min_ms..=max_ms);
    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
}

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

/// Initialize the SQLite connection pool with WAL mode and foreign keys enabled,
/// create parent directories when using a file-backed database, run embedded
/// versioned SQLx migrations, and verify `PRAGMA foreign_key_check` and
/// `PRAGMA integrity_check`.
pub async fn init_pool(database_url: &str, data_dir: &Path) -> AppResult<SqlitePool> {
    tokio::fs::create_dir_all(data_dir).await.map_err(|e| {
        anyhow::anyhow!(
            "Failed to create data directory {}: {e}",
            data_dir.display()
        )
    })?;

    if let Some(db_file_path) = extract_sqlite_file_path(database_url)
        && let Some(parent) = db_file_path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            anyhow::anyhow!(
                "Failed to create database parent directory {}: {e}",
                parent.display()
            )
        })?;
    }

    let connect_options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(10));

    run_migrations(&sqlx::migrate!("./migrations"), &connect_options).await?;

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .after_release(|conn, _| {
            Box::pin(async move {
                // A cancelled `Transaction` only queues ROLLBACK. sqlx can return the
                // connection to the pool while its transaction depth is still non-zero,
                // and the next `BEGIN IMMEDIATE` then fails with InvalidSavePointStatement.
                // `SELECT 1` does not clear that depth. Prove an immediate transaction can
                // start. If it cannot, close the connection so the pool opens a fresh one.
                match conn.begin_with("BEGIN IMMEDIATE").await {
                    Ok(tx) => match tx.rollback().await {
                        Ok(()) => Ok(true),
                        Err(_) => Ok(false),
                    },
                    Err(_) => Ok(false),
                }
            })
        })
        .connect_with(connect_options)
        .await?;

    verify_database_integrity(&pool).await?;

    info!("SQLite database initialized and migrations applied");

    Ok(pool)
}

/// Verify `PRAGMA foreign_key_check` and `PRAGMA integrity_check` after running
/// migrations.
pub async fn verify_database_integrity(pool: &SqlitePool) -> AppResult<()> {
    let fk_rows: Vec<(String, Option<i64>, String, Option<i64>)> =
        sqlx::query_as("PRAGMA foreign_key_check")
            .fetch_all(pool)
            .await?;
    if !fk_rows.is_empty() {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Database foreign_key_check failed with {} violation(s): {:?}",
            fk_rows.len(),
            fk_rows.first()
        )));
    }

    let (integrity,): (String,) = sqlx::query_as("PRAGMA integrity_check")
        .fetch_one(pool)
        .await?;
    if !integrity.eq_ignore_ascii_case("ok") {
        return Err(AppError::Internal(anyhow::anyhow!(
            "Database integrity_check failed: {integrity}"
        )));
    }

    Ok(())
}

fn extract_sqlite_file_path(database_url: &str) -> Option<std::path::PathBuf> {
    if database_url.contains(":memory:") {
        return None;
    }
    let without_scheme = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite:"))?;
    let path_part = without_scheme.split('?').next().unwrap_or(without_scheme);
    if path_part.is_empty() {
        None
    } else {
        Some(std::path::PathBuf::from(path_part))
    }
}

pub async fn check_health(pool: &SqlitePool) -> AppResult<DbHealth> {
    let ping: (i64,) = sqlx::query_as("SELECT 1").fetch_one(pool).await?;
    let journal_mode: (String,) = sqlx::query_as("PRAGMA journal_mode")
        .fetch_one(pool)
        .await?;
    let foreign_keys: (i64,) = sqlx::query_as("PRAGMA foreign_keys")
        .fetch_one(pool)
        .await?;

    Ok(DbHealth {
        ok: ping.0 == 1,
        journal_mode: journal_mode.0.to_lowercase(),
        foreign_keys_enabled: foreign_keys.0 == 1,
    })
}

pub async fn get_dashboard_counts(pool: &SqlitePool) -> AppResult<DashboardCounts> {
    let (total_feeds,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM feeds")
        .fetch_one(pool)
        .await?;
    let (enabled_feeds,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM feeds WHERE enabled = 1")
        .fetch_one(pool)
        .await?;
    let (due_feeds,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM feeds
         WHERE enabled = 1
           AND fetch_in_progress = 0
           AND (next_scheduled_fetch_at IS NULL OR next_scheduled_fetch_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
    )
    .fetch_one(pool)
    .await?;
    let (total_articles,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(pool)
        .await?;
    let (extracted_articles,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM articles a
         LEFT JOIN article_extraction_jobs j ON j.article_id = a.id
         WHERE a.extracted_content_type = 'html'
           AND a.extracted_content IS NOT NULL
           AND a.extracted_content != ''
           AND COALESCE(j.status, 'completed') = 'completed'",
    )
    .fetch_one(pool)
    .await?;
    let (fallback_articles,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM articles a
         LEFT JOIN article_extraction_jobs j ON j.article_id = a.id
         WHERE a.extracted_content_type = 'summary_fallback'
           AND COALESCE(j.status, 'completed') IN ('completed', 'failed')",
    )
    .fetch_one(pool)
    .await?;
    let (pending_extractions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM article_extraction_jobs WHERE status = 'queued'")
            .fetch_one(pool)
            .await?;
    let (processing_extractions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM article_extraction_jobs WHERE status = 'processing'")
            .fetch_one(pool)
            .await?;
    let (failed_extractions,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM articles a
         LEFT JOIN article_extraction_jobs j ON j.article_id = a.id
         WHERE COALESCE(a.extracted_content_type, 'none') = 'none'
           AND (j.status = 'failed' OR (j.id IS NULL AND a.extraction_status = 'failed'))",
    )
    .fetch_one(pool)
    .await?;
    let (completed_extractions_24h,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM article_extraction_jobs
         WHERE status = 'completed'
           AND completed_at >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-24 hours')",
    )
    .fetch_one(pool)
    .await?;
    let (stuck_extractions,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM article_extraction_jobs
         WHERE status = 'processing'
           AND claimed_at IS NOT NULL
           AND claimed_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')",
    )
    .fetch_one(pool)
    .await?;
    let (oldest_queued_age_seconds,): (Option<i64>,) = sqlx::query_as(
        "SELECT CAST(strftime('%s', 'now') - strftime('%s', MIN(next_eligible_at)) AS INTEGER)
         FROM article_extraction_jobs
         WHERE status = 'queued'
           AND next_eligible_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')",
    )
    .fetch_one(pool)
    .await?;
    let (running_fetches,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM feeds WHERE fetch_in_progress = 1")
            .fetch_one(pool)
            .await?;
    let (total_editions,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM editions")
        .fetch_one(pool)
        .await?;
    let (last_ingestion_activity_at,): (Option<String>,) =
        sqlx::query_as("SELECT MAX(last_attempted_fetch_at) FROM feeds")
            .fetch_one(pool)
            .await?;

    Ok(DashboardCounts {
        total_feeds,
        enabled_feeds,
        due_feeds,
        total_articles,
        extracted_articles,
        fallback_articles,
        pending_extractions,
        processing_extractions,
        failed_extractions,
        completed_extractions_24h,
        stuck_extractions,
        oldest_queued_age_seconds: oldest_queued_age_seconds.map(|s| s.max(0)),
        running_fetches,
        total_editions,
        last_ingestion_activity_at,
    })
}

pub async fn get_recent_ingestion_failures(
    pool: &SqlitePool,
    limit: i64,
) -> AppResult<Vec<RecentFailureRow>> {
    let rows = sqlx::query_as::<_, RecentFailureRow>(
        "SELECT
            r.feed_id,
            f.name AS feed_name,
            r.started_at,
            r.error_category,
            COALESCE(r.error_message, 'Unknown error') AS error_message
         FROM feed_fetch_runs r
         INNER JOIN feeds f ON f.id = r.feed_id
         WHERE r.status IN ('failed', 'partial')
           AND r.error_message IS NOT NULL
         ORDER BY r.started_at DESC, r.id DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

pub async fn get_all_settings(pool: &SqlitePool) -> AppResult<HashMap<String, String>> {
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT key, value FROM settings ORDER BY key ASC")
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().collect())
}

/// Upsert nonsecret settings inside a transaction.
/// Rejects any attempt to store secret keys in the database.
pub async fn upsert_settings(pool: &SqlitePool, entries: &[(&str, &str)]) -> AppResult<()> {
    const ALLOWED_KEYS: &[&str] = &[
        "timezone",
        "fetch_enabled",
        "fetch_interval_minutes",
        "fetch_concurrency",
        "extraction_concurrency",
        "feed_request_timeout_seconds",
        "article_request_timeout_seconds",
        "max_feed_response_bytes",
        "max_article_response_bytes",
        "max_feed_entries",
        "fetch_max_attempts",
        "ai_provider",
        "ai_base_url",
        "ai_model",
    ];

    let mut tx = pool.begin().await?;
    for &(key, value) in entries {
        if !ALLOWED_KEYS.contains(&key) {
            return Err(crate::error::AppError::Validation(format!(
                "Setting key '{key}' cannot be stored in the database."
            )));
        }
        let trimmed = value.trim();
        if trimmed.is_empty() {
            sqlx::query("DELETE FROM settings WHERE key = ?")
                .bind(key)
                .execute(&mut *tx)
                .await?;
        } else {
            sqlx::query(
                "INSERT INTO settings (key, value, updated_at)
                 VALUES (?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
                 ON CONFLICT(key) DO UPDATE SET
                     value = excluded.value,
                     updated_at = excluded.updated_at",
            )
            .bind(key)
            .bind(trimmed)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}
