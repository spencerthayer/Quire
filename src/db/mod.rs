use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use serde::Serialize;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{FromRow, SqlitePool};
use tracing::info;

use crate::error::AppResult;

#[derive(Debug, Clone, Serialize)]
pub struct DashboardCounts {
    pub total_feeds: i64,
    pub enabled_feeds: i64,
    pub total_articles: i64,
    pub extracted_articles: i64,
    pub fallback_articles: i64,
    pub pending_extractions: i64,
    pub failed_extractions: i64,
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

/// Initialize the SQLite connection pool with WAL mode and foreign keys enabled,
/// create parent directories when using a file-backed database, and run embedded
/// versioned SQLx migrations.
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

    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(connect_options)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    info!("SQLite database initialized and migrations applied");

    Ok(pool)
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
    let (total_articles,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(pool)
        .await?;
    let (extracted_articles,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM articles
         WHERE extraction_status = 'extracted'
           AND COALESCE(extracted_content_type, 'html') = 'html'",
    )
    .fetch_one(pool)
    .await?;
    let (fallback_articles,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*)
         FROM articles
         WHERE extraction_status = 'skipped'
            OR COALESCE(extracted_content_type, '') = 'summary_fallback'",
    )
    .fetch_one(pool)
    .await?;
    let (pending_extractions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM articles WHERE extraction_status = 'pending'")
            .fetch_one(pool)
            .await?;
    let (failed_extractions,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM articles WHERE extraction_status = 'failed'")
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
        total_articles,
        extracted_articles,
        fallback_articles,
        pending_extractions,
        failed_extractions,
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
