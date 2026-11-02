pub mod fetcher;
pub mod parser;
pub mod scheduler;
pub mod validation;

use std::time::Duration;

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use tracing::warn;

use crate::articles::persist_discovered_entries_in_tx;
use crate::config::{DEFAULT_FETCH_MAX_INTERVAL_MINUTES, DEFAULT_FETCH_MIN_INTERVAL_MINUTES};
use crate::db::with_write_retry;
use crate::error::{AppError, AppResult};
use crate::feeds::parser::{NormalizedFeedEntry, ParsedFeedMetadata};
pub use crate::feeds::validation::validate_http_url;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Feed {
    pub id: i64,
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    pub last_successful_fetch_at: Option<String>,
    pub last_attempted_fetch_at: Option<String>,
    pub last_error: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub last_fetch_status: String,
    pub last_http_status: Option<i64>,
    pub last_fetch_duration_ms: Option<i64>,
    pub last_discovered_count: i64,
    pub last_inserted_count: i64,
    pub last_extraction_count: i64,
    pub consecutive_failures: i64,
    pub next_scheduled_fetch_at: Option<String>,
    pub fetch_interval_minutes: Option<i64>,
    pub max_entries_override: Option<i64>,
    pub fetch_in_progress: bool,
    pub current_run_id: Option<i64>,
    pub manual_requested_at: Option<String>,
    pub feed_language: Option<String>,
    pub feed_updated_at: Option<String>,
    pub article_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FeedFetchRun {
    pub id: i64,
    pub feed_id: i64,
    pub trigger_type: String,
    pub status: String,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub duration_ms: Option<i64>,
    pub http_status: Option<i64>,
    pub entries_seen: i64,
    pub entries_skipped: i64,
    pub entries_truncated: i64,
    pub articles_inserted: i64,
    pub articles_updated: i64,
    pub articles_unchanged: i64,
    pub extraction_queued: i64,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct NewFeedInput {
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub fetch_interval_minutes: Option<i64>,
    pub max_entries_override: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateFeedInput {
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub fetch_interval_minutes: Option<i64>,
    pub max_entries_override: Option<i64>,
}

const FEED_SELECT_SQL: &str = "
    SELECT
        f.id,
        f.name,
        f.feed_url,
        f.website_url,
        f.description,
        f.enabled,
        f.created_at,
        f.updated_at,
        f.last_successful_fetch_at,
        f.last_attempted_fetch_at,
        f.last_error,
        f.etag,
        f.last_modified,
        f.last_fetch_status,
        f.last_http_status,
        f.last_fetch_duration_ms,
        f.last_discovered_count,
        f.last_inserted_count,
        f.last_extraction_count,
        f.consecutive_failures,
        f.next_scheduled_fetch_at,
        f.fetch_interval_minutes,
        f.max_entries_override,
        f.fetch_in_progress,
        f.current_run_id,
        f.manual_requested_at,
        f.feed_language,
        f.feed_updated_at,
        (
            SELECT COUNT(*)
            FROM article_feed_sources afs
            WHERE afs.feed_id = f.id
        ) AS article_count
    FROM feeds f
";

fn normalize_name(name: &str) -> AppResult<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "Feed name is required and cannot be empty.".to_string(),
        ));
    }
    if trimmed.chars().count() > 200 {
        return Err(AppError::Validation(
            "Feed name must not exceed 200 characters.".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_description(desc: Option<&str>) -> AppResult<Option<String>> {
    let Some(raw) = desc else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > 1000 {
        return Err(AppError::Validation(
            "Description must not exceed 1000 characters.".to_string(),
        ));
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_optional_website_url(url: Option<&str>) -> AppResult<Option<String>> {
    let Some(raw) = url else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_http_url(trimmed, "Website URL").map(Some)
}

fn validate_optional_interval(interval: Option<i64>) -> AppResult<Option<i64>> {
    let Some(val) = interval else {
        return Ok(None);
    };
    let min = i64::from(DEFAULT_FETCH_MIN_INTERVAL_MINUTES);
    let max = i64::from(DEFAULT_FETCH_MAX_INTERVAL_MINUTES);
    if !(min..=max).contains(&val) {
        return Err(AppError::Validation(format!(
            "Fetch interval override must be between {min} and {max} minutes."
        )));
    }
    Ok(Some(val))
}

fn validate_optional_max_entries(max_entries: Option<i64>) -> AppResult<Option<i64>> {
    let Some(val) = max_entries else {
        return Ok(None);
    };
    if !(1..=5000).contains(&val) {
        return Err(AppError::Validation(
            "Per-feed max entries override must be between 1 and 5000.".to_string(),
        ));
    }
    Ok(Some(val))
}

pub async fn list_feeds(pool: &SqlitePool) -> AppResult<Vec<Feed>> {
    let sql = format!("{FEED_SELECT_SQL} ORDER BY f.name COLLATE NOCASE ASC, f.id ASC");
    let feeds = sqlx::query_as::<_, Feed>(&sql).fetch_all(pool).await?;
    Ok(feeds)
}

pub async fn get_feed_by_id(pool: &SqlitePool, id: i64) -> AppResult<Feed> {
    let sql = format!("{FEED_SELECT_SQL} WHERE f.id = ?");
    let feed = sqlx::query_as::<_, Feed>(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Feed with ID {id} was not found.")))?;
    Ok(feed)
}

pub async fn create_feed(pool: &SqlitePool, input: NewFeedInput) -> AppResult<Feed> {
    let name = normalize_name(&input.name)?;
    let feed_url = validate_http_url(&input.feed_url, "Feed URL")?;
    let website_url = normalize_optional_website_url(input.website_url.as_deref())?;
    let description = normalize_optional_description(input.description.as_deref())?;
    let fetch_interval_minutes = validate_optional_interval(input.fetch_interval_minutes)?;
    let max_entries_override = validate_optional_max_entries(input.max_entries_override)?;
    let enabled_int: i64 = if input.enabled { 1 } else { 0 };
    let next_scheduled = if input.enabled {
        Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true))
    } else {
        None
    };

    let mut tx = pool.begin().await?;
    let insert_res = sqlx::query(
        "INSERT INTO feeds (
            name,
            feed_url,
            website_url,
            description,
            enabled,
            fetch_interval_minutes,
            max_entries_override,
            next_scheduled_fetch_at
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&name)
    .bind(&feed_url)
    .bind(&website_url)
    .bind(&description)
    .bind(enabled_int)
    .bind(fetch_interval_minutes)
    .bind(max_entries_override)
    .bind(&next_scheduled)
    .execute(&mut *tx)
    .await;

    let result = match insert_res {
        Ok(res) => res,
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            return Err(AppError::Conflict(format!(
                "A feed with URL '{feed_url}' is already configured."
            )));
        }
        Err(other) => return Err(AppError::Database(other)),
    };

    let id = result.last_insert_rowid();
    tx.commit().await?;

    get_feed_by_id(pool, id).await
}

pub async fn update_feed(pool: &SqlitePool, id: i64, input: UpdateFeedInput) -> AppResult<Feed> {
    let name = normalize_name(&input.name)?;
    let feed_url = validate_http_url(&input.feed_url, "Feed URL")?;
    let website_url = normalize_optional_website_url(input.website_url.as_deref())?;
    let description = normalize_optional_description(input.description.as_deref())?;
    let fetch_interval_minutes = validate_optional_interval(input.fetch_interval_minutes)?;
    let max_entries_override = validate_optional_max_entries(input.max_entries_override)?;
    let enabled_int: i64 = if input.enabled { 1 } else { 0 };

    // Recompute `next_scheduled_fetch_at` when a feed's interval override changes (QUA-021)
    let existing = get_feed_by_id(pool, id).await?;
    let recomputed_next = if !input.enabled {
        existing.next_scheduled_fetch_at.clone()
    } else if existing.fetch_interval_minutes != fetch_interval_minutes {
        if let Some(new_mins) = fetch_interval_minutes {
            if let Some(last_attempt) = existing
                .last_attempted_fetch_at
                .as_deref()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            {
                let next_dt =
                    last_attempt.with_timezone(&Utc) + ChronoDuration::minutes(new_mins.max(5));
                Some(next_dt.to_rfc3339_opts(SecondsFormat::Secs, true))
            } else {
                Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true))
            }
        } else {
            existing
                .next_scheduled_fetch_at
                .clone()
                .or_else(|| Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)))
        }
    } else {
        existing
            .next_scheduled_fetch_at
            .clone()
            .or_else(|| Some(Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)))
    };

    let mut tx = pool.begin().await?;
    let update_res = sqlx::query(
        "UPDATE feeds
         SET name = ?,
             feed_url = ?,
             website_url = ?,
             description = ?,
             enabled = ?,
             fetch_interval_minutes = ?,
             max_entries_override = ?,
             next_scheduled_fetch_at = ?,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(&name)
    .bind(&feed_url)
    .bind(&website_url)
    .bind(&description)
    .bind(enabled_int)
    .bind(fetch_interval_minutes)
    .bind(max_entries_override)
    .bind(&recomputed_next)
    .bind(id)
    .execute(&mut *tx)
    .await;

    let result = match update_res {
        Ok(res) => res,
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            return Err(AppError::Conflict(format!(
                "Another feed with URL '{feed_url}' already exists."
            )));
        }
        Err(other) => return Err(AppError::Database(other)),
    };

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    get_feed_by_id(pool, id).await
}

pub async fn set_feed_enabled(pool: &SqlitePool, id: i64, enabled: bool) -> AppResult<Feed> {
    let enabled_int: i64 = if enabled { 1 } else { 0 };
    let mut tx = pool.begin().await?;
    let result = sqlx::query(
        "UPDATE feeds
         SET enabled = ?,
             next_scheduled_fetch_at = CASE
                 WHEN ? = 1 AND next_scheduled_fetch_at IS NULL
                 THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 ELSE next_scheduled_fetch_at
             END,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(enabled_int)
    .bind(enabled_int)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    get_feed_by_id(pool, id).await
}

/// Safely delete a feed per Revision 2 §5.1 (fixes QUA-006 and QUA-007):
/// 1. `DELETE FROM feeds WHERE id = ?` (cascades to `article_feed_sources` and
///    `feed_fetch_runs`, and sets `articles.feed_id = NULL` via `ON DELETE SET NULL`).
/// 2. Repoint affected articles (`feed_id IS NULL`) to their earliest surviving
///    source in `article_feed_sources`, if one exists.
/// 3. Delete orphaned articles (`feed_id IS NULL` and no remaining `article_feed_sources`)
///    ONLY when they are not referenced by any edition (`edition_articles`).
///    Articles in editions survive with `feed_id = NULL`.
pub async fn delete_feed(pool: &SqlitePool, id: i64) -> AppResult<()> {
    with_write_retry(pool, |tx| {
        Box::pin(async move {
            let result = sqlx::query("DELETE FROM feeds WHERE id = ?")
                .bind(id)
                .execute(&mut **tx)
                .await?;

            if result.rows_affected() == 0 {
                return Err(AppError::NotFound(format!(
                    "Feed with ID {id} was not found."
                )));
            }

            sqlx::query(
                "UPDATE articles
                 SET feed_id = (
                     SELECT s.feed_id
                     FROM article_feed_sources s
                     WHERE s.article_id = articles.id
                     ORDER BY s.discovered_at ASC, s.feed_id ASC
                     LIMIT 1
                 )
                 WHERE feed_id IS NULL",
            )
            .execute(&mut **tx)
            .await?;

            sqlx::query(
                "DELETE FROM articles
                 WHERE feed_id IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM article_feed_sources s
                       WHERE s.article_id = articles.id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM edition_articles e
                       WHERE e.article_id = articles.id
                   )",
            )
            .execute(&mut **tx)
            .await?;

            Ok(())
        })
    })
    .await
}

/// Durable manual fetch request for a single feed (Revision 2 §2.2).
/// Marks the feed due (`next_scheduled_fetch_at = now`, `manual_requested_at = now`)
/// without spawning a detached task. Returns `Ok(true)` if marked due, or `Ok(false)`
/// if the feed is already running (`fetch_in_progress = 1`).
pub async fn mark_feed_manual_fetch_due(pool: &SqlitePool, feed_id: i64) -> AppResult<bool> {
    let feed = get_feed_by_id(pool, feed_id).await?;
    if feed.fetch_in_progress {
        return Ok(false);
    }

    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut tx = pool.begin().await?;
    let res = sqlx::query(
        "UPDATE feeds
         SET next_scheduled_fetch_at = ?,
             manual_requested_at = ?,
             last_fetch_status = CASE
                 WHEN fetch_in_progress = 0 THEN 'queued'
                 ELSE last_fetch_status
             END,
             updated_at = ?
         WHERE id = ? AND fetch_in_progress = 0",
    )
    .bind(&now_str)
    .bind(&now_str)
    .bind(&now_str)
    .bind(feed_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(res.rows_affected() > 0)
}

/// Durable manual fetch-all request for all enabled feeds that are not currently
/// running (Revision 2 §2.2).
pub async fn mark_all_enabled_feeds_manual_fetch_due(pool: &SqlitePool) -> AppResult<usize> {
    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut tx = pool.begin().await?;
    let res = sqlx::query(
        "UPDATE feeds
         SET next_scheduled_fetch_at = ?,
             manual_requested_at = ?,
             last_fetch_status = 'queued',
             updated_at = ?
         WHERE enabled = 1 AND fetch_in_progress = 0",
    )
    .bind(&now_str)
    .bind(&now_str)
    .bind(&now_str)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(res.rows_affected() as usize)
}

/// Atomically claim the next eligible feed for execution inside a managed
/// `BEGIN IMMEDIATE` transaction (`db::with_write_retry`), setting `current_run_id`
/// and `trigger_type = 'manual'` when `manual_requested_at IS NOT NULL` (or `'scheduled'`
/// otherwise) and clearing `manual_requested_at` (Revision 2 §2.2 & §2.4).
pub async fn claim_next_due_feed_fetch(
    pool: &SqlitePool,
    fetch_enabled: bool,
) -> AppResult<Option<(Feed, i64)>> {
    let claimed_pair: Option<(i64, i64)> = with_write_retry(pool, |tx| {
        Box::pin(async move {
            let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

            let candidate: Option<(i64, Option<String>)> = if fetch_enabled {
                sqlx::query_as(
                    "SELECT id, manual_requested_at
                     FROM feeds
                     WHERE fetch_in_progress = 0
                       AND (
                           manual_requested_at IS NOT NULL
                           OR (
                               enabled = 1
                               AND (next_scheduled_fetch_at IS NULL OR next_scheduled_fetch_at <= ?)
                           )
                       )
                     ORDER BY
                       CASE WHEN manual_requested_at IS NOT NULL THEN 0 ELSE 1 END ASC,
                       COALESCE(next_scheduled_fetch_at, '1970-01-01T00:00:00Z') ASC,
                       id ASC
                     LIMIT 1",
                )
                .bind(&now_str)
                .fetch_optional(&mut **tx)
                .await?
            } else {
                // When global automatic fetching is disabled (`fetch_enabled = false`),
                // manual requests still run because the user asked explicitly (§2.2).
                sqlx::query_as(
                    "SELECT id, manual_requested_at
                     FROM feeds
                     WHERE fetch_in_progress = 0
                       AND manual_requested_at IS NOT NULL
                     ORDER BY manual_requested_at ASC, id ASC
                     LIMIT 1",
                )
                .fetch_optional(&mut **tx)
                .await?
            };

            let Some((feed_id, manual_req)) = candidate else {
                return Ok(None);
            };

            let trigger_type = if manual_req.is_some() {
                "manual"
            } else {
                "scheduled"
            };

            let run_res = sqlx::query(
                "INSERT INTO feed_fetch_runs (feed_id, trigger_type, status)
                 VALUES (?, ?, 'running')",
            )
            .bind(feed_id)
            .bind(trigger_type)
            .execute(&mut **tx)
            .await?;
            let run_id = run_res.last_insert_rowid();

            let update_res = sqlx::query(
                "UPDATE feeds
                 SET fetch_in_progress = 1,
                     current_run_id = ?,
                     manual_requested_at = NULL,
                     last_fetch_status = 'running',
                     last_attempted_fetch_at = ?,
                     updated_at = ?
                 WHERE id = ? AND fetch_in_progress = 0",
            )
            .bind(run_id)
            .bind(&now_str)
            .bind(&now_str)
            .bind(feed_id)
            .execute(&mut **tx)
            .await?;

            if update_res.rows_affected() == 0 {
                return Err(AppError::Conflict(
                    "Feed claim race detected; rolling back run row.".to_string(),
                ));
            }

            Ok(Some((feed_id, run_id)))
        })
    })
    .await?;

    let Some((feed_id, run_id)) = claimed_pair else {
        return Ok(None);
    };

    let feed = get_feed_by_id(pool, feed_id).await?;
    Ok(Some((feed, run_id)))
}

/// Atomically claim a specific `feed_id` for execution inside a managed
/// `BEGIN IMMEDIATE` transaction (`db::with_write_retry`), setting `current_run_id`.
/// Returns `Ok(None)` if the feed is already in progress.
pub async fn try_claim_feed_fetch(
    pool: &SqlitePool,
    feed_id: i64,
    trigger_type: &str,
) -> AppResult<Option<(Feed, i64)>> {
    let trig_owned = trigger_type.to_string();
    let run_id_opt: Option<i64> = with_write_retry(pool, |tx| {
        let trig = trig_owned.clone();
        Box::pin(async move {
            let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

            let in_prog: Option<(i64, Option<String>)> = sqlx::query_as(
                "SELECT fetch_in_progress, manual_requested_at FROM feeds WHERE id = ?",
            )
            .bind(feed_id)
            .fetch_optional(&mut **tx)
            .await?;

            let Some((prog, manual_req)) = in_prog else {
                return Err(AppError::NotFound(format!(
                    "Feed with ID {feed_id} was not found."
                )));
            };
            if prog != 0 {
                return Ok(None);
            }

            let effective_trigger = if manual_req.is_some() {
                "manual"
            } else {
                trig.as_str()
            };

            let run_res = sqlx::query(
                "INSERT INTO feed_fetch_runs (feed_id, trigger_type, status)
                 VALUES (?, ?, 'running')",
            )
            .bind(feed_id)
            .bind(effective_trigger)
            .execute(&mut **tx)
            .await?;
            let run_id = run_res.last_insert_rowid();

            let update_res = sqlx::query(
                "UPDATE feeds
                 SET fetch_in_progress = 1,
                     current_run_id = ?,
                     manual_requested_at = NULL,
                     last_fetch_status = 'running',
                     last_attempted_fetch_at = ?,
                     updated_at = ?
                 WHERE id = ? AND fetch_in_progress = 0",
            )
            .bind(run_id)
            .bind(&now_str)
            .bind(&now_str)
            .bind(feed_id)
            .execute(&mut **tx)
            .await?;

            if update_res.rows_affected() == 0 {
                return Ok(None);
            }

            Ok(Some(run_id))
        })
    })
    .await?;

    let Some(run_id) = run_id_opt else {
        return Ok(None);
    };

    let feed = get_feed_by_id(pool, feed_id).await?;
    Ok(Some((feed, run_id)))
}

#[derive(Debug, Clone)]
pub struct CompleteFeedRunUpdate {
    pub feed_id: i64,
    pub run_id: i64,
    pub status: &'static str,
    pub http_status: Option<u16>,
    pub duration_ms: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub metadata: Option<ParsedFeedMetadata>,
    pub discovered_entries: Vec<NormalizedFeedEntry>,
    pub max_extraction_attempts: u32,
    pub entries_seen: usize,
    pub entries_skipped: usize,
    pub entries_truncated: usize,
    pub articles_inserted: usize,
    pub articles_updated: usize,
    pub articles_unchanged: usize,
    pub extraction_queued: usize,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub retry_after: Option<Duration>,
    pub default_interval_minutes: u32,
    pub retention_limit: i64,
}

/// Complete a feed fetch run inside ONE managed `BEGIN IMMEDIATE` transaction
/// (`db::with_write_retry`) that:
/// 1. Verifies ownership (`WHERE id = :feed_id AND current_run_id = :run_id AND fetch_in_progress = 1`).
///    If 0 rows match, rolls back and discards the stale result without touching
///    articles, validators, or scheduling state (Revision 2 §2.4 / TRP-003).
/// 2. Persists all `discovered_entries` and enqueues extraction jobs in the same transaction.
/// 3. Updates `feeds` (validators, counters, schedule, `fetch_in_progress = 0`, `current_run_id = NULL`).
/// 4. Completes the `feed_fetch_runs` row and prunes history.
pub async fn complete_feed_fetch_run(
    pool: &SqlitePool,
    update: CompleteFeedRunUpdate,
) -> AppResult<bool> {
    with_write_retry(pool, |tx| {
        let upd = update.clone();
        Box::pin(async move {
            let now = Utc::now();
            let now_str = now.to_rfc3339_opts(SecondsFormat::Secs, true);

            // Read current feed state inside the immediate transaction and verify ownership
            let sql = format!(
                "{FEED_SELECT_SQL} WHERE f.id = ? AND f.current_run_id = ? AND f.fetch_in_progress = 1"
            );
            let Some(feed) = sqlx::query_as::<_, Feed>(&sql)
                .bind(upd.feed_id)
                .bind(upd.run_id)
                .fetch_optional(&mut **tx)
                .await?
            else {
                warn!(
                    feed_id = upd.feed_id,
                    run_id = upd.run_id,
                    "Discarding stale feed completion: current_run_id ownership check failed"
                );
                return Ok(false);
            };

            // Step 2: Persist discovered entries and enqueue extraction jobs atomically
            let (inserted, updated, unchanged, queued) = if !upd.discovered_entries.is_empty() {
                let stats = persist_discovered_entries_in_tx(
                    tx,
                    upd.feed_id,
                    &upd.discovered_entries,
                    upd.max_extraction_attempts,
                )
                .await?;
                (
                    stats.inserted,
                    stats.updated,
                    stats.unchanged,
                    stats.extraction_queued,
                )
            } else {
                (
                    upd.articles_inserted,
                    upd.articles_updated,
                    upd.articles_unchanged,
                    upd.extraction_queued,
                )
            };

            let interval_mins = feed
                .fetch_interval_minutes
                .unwrap_or(i64::from(upd.default_interval_minutes))
                .max(5);

            let is_success = matches!(upd.status, "success" | "unchanged" | "partial");
            let consecutive_failures = if is_success {
                0
            } else {
                feed.consecutive_failures.saturating_add(1)
            };

            let next_delay_secs = if consecutive_failures == 0 {
                interval_mins * 60
            } else {
                let factor = 2i64.saturating_pow((consecutive_failures as u32).min(4));
                let exp_secs = (interval_mins.saturating_mul(factor)).min(10_080) * 60;
                let ra_secs = upd
                    .retry_after
                    .map(|d| (d.as_secs() as i64).min(24 * 3600))
                    .unwrap_or(0);
                exp_secs.max(ra_secs)
            };
            let next_scheduled_str = (now + ChronoDuration::seconds(next_delay_secs))
                .to_rfc3339_opts(SecondsFormat::Secs, true);

            let new_website_url = feed.website_url.or_else(|| {
                upd.metadata
                    .as_ref()
                    .and_then(|m| m.website_url.clone())
            });
            let new_description = feed.description.or_else(|| {
                upd.metadata
                    .as_ref()
                    .and_then(|m| m.description.clone())
            });
            let new_language = upd
                .metadata
                .as_ref()
                .and_then(|m| m.language.clone())
                .or(feed.feed_language);
            let new_feed_updated = upd
                .metadata
                .as_ref()
                .and_then(|m| m.updated_at.clone())
                .or(feed.feed_updated_at);

            let new_last_success = if is_success {
                Some(now_str.clone())
            } else {
                feed.last_successful_fetch_at
            };

            let new_etag = if is_success {
                upd.etag.or(feed.etag)
            } else {
                feed.etag
            };
            let new_last_modified = if is_success {
                upd.last_modified.or(feed.last_modified)
            } else {
                feed.last_modified
            };

            let feed_upd_res = sqlx::query(
                "UPDATE feeds
                 SET website_url = ?,
                     description = ?,
                     feed_language = ?,
                     feed_updated_at = ?,
                     etag = ?,
                     last_modified = ?,
                     last_successful_fetch_at = ?,
                     last_fetch_status = ?,
                     last_http_status = ?,
                     last_fetch_duration_ms = ?,
                     last_discovered_count = ?,
                     last_inserted_count = ?,
                     last_extraction_count = ?,
                     consecutive_failures = ?,
                     last_error = ?,
                     next_scheduled_fetch_at = ?,
                     fetch_in_progress = 0,
                     current_run_id = NULL,
                     updated_at = ?
                 WHERE id = ? AND current_run_id = ? AND fetch_in_progress = 1",
            )
            .bind(new_website_url)
            .bind(new_description)
            .bind(new_language)
            .bind(new_feed_updated)
            .bind(new_etag)
            .bind(new_last_modified)
            .bind(new_last_success)
            .bind(upd.status)
            .bind(upd.http_status.map(i64::from))
            .bind(upd.duration_ms as i64)
            .bind(upd.entries_seen as i64)
            .bind(inserted as i64)
            .bind(queued as i64)
            .bind(consecutive_failures)
            .bind(&upd.error_message)
            .bind(&next_scheduled_str)
            .bind(&now_str)
            .bind(upd.feed_id)
            .bind(upd.run_id)
            .execute(&mut **tx)
            .await?;

            if feed_upd_res.rows_affected() == 0 {
                return Err(AppError::Conflict(
                    "Feed ownership changed during completion.".to_string(),
                ));
            }

            sqlx::query(
                "UPDATE feed_fetch_runs
                 SET status = ?,
                     completed_at = ?,
                     duration_ms = ?,
                     http_status = ?,
                     entries_seen = ?,
                     entries_skipped = ?,
                     entries_truncated = ?,
                     articles_inserted = ?,
                     articles_updated = ?,
                     articles_unchanged = ?,
                     extraction_queued = ?,
                     error_category = ?,
                     error_message = ?
                 WHERE id = ? AND status = 'running'",
            )
            .bind(upd.status)
            .bind(&now_str)
            .bind(upd.duration_ms as i64)
            .bind(upd.http_status.map(i64::from))
            .bind(upd.entries_seen as i64)
            .bind(upd.entries_skipped as i64)
            .bind(upd.entries_truncated as i64)
            .bind(inserted as i64)
            .bind(updated as i64)
            .bind(unchanged as i64)
            .bind(queued as i64)
            .bind(&upd.error_category)
            .bind(&upd.error_message)
            .bind(upd.run_id)
            .execute(&mut **tx)
            .await?;

            sqlx::query(
                "DELETE FROM feed_fetch_runs
                 WHERE feed_id = ?
                   AND id NOT IN (
                       SELECT id FROM feed_fetch_runs
                       WHERE feed_id = ?
                       ORDER BY started_at DESC, id DESC
                       LIMIT ?
                   )",
            )
            .bind(upd.feed_id)
            .bind(upd.feed_id)
            .bind(upd.retention_limit.max(10))
            .execute(&mut **tx)
            .await?;

            Ok(true)
        })
    })
    .await
}

/// Conditionally release an in-flight feed fetch when a worker panics, hits its
/// hard deadline, or is cancelled during graceful shutdown (Revision 2 §2.4 & §2.5).
pub async fn release_feed_fetch_run(
    pool: &SqlitePool,
    feed_id: i64,
    run_id: i64,
    run_status: &'static str,
    error_category: &'static str,
    error_message: &str,
    default_interval_minutes: u32,
) -> AppResult<bool> {
    let err_msg_owned = error_message.to_string();

    with_write_retry(pool, |tx| {
        let err_msg = err_msg_owned.clone();
        Box::pin(async move {
            let now = Utc::now();
            let now_str = now.to_rfc3339_opts(SecondsFormat::Secs, true);

            let sql = format!(
                "{FEED_SELECT_SQL} WHERE f.id = ? AND f.current_run_id = ? AND f.fetch_in_progress = 1"
            );
            let Some(feed) = sqlx::query_as::<_, Feed>(&sql)
                .bind(feed_id)
                .bind(run_id)
                .fetch_optional(&mut **tx)
                .await?
            else {
                return Ok(false);
            };

            let is_cancelled = run_status == "cancelled";
            let consecutive_failures = if is_cancelled {
                feed.consecutive_failures
            } else {
                feed.consecutive_failures.saturating_add(1)
            };

            let interval_mins = feed
                .fetch_interval_minutes
                .unwrap_or(i64::from(default_interval_minutes))
                .max(5);

            let next_scheduled_str = if is_cancelled {
                now_str.clone()
            } else {
                let factor = 2i64.saturating_pow((consecutive_failures as u32).min(4));
                let delay_mins = (interval_mins.saturating_mul(factor)).min(10_080);
                (now + ChronoDuration::minutes(delay_mins))
                    .to_rfc3339_opts(SecondsFormat::Secs, true)
            };

            let res = sqlx::query(
                "UPDATE feeds
                 SET fetch_in_progress = 0,
                     current_run_id = NULL,
                     last_fetch_status = ?,
                     consecutive_failures = ?,
                     last_error = CASE WHEN ? THEN last_error ELSE ? END,
                     next_scheduled_fetch_at = ?,
                     updated_at = ?
                 WHERE id = ? AND current_run_id = ? AND fetch_in_progress = 1",
            )
            .bind(run_status)
            .bind(consecutive_failures)
            .bind(is_cancelled)
            .bind(&err_msg)
            .bind(&next_scheduled_str)
            .bind(&now_str)
            .bind(feed_id)
            .bind(run_id)
            .execute(&mut **tx)
            .await?;

            if res.rows_affected() == 0 {
                return Ok(false);
            }

            sqlx::query(
                "UPDATE feed_fetch_runs
                 SET status = ?,
                     completed_at = ?,
                     error_category = ?,
                     error_message = ?
                 WHERE id = ? AND status = 'running'",
            )
            .bind(run_status)
            .bind(&now_str)
            .bind(error_category)
            .bind(&err_msg)
            .bind(run_id)
            .execute(&mut **tx)
            .await?;

            Ok(true)
        })
    })
    .await
}

/// Recover interrupted feed fetches left in `fetch_in_progress = 1` or
/// `feed_fetch_runs.status = 'running'` after a crash or container restart.
pub async fn recover_interrupted_fetches(pool: &SqlitePool) -> AppResult<u64> {
    with_write_retry(pool, |tx| {
        Box::pin(async move {
            let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

            let res = sqlx::query(
                "UPDATE feeds
                 SET fetch_in_progress = 0,
                     current_run_id = NULL,
                     last_fetch_status = CASE
                         WHEN last_fetch_status = 'running' THEN 'cancelled'
                         ELSE last_fetch_status
                     END,
                     next_scheduled_fetch_at = CASE
                         WHEN manual_requested_at IS NOT NULL THEN ?
                         ELSE COALESCE(next_scheduled_fetch_at, ?)
                     END,
                     updated_at = ?
                 WHERE fetch_in_progress = 1 OR current_run_id IS NOT NULL",
            )
            .bind(&now_str)
            .bind(&now_str)
            .bind(&now_str)
            .execute(&mut **tx)
            .await?;

            sqlx::query(
                "UPDATE feed_fetch_runs
                 SET status = 'cancelled',
                     completed_at = ?,
                     error_category = 'cancelled',
                     error_message = 'Interrupted by application restart or shutdown.'
                 WHERE status = 'running'",
            )
            .bind(&now_str)
            .bind(&now_str)
            .execute(&mut **tx)
            .await?;

            Ok(res.rows_affected())
        })
    })
    .await
}

/// Return IDs of enabled feeds (or feeds with a pending manual request) that are
/// due for polling.
pub async fn list_due_feed_ids(pool: &SqlitePool, limit: i64) -> AppResult<Vec<i64>> {
    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT id
         FROM feeds
         WHERE fetch_in_progress = 0
           AND (
               manual_requested_at IS NOT NULL
               OR (
                   enabled = 1
                   AND (next_scheduled_fetch_at IS NULL OR next_scheduled_fetch_at <= ?)
               )
           )
         ORDER BY
           CASE WHEN manual_requested_at IS NOT NULL THEN 0 ELSE 1 END ASC,
           COALESCE(next_scheduled_fetch_at, '1970-01-01T00:00:00Z') ASC,
           id ASC
         LIMIT ?",
    )
    .bind(&now_str)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

pub async fn list_feed_fetch_runs(
    pool: &SqlitePool,
    feed_id: i64,
    limit: i64,
) -> AppResult<Vec<FeedFetchRun>> {
    let runs = sqlx::query_as::<_, FeedFetchRun>(
        "SELECT
            id,
            feed_id,
            trigger_type,
            status,
            started_at,
            completed_at,
            duration_ms,
            http_status,
            entries_seen,
            entries_skipped,
            entries_truncated,
            articles_inserted,
            articles_updated,
            articles_unchanged,
            extraction_queued,
            error_category,
            error_message
         FROM feed_fetch_runs
         WHERE feed_id = ?
         ORDER BY started_at DESC, id DESC
         LIMIT ?",
    )
    .bind(feed_id)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(runs)
}
