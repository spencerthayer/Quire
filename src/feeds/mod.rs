pub mod fetcher;
pub mod parser;
pub mod scheduler;
pub mod validation;

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};

use crate::config::{DEFAULT_FETCH_MAX_INTERVAL_MINUTES, DEFAULT_FETCH_MIN_INTERVAL_MINUTES};
use crate::error::{AppError, AppResult};
use crate::feeds::parser::ParsedFeedMetadata;
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
             next_scheduled_fetch_at = CASE
                 WHEN ? = 1 AND next_scheduled_fetch_at IS NULL
                 THEN strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 ELSE next_scheduled_fetch_at
             END,
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
    .bind(enabled_int)
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

/// Safely delete a feed.
/// If any article primarily owned by this feed (`articles.feed_id = id`) is also
/// referenced by another feed in `article_feed_sources`, reassign `articles.feed_id`
/// to the earliest surviving feed so shared cross-feed articles are not deleted.
pub async fn delete_feed(pool: &SqlitePool, id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;

    sqlx::query(
        "UPDATE articles
         SET feed_id = (
             SELECT afs.feed_id
             FROM article_feed_sources afs
             WHERE afs.article_id = articles.id
               AND afs.feed_id != ?
             ORDER BY afs.discovered_at ASC, afs.feed_id ASC
             LIMIT 1
         ),
         external_guid = (
             SELECT afs.external_guid
             FROM article_feed_sources afs
             WHERE afs.article_id = articles.id
               AND afs.feed_id != ?
             ORDER BY afs.discovered_at ASC, afs.feed_id ASC
             LIMIT 1
         )
         WHERE feed_id = ?
           AND EXISTS (
               SELECT 1
               FROM article_feed_sources afs
               WHERE afs.article_id = articles.id
                 AND afs.feed_id != ?
           )",
    )
    .bind(id)
    .bind(id)
    .bind(id)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    let result = sqlx::query("DELETE FROM feeds WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    Ok(())
}

/// Atomically claim a feed for execution and create a `feed_fetch_runs` row
/// in `running` status. Returns `Ok(None)` if the feed is already in progress.
pub async fn try_claim_feed_fetch(
    pool: &SqlitePool,
    feed_id: i64,
    trigger_type: &str,
) -> AppResult<Option<(Feed, i64)>> {
    let mut tx = pool.begin().await?;

    let update_res = sqlx::query(
        "UPDATE feeds
         SET fetch_in_progress = 1,
             last_fetch_status = 'running',
             last_attempted_fetch_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ? AND fetch_in_progress = 0",
    )
    .bind(feed_id)
    .execute(&mut *tx)
    .await?;

    if update_res.rows_affected() == 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let run_res = sqlx::query(
        "INSERT INTO feed_fetch_runs (feed_id, trigger_type, status)
         VALUES (?, ?, 'running')",
    )
    .bind(feed_id)
    .bind(trigger_type)
    .execute(&mut *tx)
    .await?;

    let run_id = run_res.last_insert_rowid();
    tx.commit().await?;

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
    pub entries_seen: usize,
    pub entries_skipped: usize,
    pub entries_truncated: usize,
    pub articles_inserted: usize,
    pub articles_updated: usize,
    pub articles_unchanged: usize,
    pub extraction_queued: usize,
    pub error_category: Option<String>,
    pub error_message: Option<String>,
    pub default_interval_minutes: u32,
    pub retention_limit: i64,
}

pub async fn complete_feed_fetch_run(
    pool: &SqlitePool,
    update: CompleteFeedRunUpdate,
) -> AppResult<()> {
    let now = Utc::now();
    let now_str = now.to_rfc3339_opts(SecondsFormat::Secs, true);

    let feed = get_feed_by_id(pool, update.feed_id).await?;
    let interval_mins = feed
        .fetch_interval_minutes
        .unwrap_or(i64::from(update.default_interval_minutes))
        .max(5);

    let is_success = matches!(update.status, "success" | "unchanged" | "partial");
    let consecutive_failures = if is_success {
        0
    } else {
        feed.consecutive_failures.saturating_add(1)
    };

    // Apply exponential backoff to next_scheduled_fetch_at when consecutive failures occur
    let next_delay_mins = if consecutive_failures == 0 {
        interval_mins
    } else {
        let factor = 2i64.saturating_pow((consecutive_failures as u32).min(4));
        (interval_mins.saturating_mul(factor)).min(10_080)
    };
    let next_scheduled_str =
        (now + ChronoDuration::minutes(next_delay_mins)).to_rfc3339_opts(SecondsFormat::Secs, true);

    let mut tx = pool.begin().await?;

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
         WHERE id = ?",
    )
    .bind(update.status)
    .bind(&now_str)
    .bind(update.duration_ms as i64)
    .bind(update.http_status.map(i64::from))
    .bind(update.entries_seen as i64)
    .bind(update.entries_skipped as i64)
    .bind(update.entries_truncated as i64)
    .bind(update.articles_inserted as i64)
    .bind(update.articles_updated as i64)
    .bind(update.articles_unchanged as i64)
    .bind(update.extraction_queued as i64)
    .bind(&update.error_category)
    .bind(&update.error_message)
    .bind(update.run_id)
    .execute(&mut *tx)
    .await?;

    let new_website_url = feed
        .website_url
        .or_else(|| update.metadata.as_ref().and_then(|m| m.website_url.clone()));
    let new_description = feed
        .description
        .or_else(|| update.metadata.as_ref().and_then(|m| m.description.clone()));
    let new_language = update
        .metadata
        .as_ref()
        .and_then(|m| m.language.clone())
        .or(feed.feed_language);
    let new_feed_updated = update
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
        update.etag.or(feed.etag)
    } else {
        feed.etag
    };
    let new_last_modified = if is_success {
        update.last_modified.or(feed.last_modified)
    } else {
        feed.last_modified
    };

    sqlx::query(
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
             updated_at = ?
         WHERE id = ?",
    )
    .bind(new_website_url)
    .bind(new_description)
    .bind(new_language)
    .bind(new_feed_updated)
    .bind(new_etag)
    .bind(new_last_modified)
    .bind(new_last_success)
    .bind(update.status)
    .bind(update.http_status.map(i64::from))
    .bind(update.duration_ms as i64)
    .bind(update.entries_seen as i64)
    .bind(update.articles_inserted as i64)
    .bind(update.extraction_queued as i64)
    .bind(consecutive_failures)
    .bind(&update.error_message)
    .bind(&next_scheduled_str)
    .bind(&now_str)
    .bind(update.feed_id)
    .execute(&mut *tx)
    .await?;

    // Prune older fetch history beyond `retention_limit` for this feed
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
    .bind(update.feed_id)
    .bind(update.feed_id)
    .bind(update.retention_limit.max(10))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Recover interrupted feed fetches left in `fetch_in_progress = 1` or
/// `feed_fetch_runs.status = 'running'` after a crash or container restart.
pub async fn recover_interrupted_fetches(pool: &SqlitePool) -> AppResult<u64> {
    let mut tx = pool.begin().await?;
    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let res = sqlx::query(
        "UPDATE feeds
         SET fetch_in_progress = 0,
             last_fetch_status = CASE
                 WHEN last_fetch_status IN ('running', 'queued') THEN 'cancelled'
                 ELSE last_fetch_status
             END,
             next_scheduled_fetch_at = COALESCE(next_scheduled_fetch_at, ?),
             updated_at = ?
         WHERE fetch_in_progress = 1 OR last_fetch_status IN ('running', 'queued')",
    )
    .bind(&now_str)
    .bind(&now_str)
    .execute(&mut *tx)
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
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(res.rows_affected())
}

/// Return IDs of enabled feeds that are due for scheduled polling.
pub async fn list_due_feed_ids(pool: &SqlitePool, limit: i64) -> AppResult<Vec<i64>> {
    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    let rows: Vec<(i64,)> = sqlx::query_as(
        "SELECT id
         FROM feeds
         WHERE enabled = 1
           AND fetch_in_progress = 0
           AND (next_scheduled_fetch_at IS NULL OR next_scheduled_fetch_at <= ?)
         ORDER BY COALESCE(next_scheduled_fetch_at, '1970-01-01T00:00:00Z') ASC, id ASC
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
