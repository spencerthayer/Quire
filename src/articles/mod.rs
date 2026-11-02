pub mod extractor;
pub mod normalizer;
pub mod sanitizer;

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};

use crate::error::{AppError, AppResult};
use crate::feeds::parser::NormalizedFeedEntry;

/// Computes the normalized logical extraction state (`completed`, `fallback`,
/// `processing`, `pending`, or `failed`) from `articles` and `article_extraction_jobs`
/// while preserving SQLite's Phase 1 `CHECK (extraction_status IN ('pending', 'extracted', 'failed', 'skipped'))`
/// constraint on the underlying `articles.extraction_status` column.
const LOGICAL_STATUS_SQL: &str = "
    CASE
        WHEN EXISTS (
            SELECT 1 FROM article_extraction_jobs j
            WHERE j.article_id = a.id AND j.status = 'processing'
        ) THEN 'processing'
        WHEN a.extraction_status = 'extracted'
             AND COALESCE(a.extracted_content_type, 'html') = 'summary_fallback'
             THEN 'fallback'
        WHEN a.extraction_status = 'skipped' THEN 'fallback'
        WHEN a.extraction_status = 'extracted' THEN 'completed'
        ELSE a.extraction_status
    END
";

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleSummaryRow {
    pub id: i64,
    pub feed_id: i64,
    pub feed_name: String,
    pub external_guid: Option<String>,
    pub canonical_url: String,
    pub title: String,
    pub author: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<String>,
    pub discovered_at: String,
    pub extraction_status: String,
    pub has_extracted_content: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleDetail {
    pub id: i64,
    pub feed_id: i64,
    pub feed_name: String,
    pub external_guid: Option<String>,
    pub canonical_url: String,
    pub original_source_url: Option<String>,
    pub title: String,
    pub author: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<String>,
    pub entry_updated_at: Option<String>,
    pub discovered_at: String,
    pub extracted_content: Option<String>,
    pub extraction_status: String,
    pub content_hash: Option<String>,
    pub extraction_attempts: i64,
    pub last_extraction_attempt_at: Option<String>,
    pub last_successful_extraction_at: Option<String>,
    pub extraction_error: Option<String>,
    pub next_extraction_retry_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleFeedSourceRow {
    pub feed_id: i64,
    pub feed_name: String,
    pub external_guid: Option<String>,
    pub discovered_at: String,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Default)]
pub struct ArticleFilter {
    pub feed_id: Option<i64>,
    pub extraction_status: Option<String>,
    pub published_date: Option<String>,
    pub discovered_date: Option<String>,
    pub page: usize,
    pub page_size: usize,
}

#[derive(Debug, Clone)]
pub struct PaginatedArticles {
    pub items: Vec<ArticleSummaryRow>,
    pub total_count: i64,
    pub page: usize,
    pub page_size: usize,
    pub total_pages: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PersistBatchStats {
    pub inserted: usize,
    pub updated: usize,
    pub unchanged: usize,
    pub extraction_queued: usize,
}

#[derive(Debug, Clone, FromRow)]
pub struct ClaimedExtractionJob {
    pub job_id: i64,
    pub article_id: i64,
    pub canonical_url: String,
    pub description: Option<String>,
    pub attempts: i64,
    pub max_attempts: i64,
    pub trigger_type: String,
}

#[derive(Debug, FromRow)]
struct ExistingArticleRow {
    id: i64,
    feed_id: i64,
    external_guid: Option<String>,
    title: String,
    author: Option<String>,
    description: Option<String>,
    published_at: Option<String>,
    entry_updated_at: Option<String>,
    extraction_status: String,
}

/// Persist a batch of normalized feed entries transactionally, enforcing
/// deduplication across feeds and enqueuing persistent SQLite extraction jobs
/// inside the same transaction.
pub async fn persist_discovered_entries(
    pool: &SqlitePool,
    feed_id: i64,
    entries: &[NormalizedFeedEntry],
    max_attempts: u32,
) -> AppResult<PersistBatchStats> {
    let mut stats = PersistBatchStats::default();
    if entries.is_empty() {
        return Ok(stats);
    }

    let mut tx = pool.begin().await?;
    for entry in entries {
        let outcome = upsert_single_entry(&mut tx, feed_id, entry, i64::from(max_attempts)).await?;
        match outcome {
            EntryUpsertOutcome::Inserted { queued } => {
                stats.inserted += 1;
                if queued {
                    stats.extraction_queued += 1;
                }
            }
            EntryUpsertOutcome::Updated { queued } => {
                stats.updated += 1;
                if queued {
                    stats.extraction_queued += 1;
                }
            }
            EntryUpsertOutcome::Unchanged => {
                stats.unchanged += 1;
            }
        }
    }
    tx.commit().await?;
    Ok(stats)
}

enum EntryUpsertOutcome {
    Inserted { queued: bool },
    Updated { queued: bool },
    Unchanged,
}

async fn upsert_single_entry(
    tx: &mut Transaction<'_, Sqlite>,
    feed_id: i64,
    entry: &NormalizedFeedEntry,
    max_attempts: i64,
) -> AppResult<EntryUpsertOutcome> {
    let existing = sqlx::query_as::<_, ExistingArticleRow>(
        "SELECT
            id,
            feed_id,
            external_guid,
            title,
            author,
            description,
            published_at,
            entry_updated_at,
            extraction_status
         FROM articles
         WHERE canonical_url = ?",
    )
    .bind(&entry.canonical_url)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some(existing_row) = existing {
        if let Some(guid) = entry.external_guid.as_deref().filter(|g| !g.is_empty()) {
            sqlx::query(
                "UPDATE article_feed_sources
                 SET external_guid = NULL
                 WHERE feed_id = ? AND external_guid = ? AND article_id != ?",
            )
            .bind(feed_id)
            .bind(guid)
            .bind(existing_row.id)
            .execute(&mut **tx)
            .await?;

            if existing_row.feed_id == feed_id {
                sqlx::query(
                    "UPDATE articles
                     SET external_guid = NULL
                     WHERE feed_id = ? AND external_guid = ? AND id != ?",
                )
                .bind(feed_id)
                .bind(guid)
                .bind(existing_row.id)
                .execute(&mut **tx)
                .await?;
            }
        }

        sqlx::query(
            "INSERT INTO article_feed_sources (article_id, feed_id, external_guid, discovered_at, last_seen_at)
             VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
             ON CONFLICT(article_id, feed_id) DO UPDATE SET
                 external_guid = COALESCE(excluded.external_guid, article_feed_sources.external_guid),
                 last_seen_at = excluded.last_seen_at",
        )
        .bind(existing_row.id)
        .bind(feed_id)
        .bind(&entry.external_guid)
        .execute(&mut **tx)
        .await?;

        let entry_updated_changed = entry.entry_updated_at.is_some()
            && entry.entry_updated_at != existing_row.entry_updated_at;
        let title_changed = !entry.title.is_empty() && entry.title != existing_row.title;
        let summary_changed = entry.summary.is_some() && entry.summary != existing_row.description;
        let guid_changed = existing_row.feed_id == feed_id
            && entry.external_guid.is_some()
            && entry.external_guid != existing_row.external_guid;

        if entry_updated_changed || title_changed || summary_changed || guid_changed {
            let new_guid = if existing_row.feed_id == feed_id {
                entry
                    .external_guid
                    .clone()
                    .or(existing_row.external_guid.clone())
            } else {
                existing_row.external_guid.clone()
            };
            let new_author = entry.author.clone().or(existing_row.author.clone());
            let new_desc = entry.summary.clone().or(existing_row.description.clone());
            let new_pub = entry
                .published_at
                .clone()
                .or(existing_row.published_at.clone());
            let new_entry_updated = entry
                .entry_updated_at
                .clone()
                .or(existing_row.entry_updated_at.clone());

            sqlx::query(
                "UPDATE articles
                 SET external_guid = ?,
                     title = ?,
                     author = ?,
                     description = ?,
                     published_at = ?,
                     entry_updated_at = ?,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE id = ?",
            )
            .bind(new_guid)
            .bind(&entry.title)
            .bind(new_author)
            .bind(new_desc)
            .bind(new_pub)
            .bind(new_entry_updated)
            .bind(existing_row.id)
            .execute(&mut **tx)
            .await?;

            let should_requeue = entry_updated_changed
                || matches!(
                    existing_row.extraction_status.as_str(),
                    "pending" | "failed"
                );
            let mut queued = false;
            if should_requeue {
                queued =
                    enqueue_extraction_job_in_tx(tx, existing_row.id, "auto", max_attempts, false)
                        .await?;
            }
            return Ok(EntryUpsertOutcome::Updated { queued });
        }

        return Ok(EntryUpsertOutcome::Unchanged);
    }

    if let Some(guid) = entry.external_guid.as_deref().filter(|g| !g.is_empty()) {
        sqlx::query(
            "UPDATE articles
             SET external_guid = NULL
             WHERE feed_id = ? AND external_guid = ?",
        )
        .bind(feed_id)
        .bind(guid)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            "UPDATE article_feed_sources
             SET external_guid = NULL
             WHERE feed_id = ? AND external_guid = ?",
        )
        .bind(feed_id)
        .bind(guid)
        .execute(&mut **tx)
        .await?;
    }

    let insert_res = sqlx::query(
        "INSERT INTO articles (
            feed_id,
            external_guid,
            canonical_url,
            original_source_url,
            title,
            author,
            description,
            published_at,
            entry_updated_at,
            extraction_status
         )
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
    )
    .bind(feed_id)
    .bind(&entry.external_guid)
    .bind(&entry.canonical_url)
    .bind(&entry.original_source_url)
    .bind(&entry.title)
    .bind(&entry.author)
    .bind(&entry.summary)
    .bind(&entry.published_at)
    .bind(&entry.entry_updated_at)
    .execute(&mut **tx)
    .await?;

    let article_id = insert_res.last_insert_rowid();

    sqlx::query(
        "INSERT INTO article_feed_sources (article_id, feed_id, external_guid)
         VALUES (?, ?, ?)",
    )
    .bind(article_id)
    .bind(feed_id)
    .bind(&entry.external_guid)
    .execute(&mut **tx)
    .await?;

    let queued = enqueue_extraction_job_in_tx(tx, article_id, "auto", max_attempts, true).await?;

    Ok(EntryUpsertOutcome::Inserted { queued })
}

/// Enqueue or reset an extraction job for `article_id` inside an active SQLite
/// transaction. Guarantees at most one job row per article (`UNIQUE(article_id)`).
pub async fn enqueue_extraction_job_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    article_id: i64,
    trigger_type: &str,
    max_attempts: i64,
    force_reset: bool,
) -> AppResult<bool> {
    let existing_status: Option<(String,)> =
        sqlx::query_as("SELECT status FROM article_extraction_jobs WHERE article_id = ?")
            .bind(article_id)
            .fetch_optional(&mut **tx)
            .await?;

    if let Some((status,)) = existing_status {
        if status == "processing" || (status == "queued" && !force_reset) {
            return Ok(false);
        }
        sqlx::query(
            "UPDATE article_extraction_jobs
             SET status = 'queued',
                 trigger_type = ?,
                 attempts = CASE WHEN ? THEN 0 ELSE attempts END,
                 max_attempts = ?,
                 next_eligible_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                 claimed_at = NULL,
                 completed_at = NULL,
                 updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
             WHERE article_id = ?",
        )
        .bind(trigger_type)
        .bind(force_reset)
        .bind(max_attempts)
        .bind(article_id)
        .execute(&mut **tx)
        .await?;
    } else {
        sqlx::query(
            "INSERT INTO article_extraction_jobs (
                article_id,
                status,
                trigger_type,
                attempts,
                max_attempts,
                next_eligible_at
             )
             VALUES (?, 'queued', ?, 0, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        )
        .bind(article_id)
        .bind(trigger_type)
        .bind(max_attempts)
        .execute(&mut **tx)
        .await?;
    }

    sqlx::query(
        "UPDATE articles
         SET extraction_status = 'pending',
             next_extraction_retry_at = NULL,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(article_id)
    .execute(&mut **tx)
    .await?;

    Ok(true)
}

/// Enqueue a manual retry or content refresh for a single article from the web UI.
pub async fn enqueue_manual_article_extraction(
    pool: &SqlitePool,
    article_id: i64,
    max_attempts: u32,
) -> AppResult<bool> {
    let exists: Option<(i64,)> = sqlx::query_as("SELECT id FROM articles WHERE id = ?")
        .bind(article_id)
        .fetch_optional(pool)
        .await?;
    if exists.is_none() {
        return Err(AppError::NotFound(format!(
            "Article with ID {article_id} was not found."
        )));
    }

    let mut tx = pool.begin().await?;
    let queued = enqueue_extraction_job_in_tx(
        &mut tx,
        article_id,
        "manual",
        i64::from(max_attempts.max(1)),
        true,
    )
    .await?;
    tx.commit().await?;
    Ok(queued)
}

/// Recover interrupted extraction jobs left in `processing` state after an
/// unexpected crash or container restart.
pub async fn recover_interrupted_extractions(pool: &SqlitePool) -> AppResult<u64> {
    let mut tx = pool.begin().await?;
    let res = sqlx::query(
        "UPDATE article_extraction_jobs
         SET status = 'queued',
             claimed_at = NULL,
             next_eligible_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE status = 'processing'",
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(res.rows_affected())
}

/// Atomically claim the next eligible extraction job from SQLite so multiple
/// workers cannot process the same job simultaneously.
/// Uses an immediate write transaction (`BEGIN IMMEDIATE`) so concurrent workers
/// serialize their claim without hitting `SQLITE_BUSY_SNAPSHOT`.
pub async fn claim_next_extraction_job(
    pool: &SqlitePool,
) -> AppResult<Option<ClaimedExtractionJob>> {
    let mut conn = pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;

    let claim_res: AppResult<Option<ClaimedExtractionJob>> = async {
        let candidate: Option<(i64, i64)> = sqlx::query_as(
            "SELECT id, article_id
             FROM article_extraction_jobs
             WHERE status = 'queued'
               AND next_eligible_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
             ORDER BY next_eligible_at ASC, id ASC
             LIMIT 1",
        )
        .fetch_optional(&mut *conn)
        .await?;

        let Some((job_id, article_id)) = candidate else {
            return Ok(None);
        };

        let update_res = sqlx::query(
            "UPDATE article_extraction_jobs
             SET status = 'processing',
                 attempts = attempts + 1,
                 claimed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
             WHERE id = ? AND status = 'queued'",
        )
        .bind(job_id)
        .execute(&mut *conn)
        .await?;

        if update_res.rows_affected() == 0 {
            return Ok(None);
        }

        sqlx::query(
            "UPDATE articles
             SET extraction_status = 'pending',
                 extraction_attempts = extraction_attempts + 1,
                 last_extraction_attempt_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
             WHERE id = ?",
        )
        .bind(article_id)
        .execute(&mut *conn)
        .await?;

        let claimed = sqlx::query_as::<_, ClaimedExtractionJob>(
            "SELECT
                j.id AS job_id,
                j.article_id,
                a.canonical_url,
                a.description,
                j.attempts,
                j.max_attempts,
                j.trigger_type
             FROM article_extraction_jobs j
             INNER JOIN articles a ON a.id = j.article_id
             WHERE j.id = ?",
        )
        .bind(job_id)
        .fetch_optional(&mut *conn)
        .await?;

        Ok(claimed)
    }
    .await;

    match claim_res {
        Ok(val) => {
            sqlx::query("COMMIT").execute(&mut *conn).await?;
            Ok(val)
        }
        Err(err) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(err)
        }
    }
}

/// Persist the outcome of a claimed extraction job using `BEGIN IMMEDIATE` so
/// concurrent extraction workers serialize their completion writes cleanly.
pub async fn complete_extraction_job(
    pool: &SqlitePool,
    job: &ClaimedExtractionJob,
    outcome: &extractor::ExtractionResult,
) -> AppResult<()> {
    let mut conn = pool.acquire().await?;
    sqlx::query("BEGIN IMMEDIATE").execute(&mut *conn).await?;
    let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let write_res: AppResult<()> = async {
        match outcome {
            extractor::ExtractionResult::Completed {
                sanitized_html,
                content_hash,
                ..
            } => {
                let existing_hash: Option<(Option<String>, String)> = sqlx::query_as(
                    "SELECT content_hash, extraction_status FROM articles WHERE id = ?",
                )
                .bind(job.article_id)
                .fetch_optional(&mut *conn)
                .await?;

                let unchanged_completed = existing_hash.as_ref().is_some_and(|(h, st)| {
                    h.as_deref() == Some(content_hash.as_str()) && st == "extracted"
                });

                if unchanged_completed {
                    sqlx::query(
                        "UPDATE articles
                         SET extraction_status = 'extracted',
                             extracted_content_type = 'html',
                             last_successful_extraction_at = ?,
                             extraction_error = NULL,
                             next_extraction_retry_at = NULL
                         WHERE id = ?",
                    )
                    .bind(&now_str)
                    .bind(job.article_id)
                    .execute(&mut *conn)
                    .await?;
                } else {
                    sqlx::query(
                        "UPDATE articles
                         SET extracted_content = ?,
                             extraction_status = 'extracted',
                             extracted_content_type = 'html',
                             content_hash = ?,
                             last_successful_extraction_at = ?,
                             extraction_error = NULL,
                             next_extraction_retry_at = NULL,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(sanitized_html)
                    .bind(content_hash)
                    .bind(&now_str)
                    .bind(&now_str)
                    .bind(job.article_id)
                    .execute(&mut *conn)
                    .await?;
                }

                sqlx::query(
                    "UPDATE article_extraction_jobs
                     SET status = 'completed',
                         completed_at = ?,
                         last_error = NULL,
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(&now_str)
                .bind(&now_str)
                .bind(job.job_id)
                .execute(&mut *conn)
                .await?;
            }
            extractor::ExtractionResult::Fallback {
                sanitized_html,
                content_hash,
                reason,
            } => {
                sqlx::query(
                    "UPDATE articles
                     SET extracted_content = ?,
                         extraction_status = 'skipped',
                         extracted_content_type = 'summary_fallback',
                         content_hash = ?,
                         extraction_error = ?,
                         next_extraction_retry_at = NULL,
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(sanitized_html)
                .bind(content_hash)
                .bind(reason)
                .bind(&now_str)
                .bind(job.article_id)
                .execute(&mut *conn)
                .await?;

                sqlx::query(
                    "UPDATE article_extraction_jobs
                     SET status = 'completed',
                         completed_at = ?,
                         last_error = ?,
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(&now_str)
                .bind(reason)
                .bind(&now_str)
                .bind(job.job_id)
                .execute(&mut *conn)
                .await?;
            }
            extractor::ExtractionResult::Failed {
                error_message,
                retryable,
            } => {
                if *retryable && job.attempts < job.max_attempts {
                    let backoff_secs =
                        15i64.saturating_mul(2i64.saturating_pow(job.attempts as u32));
                    let next_retry = (Utc::now() + ChronoDuration::seconds(backoff_secs.min(1800)))
                        .to_rfc3339_opts(SecondsFormat::Secs, true);

                    sqlx::query(
                        "UPDATE articles
                         SET extraction_status = 'pending',
                             extraction_error = ?,
                             next_extraction_retry_at = ?,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(error_message)
                    .bind(&next_retry)
                    .bind(&now_str)
                    .bind(job.article_id)
                    .execute(&mut *conn)
                    .await?;

                    sqlx::query(
                        "UPDATE article_extraction_jobs
                         SET status = 'queued',
                             next_eligible_at = ?,
                             claimed_at = NULL,
                             last_error = ?,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(&next_retry)
                    .bind(error_message)
                    .bind(&now_str)
                    .bind(job.job_id)
                    .execute(&mut *conn)
                    .await?;
                } else {
                    sqlx::query(
                        "UPDATE articles
                         SET extraction_status = 'failed',
                             extraction_error = ?,
                             next_extraction_retry_at = NULL,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(error_message)
                    .bind(&now_str)
                    .bind(job.article_id)
                    .execute(&mut *conn)
                    .await?;

                    sqlx::query(
                        "UPDATE article_extraction_jobs
                         SET status = 'failed',
                             completed_at = ?,
                             last_error = ?,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(&now_str)
                    .bind(error_message)
                    .bind(&now_str)
                    .bind(job.job_id)
                    .execute(&mut *conn)
                    .await?;
                }
            }
        }
        Ok(())
    }
    .await;

    match write_res {
        Ok(()) => {
            sqlx::query("COMMIT").execute(&mut *conn).await?;
            Ok(())
        }
        Err(err) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut *conn).await;
            Err(err)
        }
    }
}

pub async fn list_recent_articles(
    pool: &SqlitePool,
    limit: i64,
) -> AppResult<Vec<ArticleSummaryRow>> {
    let sql = format!(
        "SELECT
            a.id,
            a.feed_id,
            f.name AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM articles a
         INNER JOIN feeds f ON f.id = a.feed_id
         ORDER BY COALESCE(a.published_at, a.discovered_at) DESC, a.id DESC
         LIMIT ?"
    );
    let articles = sqlx::query_as::<_, ArticleSummaryRow>(&sql)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(articles)
}

pub async fn list_articles_for_feed(
    pool: &SqlitePool,
    feed_id: i64,
    limit: i64,
) -> AppResult<Vec<ArticleSummaryRow>> {
    let sql = format!(
        "SELECT
            a.id,
            a.feed_id,
            f.name AS feed_name,
            COALESCE(afs.external_guid, a.external_guid) AS external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            afs.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM article_feed_sources afs
         INNER JOIN articles a ON a.id = afs.article_id
         INNER JOIN feeds f ON f.id = afs.feed_id
         WHERE afs.feed_id = ?
         ORDER BY COALESCE(a.published_at, afs.discovered_at) DESC, a.id DESC
         LIMIT ?"
    );
    let articles = sqlx::query_as::<_, ArticleSummaryRow>(&sql)
        .bind(feed_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    Ok(articles)
}

pub async fn query_articles_paginated(
    pool: &SqlitePool,
    filter: &ArticleFilter,
) -> AppResult<PaginatedArticles> {
    let page = filter.page.max(1);
    let page_size = filter.page_size.clamp(1, 100);
    let offset = ((page - 1) * page_size) as i64;

    let status_filter = filter
        .extraction_status
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let pub_date = filter
        .published_date
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let disc_date = filter
        .discovered_date
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    let count_sql = format!(
        "SELECT COUNT(*)
         FROM articles a
         WHERE (?1 IS NULL OR a.feed_id = ?1 OR EXISTS (
                    SELECT 1 FROM article_feed_sources afs
                    WHERE afs.article_id = a.id AND afs.feed_id = ?1
                ))
           AND (?2 IS NULL OR ({LOGICAL_STATUS_SQL}) = ?2)
           AND (?3 IS NULL OR substr(a.published_at, 1, 10) = ?3)
           AND (?4 IS NULL OR substr(a.discovered_at, 1, 10) = ?4)"
    );

    let (total_count,): (i64,) = sqlx::query_as(&count_sql)
        .bind(filter.feed_id)
        .bind(status_filter)
        .bind(pub_date)
        .bind(disc_date)
        .fetch_one(pool)
        .await?;

    let items_sql = format!(
        "SELECT
            a.id,
            a.feed_id,
            f.name AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM articles a
         INNER JOIN feeds f ON f.id = a.feed_id
         WHERE (?1 IS NULL OR a.feed_id = ?1 OR EXISTS (
                    SELECT 1 FROM article_feed_sources afs
                    WHERE afs.article_id = a.id AND afs.feed_id = ?1
                ))
           AND (?2 IS NULL OR ({LOGICAL_STATUS_SQL}) = ?2)
           AND (?3 IS NULL OR substr(a.published_at, 1, 10) = ?3)
           AND (?4 IS NULL OR substr(a.discovered_at, 1, 10) = ?4)
         ORDER BY COALESCE(a.published_at, a.discovered_at) DESC, a.id DESC
         LIMIT ?5 OFFSET ?6"
    );

    let items = sqlx::query_as::<_, ArticleSummaryRow>(&items_sql)
        .bind(filter.feed_id)
        .bind(status_filter)
        .bind(pub_date)
        .bind(disc_date)
        .bind(page_size as i64)
        .bind(offset)
        .fetch_all(pool)
        .await?;

    let total_pages = if total_count == 0 {
        1
    } else {
        (total_count as usize).div_ceil(page_size)
    };

    Ok(PaginatedArticles {
        items,
        total_count,
        page,
        page_size,
        total_pages,
    })
}

pub async fn get_article_by_id(pool: &SqlitePool, id: i64) -> AppResult<ArticleDetail> {
    let sql = format!(
        "SELECT
            a.id,
            a.feed_id,
            f.name AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.original_source_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.entry_updated_at,
            a.discovered_at,
            a.extracted_content,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            a.content_hash,
            a.extraction_attempts,
            a.last_extraction_attempt_at,
            a.last_successful_extraction_at,
            a.extraction_error,
            a.next_extraction_retry_at,
            a.created_at,
            a.updated_at
         FROM articles a
         INNER JOIN feeds f ON f.id = a.feed_id
         WHERE a.id = ?"
    );
    let article = sqlx::query_as::<_, ArticleDetail>(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Article with ID {id} was not found.")))?;
    Ok(article)
}

pub async fn list_article_sources(
    pool: &SqlitePool,
    article_id: i64,
) -> AppResult<Vec<ArticleFeedSourceRow>> {
    let rows = sqlx::query_as::<_, ArticleFeedSourceRow>(
        "SELECT
            afs.feed_id,
            f.name AS feed_name,
            afs.external_guid,
            afs.discovered_at,
            afs.last_seen_at
         FROM article_feed_sources afs
         INNER JOIN feeds f ON f.id = afs.feed_id
         WHERE afs.article_id = ?
         ORDER BY afs.discovered_at ASC",
    )
    .bind(article_id)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}
