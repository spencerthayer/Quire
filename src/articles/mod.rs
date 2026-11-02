pub mod extractor;
pub mod normalizer;
pub mod sanitizer;

use chrono::{Duration as ChronoDuration, SecondsFormat, Utc};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use tracing::warn;

use crate::db::with_write_retry;
use crate::error::{AppError, AppResult};
use crate::feeds::parser::NormalizedFeedEntry;

/// Computes the derived display state (`Full text`, `Full text (refreshing)`,
/// `Summary (full text pending)`, `Summary only`, `Extracting`, `Pending`, or
/// `Failed`) from `articles.extracted_content_type` and `article_extraction_jobs.status`
/// per Revision 2 §4.1.
pub const DISPLAY_STATE_SQL: &str = "
    CASE
        WHEN a.extracted_content_type = 'html'
             AND EXISTS (
                 SELECT 1 FROM article_extraction_jobs j
                 WHERE j.article_id = a.id AND j.status IN ('queued', 'processing')
             ) THEN 'Full text (refreshing)'
        WHEN a.extracted_content_type = 'html' THEN 'Full text'
        WHEN a.extracted_content_type = 'summary_fallback'
             AND EXISTS (
                 SELECT 1 FROM article_extraction_jobs j
                 WHERE j.article_id = a.id AND j.status IN ('queued', 'processing')
             ) THEN 'Summary (full text pending)'
        WHEN a.extracted_content_type = 'summary_fallback' THEN 'Summary only'
        WHEN EXISTS (
            SELECT 1 FROM article_extraction_jobs j
            WHERE j.article_id = a.id AND j.status = 'processing'
        ) THEN 'Extracting'
        WHEN EXISTS (
            SELECT 1 FROM article_extraction_jobs j
            WHERE j.article_id = a.id AND j.status = 'queued'
        ) THEN 'Pending'
        ELSE 'Failed'
    END
";

/// Logical filter token (`completed`, `fallback`, `processing`, `pending`, `failed`)
/// derived strictly from `articles.extracted_content_type` and `article_extraction_jobs.status`.
pub const LOGICAL_STATUS_SQL: &str = "
    CASE
        WHEN EXISTS (
            SELECT 1 FROM article_extraction_jobs j
            WHERE j.article_id = a.id AND j.status = 'processing'
        ) THEN 'processing'
        WHEN EXISTS (
            SELECT 1 FROM article_extraction_jobs j
            WHERE j.article_id = a.id AND j.status = 'queued'
        ) THEN 'pending'
        WHEN a.extracted_content_type = 'html' THEN 'completed'
        WHEN a.extracted_content_type = 'summary_fallback' THEN 'fallback'
        ELSE 'failed'
    END
";

/// Derive the human-readable display state from `(extracted_content_type, job_status)`
/// per the Revision 2 §4.1 table.
pub fn derive_display_state(content_type: &str, job_status: Option<&str>) -> &'static str {
    match (content_type, job_status) {
        ("html", Some("queued" | "processing")) => "Full text (refreshing)",
        ("html", _) => "Full text",
        ("summary_fallback", Some("queued" | "processing")) => "Summary (full text pending)",
        ("summary_fallback", _) => "Summary only",
        (_, Some("processing")) => "Extracting",
        (_, Some("queued")) => "Pending",
        _ => "Failed",
    }
}

/// Derive the legacy `articles.extraction_status` value (`pending`, `extracted`,
/// `skipped`, `failed`) from `(extracted_content_type, job_status)` per Revision 2 §4.1.
pub fn derive_legacy_extraction_status(
    content_type: &str,
    job_status: Option<&str>,
) -> &'static str {
    match (content_type, job_status) {
        (_, Some("queued" | "processing")) => "pending",
        ("html", _) => "extracted",
        ("summary_fallback", _) => "skipped",
        _ => "failed",
    }
}

/// Synchronize the write-only legacy `articles.extraction_status` column at the
/// end of any transaction that updates `articles.extracted_content_type` or
/// `article_extraction_jobs.status`.
pub async fn sync_legacy_status(
    tx: &mut Transaction<'_, Sqlite>,
    article_id: i64,
) -> AppResult<()> {
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT
            a.extracted_content_type,
            (SELECT j.status FROM article_extraction_jobs j WHERE j.article_id = a.id)
         FROM articles a
         WHERE a.id = ?",
    )
    .bind(article_id)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some((content_type, job_status)) = row {
        let legacy = derive_legacy_extraction_status(&content_type, job_status.as_deref());
        sqlx::query("UPDATE articles SET extraction_status = ? WHERE id = ?")
            .bind(legacy)
            .bind(article_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleSummaryRow {
    pub id: i64,
    pub feed_id: Option<i64>,
    pub feed_name: String,
    pub external_guid: Option<String>,
    pub canonical_url: String,
    pub title: String,
    pub author: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<String>,
    pub discovered_at: String,
    pub extraction_status: String,
    pub display_state: String,
    pub extracted_content_type: String,
    pub has_extracted_content: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleDetail {
    pub id: i64,
    pub feed_id: Option<i64>,
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
    pub extracted_content_type: String,
    pub extraction_status: String,
    pub display_state: String,
    pub job_status: Option<String>,
    pub job_max_attempts: i64,
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
    pub claim_token: String,
    pub canonical_url: String,
    pub description: Option<String>,
    pub attempts: i64,
    pub max_attempts: i64,
    pub trigger_type: String,
}

#[derive(Debug, FromRow)]
struct ExistingArticleRow {
    id: i64,
    title: String,
    author: Option<String>,
    description: Option<String>,
    published_at: Option<String>,
    entry_updated_at: Option<String>,
    extracted_content_type: String,
}

fn generate_claim_token() -> String {
    let mut bytes = [0u8; 16];
    rand::rng().fill(&mut bytes);
    let mut out = String::with_capacity(32);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Persist a batch of normalized feed entries inside an immediate, retry-wrapped
/// SQLite transaction (`db::with_write_retry`).
pub async fn persist_discovered_entries(
    pool: &SqlitePool,
    feed_id: i64,
    entries: &[NormalizedFeedEntry],
    max_attempts: u32,
) -> AppResult<PersistBatchStats> {
    if entries.is_empty() {
        return Ok(PersistBatchStats::default());
    }

    let entries_owned = entries.to_vec();
    with_write_retry(pool, |tx| {
        let batch = entries_owned.clone();
        Box::pin(async move {
            persist_discovered_entries_in_tx(tx, feed_id, &batch, max_attempts).await
        })
    })
    .await
}

/// Persist discovered entries inside an existing transaction (used by both
/// `persist_discovered_entries` and the merged ownership-checked feed completion
/// transaction).
pub async fn persist_discovered_entries_in_tx(
    tx: &mut Transaction<'_, Sqlite>,
    feed_id: i64,
    entries: &[NormalizedFeedEntry],
    max_attempts: u32,
) -> AppResult<PersistBatchStats> {
    let mut stats = PersistBatchStats::default();
    for entry in entries {
        let outcome =
            upsert_single_entry(tx, feed_id, entry, i64::from(max_attempts.max(1))).await?;
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
            title,
            author,
            description,
            published_at,
            entry_updated_at,
            extracted_content_type
         FROM articles
         WHERE canonical_url = ?",
    )
    .bind(&entry.canonical_url)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some(existing_row) = existing {
        // `article_feed_sources` is the sole authority for feed-specific GUIDs
        // (Revision 2 §5.2). If this feed previously associated the same GUID
        // with a different article, clear that old association first.
        let prev_guid_for_feed: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT external_guid FROM article_feed_sources WHERE article_id = ? AND feed_id = ?",
        )
        .bind(existing_row.id)
        .bind(feed_id)
        .fetch_optional(&mut **tx)
        .await?;

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
        let guid_changed = prev_guid_for_feed
            .as_ref()
            .is_some_and(|(old_g,)| entry.external_guid.is_some() && &entry.external_guid != old_g);

        if entry_updated_changed || title_changed || summary_changed || guid_changed {
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
                 SET title = ?,
                     author = ?,
                     description = ?,
                     published_at = ?,
                     entry_updated_at = ?,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE id = ?",
            )
            .bind(&entry.title)
            .bind(new_author)
            .bind(new_desc)
            .bind(new_pub)
            .bind(new_entry_updated)
            .bind(existing_row.id)
            .execute(&mut **tx)
            .await?;

            let should_requeue =
                entry_updated_changed || existing_row.extracted_content_type != "html";
            let mut queued = false;
            if should_requeue {
                queued = enqueue_extraction_job_in_tx(
                    tx,
                    existing_row.id,
                    "auto",
                    max_attempts,
                    false,
                )
                .await?;
            }
            return Ok(EntryUpsertOutcome::Updated { queued });
        }

        return Ok(EntryUpsertOutcome::Unchanged);
    }

    // If the publisher reused an existing `external_guid` on the same feed for a
    // brand-new canonical URL, clear the old GUID in `article_feed_sources` so
    // distinct URLs are never merged.
    if let Some(guid) = entry.external_guid.as_deref().filter(|g| !g.is_empty()) {
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

    // QUA-023 & TRP-012: Insert `external_guid = NULL` on `articles` and
    // explicitly set `extracted_content_type = 'none'`.
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
            extraction_status,
            extracted_content_type
         )
         VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, 'pending', 'none')",
    )
    .bind(feed_id)
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
                 claim_token = NULL,
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
         SET next_extraction_retry_at = NULL,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(article_id)
    .execute(&mut **tx)
    .await?;

    sync_legacy_status(tx, article_id).await?;
    Ok(true)
}

/// Enqueue a manual retry or content refresh for a single article from the web UI
/// inside a managed `BEGIN IMMEDIATE` transaction with busy retries.
pub async fn enqueue_manual_article_extraction(
    pool: &SqlitePool,
    article_id: i64,
    max_attempts: u32,
) -> AppResult<bool> {
    with_write_retry(pool, |tx| {
        Box::pin(async move {
            let exists: Option<(i64,)> =
                sqlx::query_as("SELECT id FROM articles WHERE id = ?")
                    .bind(article_id)
                    .fetch_optional(&mut **tx)
                    .await?;
            if exists.is_none() {
                return Err(AppError::NotFound(format!(
                    "Article with ID {article_id} was not found."
                )));
            }

            enqueue_extraction_job_in_tx(
                tx,
                article_id,
                "manual",
                i64::from(max_attempts.max(1)),
                true,
            )
            .await
        })
    })
    .await
}

/// Recover interrupted extraction jobs left in `processing` state after an
/// unexpected crash or container restart (Revision 2 §2.4 / QUA-011).
/// Jobs that have already reached `attempts >= max_attempts` transition to
/// terminal `failed` rather than looping forever.
pub async fn recover_interrupted_extractions(pool: &SqlitePool) -> AppResult<u64> {
    with_write_retry(pool, |tx| {
        Box::pin(async move {
            let now_str = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

            let affected_articles: Vec<(i64,)> = sqlx::query_as(
                "SELECT article_id FROM article_extraction_jobs WHERE status = 'processing'",
            )
            .fetch_all(&mut **tx)
            .await?;

            // 1. Exhausted jobs (`attempts >= max_attempts`) become terminal `failed`
            let exhausted_res = sqlx::query(
                "UPDATE article_extraction_jobs
                 SET status = 'failed',
                     claim_token = NULL,
                     completed_at = ?,
                     last_error = COALESCE(last_error, 'Exceeded maximum extraction attempts after unexpected restart.'),
                     updated_at = ?
                 WHERE status = 'processing' AND attempts >= max_attempts",
            )
            .bind(&now_str)
            .bind(&now_str)
            .execute(&mut **tx)
            .await?;

            // 2. Remaining interrupted jobs return to `queued`
            let requeued_res = sqlx::query(
                "UPDATE article_extraction_jobs
                 SET status = 'queued',
                     claim_token = NULL,
                     claimed_at = NULL,
                     next_eligible_at = ?,
                     updated_at = ?
                 WHERE status = 'processing' AND attempts < max_attempts",
            )
            .bind(&now_str)
            .bind(&now_str)
            .execute(&mut **tx)
            .await?;

            for (art_id,) in affected_articles {
                sync_legacy_status(tx, art_id).await?;
            }

            Ok(exhausted_res.rows_affected() + requeued_res.rows_affected())
        })
    })
    .await
}

/// Atomically claim the next eligible extraction job (`attempts < max_attempts`)
/// from SQLite using a managed `BEGIN IMMEDIATE` transaction and assign a unique
/// 128-bit `claim_token` (Revision 2 §2.4 / QUA-010 / QUA-011 / QUA-019).
pub async fn claim_next_extraction_job(
    pool: &SqlitePool,
) -> AppResult<Option<ClaimedExtractionJob>> {
    with_write_retry(pool, |tx| {
        Box::pin(async move {
            let candidate: Option<(i64, i64)> = sqlx::query_as(
                "SELECT id, article_id
                 FROM article_extraction_jobs
                 WHERE status = 'queued'
                   AND next_eligible_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                   AND attempts < max_attempts
                 ORDER BY next_eligible_at ASC, id ASC
                 LIMIT 1",
            )
            .fetch_optional(&mut **tx)
            .await?;

            let Some((job_id, article_id)) = candidate else {
                return Ok(None);
            };

            let token = generate_claim_token();

            let update_res = sqlx::query(
                "UPDATE article_extraction_jobs
                 SET status = 'processing',
                     attempts = attempts + 1,
                     claim_token = ?,
                     claimed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE id = ? AND status = 'queued' AND attempts < max_attempts",
            )
            .bind(&token)
            .bind(job_id)
            .execute(&mut **tx)
            .await?;

            if update_res.rows_affected() == 0 {
                return Ok(None);
            }

            sqlx::query(
                "UPDATE articles
                 SET extraction_attempts = extraction_attempts + 1,
                     last_extraction_attempt_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
                     updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
                 WHERE id = ?",
            )
            .bind(article_id)
            .execute(&mut **tx)
            .await?;

            sync_legacy_status(tx, article_id).await?;

            let claimed = sqlx::query_as::<_, ClaimedExtractionJob>(
                "SELECT
                    j.id AS job_id,
                    j.article_id,
                    COALESCE(j.claim_token, '') AS claim_token,
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
            .fetch_optional(&mut **tx)
            .await?;

            Ok(claimed)
        })
    })
    .await
}

/// Conditionally release an in-flight extraction job when a worker panics, hits
/// its hard deadline, or is cancelled during graceful shutdown (Revision 2 §2.4 & §2.5).
pub async fn release_extraction_job(
    pool: &SqlitePool,
    job_id: i64,
    article_id: i64,
    claim_token: &str,
    revert_attempt_for_shutdown: bool,
    reason: Option<&str>,
) -> AppResult<bool> {
    let token_owned = claim_token.to_string();
    let reason_owned = reason.map(str::to_string);

    with_write_retry(pool, |tx| {
        let token_ref = token_owned.clone();
        let reason_ref = reason_owned.clone();
        Box::pin(async move {
            let now = Utc::now();
            let now_str = now.to_rfc3339_opts(SecondsFormat::Secs, true);

            let res = if revert_attempt_for_shutdown {
                // Graceful shutdown release: revert the claim's attempt increment
                // atomically in the same ownership-checked statement (§2.5).
                sqlx::query(
                    "UPDATE article_extraction_jobs
                     SET status = 'queued',
                         attempts = MAX(0, attempts - 1),
                         claim_token = NULL,
                         claimed_at = NULL,
                         next_eligible_at = ?,
                         updated_at = ?
                     WHERE id = ? AND claim_token = ? AND status = 'processing'",
                )
                .bind(&now_str)
                .bind(&now_str)
                .bind(job_id)
                .bind(&token_ref)
                .execute(&mut **tx)
                .await?
            } else {
                // Crash / panic / deadline / DB-error release: attempt remains counted.
                let next_elig = (now + ChronoDuration::seconds(60))
                    .to_rfc3339_opts(SecondsFormat::Secs, true);
                sqlx::query(
                    "UPDATE article_extraction_jobs
                     SET status = CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'queued' END,
                         claim_token = NULL,
                         claimed_at = NULL,
                         completed_at = CASE WHEN attempts >= max_attempts THEN ? ELSE completed_at END,
                         next_eligible_at = ?,
                         last_error = COALESCE(?, last_error),
                         updated_at = ?
                     WHERE id = ? AND claim_token = ? AND status = 'processing'",
                )
                .bind(&now_str)
                .bind(&next_elig)
                .bind(&reason_ref)
                .bind(&now_str)
                .bind(job_id)
                .bind(&token_ref)
                .execute(&mut **tx)
                .await?
            };

            if res.rows_affected() == 0 {
                return Ok(false);
            }

            if revert_attempt_for_shutdown {
                sqlx::query(
                    "UPDATE articles
                     SET extraction_attempts = MAX(0, extraction_attempts - 1),
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(&now_str)
                .bind(article_id)
                .execute(&mut **tx)
                .await?;
            } else if let Some(err_msg) = &reason_ref {
                sqlx::query(
                    "UPDATE articles
                     SET extraction_error = ?,
                         updated_at = ?
                     WHERE id = ?",
                )
                .bind(err_msg)
                .bind(&now_str)
                .bind(article_id)
                .execute(&mut **tx)
                .await?;
            }

            sync_legacy_status(tx, article_id).await?;
            Ok(true)
        })
    })
    .await
}

/// Persist the outcome of a claimed extraction job inside a managed `BEGIN IMMEDIATE`
/// transaction, verifying `WHERE id = :job_id AND claim_token = :tok AND status = 'processing'`
/// FIRST so a stale worker can never overwrite a newer result (Revision 2 §2.4 & §4).
pub async fn complete_extraction_job(
    pool: &SqlitePool,
    job: &ClaimedExtractionJob,
    outcome: &extractor::ExtractionResult,
) -> AppResult<bool> {
    let job_owned = job.clone();
    let outcome_owned = outcome.clone();
    with_write_retry(pool, |tx| {
        let job = job_owned.clone();
        let outcome = outcome_owned.clone();
        Box::pin(async move {
            let now = Utc::now();
            let now_str = now.to_rfc3339_opts(SecondsFormat::Secs, true);

            // Verify ownership on `article_extraction_jobs` before touching `articles`
            let current_owner: Option<(String,)> = sqlx::query_as(
                "SELECT COALESCE(claim_token, '')
                 FROM article_extraction_jobs
                 WHERE id = ? AND status = 'processing'",
            )
            .bind(job.job_id)
            .fetch_optional(&mut **tx)
            .await?;

            let Some((db_token,)) = current_owner else {
                warn!(
                    job_id = job.job_id,
                    article_id = job.article_id,
                    "Discarding stale extraction completion: job is no longer in processing state"
                );
                return Ok(false);
            };

            if db_token != job.claim_token {
                warn!(
                    job_id = job.job_id,
                    article_id = job.article_id,
                    "Discarding stale extraction completion: claim_token mismatch"
                );
                return Ok(false);
            }

            match outcome {
                extractor::ExtractionResult::Completed {
                    sanitized_html,
                    content_hash,
                    ..
                } => {
                    let job_res = sqlx::query(
                        "UPDATE article_extraction_jobs
                         SET status = 'completed',
                             claim_token = NULL,
                             completed_at = ?,
                             last_error = NULL,
                             updated_at = ?
                         WHERE id = ? AND claim_token = ? AND status = 'processing'",
                    )
                    .bind(&now_str)
                    .bind(&now_str)
                    .bind(job.job_id)
                    .bind(&job.claim_token)
                    .execute(&mut **tx)
                    .await?;

                    if job_res.rows_affected() == 0 {
                        return Ok(false);
                    }

                    let existing_hash: Option<(Option<String>, String)> = sqlx::query_as(
                        "SELECT content_hash, extracted_content_type FROM articles WHERE id = ?",
                    )
                    .bind(job.article_id)
                    .fetch_optional(&mut **tx)
                    .await?;

                    let unchanged_completed = existing_hash.as_ref().is_some_and(|(h, ct)| {
                        h.as_deref() == Some(content_hash.as_str()) && ct == "html"
                    });

                    if unchanged_completed {
                        sqlx::query(
                            "UPDATE articles
                             SET extracted_content_type = 'html',
                                 last_successful_extraction_at = ?,
                                 extraction_error = NULL,
                                 next_extraction_retry_at = NULL
                             WHERE id = ?",
                        )
                        .bind(&now_str)
                        .bind(job.article_id)
                        .execute(&mut **tx)
                        .await?;
                    } else {
                        sqlx::query(
                            "UPDATE articles
                             SET extracted_content = ?,
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
                        .execute(&mut **tx)
                        .await?;
                    }
                }
                extractor::ExtractionResult::Fallback {
                    sanitized_html,
                    content_hash,
                    reason,
                } => {
                    let job_res = sqlx::query(
                        "UPDATE article_extraction_jobs
                         SET status = 'completed',
                             claim_token = NULL,
                             completed_at = ?,
                             last_error = ?,
                             updated_at = ?
                         WHERE id = ? AND claim_token = ? AND status = 'processing'",
                    )
                    .bind(&now_str)
                    .bind(&reason)
                    .bind(&now_str)
                    .bind(job.job_id)
                    .bind(&job.claim_token)
                    .execute(&mut **tx)
                    .await?;

                    if job_res.rows_affected() == 0 {
                        return Ok(false);
                    }

                    // Preserve existing full HTML if this was a manual refresh that failed
                    sqlx::query(
                        "UPDATE articles
                         SET extracted_content = CASE
                                 WHEN extracted_content_type = 'html' THEN extracted_content
                                 ELSE ?
                             END,
                             extracted_content_type = CASE
                                 WHEN extracted_content_type = 'html' THEN 'html'
                                 ELSE 'summary_fallback'
                             END,
                             content_hash = CASE
                                 WHEN extracted_content_type = 'html' THEN content_hash
                                 ELSE ?
                             END,
                             extraction_error = ?,
                             next_extraction_retry_at = NULL,
                             updated_at = ?
                         WHERE id = ?",
                    )
                    .bind(&sanitized_html)
                    .bind(&content_hash)
                    .bind(&reason)
                    .bind(&now_str)
                    .bind(job.article_id)
                    .execute(&mut **tx)
                    .await?;
                }
                extractor::ExtractionResult::Failed {
                    error_message,
                    retryable,
                    retry_after,
                    interim_fallback,
                    cancelled: _,
                } => {
                    // If an interim summary fallback was produced and the article currently
                    // has `extracted_content_type = 'none'`, store the summary now so the
                    // reader has immediate summary access while retries continue (§4.2).
                    let interim_fallback_opt = interim_fallback.clone().or_else(|| {
                        job.description
                            .as_deref()
                            .and_then(|s| sanitizer::sanitize_summary_fallback(s, None))
                            .map(|html| {
                                let hash = normalizer::compute_content_hash(&html);
                                (html, hash)
                            })
                    });

                    if let Some((fb_html, fb_hash)) = &interim_fallback_opt {
                        sqlx::query(
                            "UPDATE articles
                             SET extracted_content = ?,
                                 extracted_content_type = 'summary_fallback',
                                 content_hash = ?
                             WHERE id = ? AND COALESCE(extracted_content_type, 'none') = 'none'",
                        )
                        .bind(fb_html)
                        .bind(fb_hash)
                        .bind(job.article_id)
                        .execute(&mut **tx)
                        .await?;
                    }

                    if retryable && job.attempts < job.max_attempts {
                        let exp_secs =
                            15i64.saturating_mul(2i64.saturating_pow(job.attempts as u32));
                        let ra_secs = retry_after.map(|d| d.as_secs() as i64).unwrap_or(0);
                        let backoff_secs = exp_secs.max(ra_secs).min(6 * 3600);
                        let next_retry = (now + ChronoDuration::seconds(backoff_secs))
                            .to_rfc3339_opts(SecondsFormat::Secs, true);

                        let job_res = sqlx::query(
                            "UPDATE article_extraction_jobs
                             SET status = 'queued',
                                 claim_token = NULL,
                                 next_eligible_at = ?,
                                 claimed_at = NULL,
                                 last_error = ?,
                                 updated_at = ?
                             WHERE id = ? AND claim_token = ? AND status = 'processing'",
                        )
                        .bind(&next_retry)
                        .bind(&error_message)
                        .bind(&now_str)
                        .bind(job.job_id)
                        .bind(&job.claim_token)
                        .execute(&mut **tx)
                        .await?;

                        if job_res.rows_affected() == 0 {
                            return Ok(false);
                        }

                        sqlx::query(
                            "UPDATE articles
                             SET extraction_error = ?,
                                 next_extraction_retry_at = ?,
                                 updated_at = ?
                             WHERE id = ?",
                        )
                        .bind(&error_message)
                        .bind(&next_retry)
                        .bind(&now_str)
                        .bind(job.article_id)
                        .execute(&mut **tx)
                        .await?;
                    } else {
                        let job_res = sqlx::query(
                            "UPDATE article_extraction_jobs
                             SET status = 'failed',
                                 claim_token = NULL,
                                 completed_at = ?,
                                 last_error = ?,
                                 updated_at = ?
                             WHERE id = ? AND claim_token = ? AND status = 'processing'",
                        )
                        .bind(&now_str)
                        .bind(&error_message)
                        .bind(&now_str)
                        .bind(job.job_id)
                        .bind(&job.claim_token)
                        .execute(&mut **tx)
                        .await?;

                        if job_res.rows_affected() == 0 {
                            return Ok(false);
                        }

                        sqlx::query(
                            "UPDATE articles
                             SET extraction_error = ?,
                                 next_extraction_retry_at = NULL,
                                 updated_at = ?
                             WHERE id = ?",
                        )
                        .bind(&error_message)
                        .bind(&now_str)
                        .bind(job.article_id)
                        .execute(&mut **tx)
                        .await?;
                    }
                }
            }

            sync_legacy_status(tx, job.article_id).await?;
            Ok(true)
        })
    })
    .await
}

pub async fn list_recent_articles(
    pool: &SqlitePool,
    limit: i64,
) -> AppResult<Vec<ArticleSummaryRow>> {
    let sql = format!(
        "SELECT
            a.id,
            a.feed_id,
            COALESCE(f.name, '(removed feed)') AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            {DISPLAY_STATE_SQL} AS display_state,
            a.extracted_content_type,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM articles a
         LEFT JOIN feeds f ON f.id = a.feed_id
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
            COALESCE(f.name, '(removed feed)') AS feed_name,
            afs.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            afs.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            {DISPLAY_STATE_SQL} AS display_state,
            a.extracted_content_type,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM article_feed_sources afs
         INNER JOIN articles a ON a.id = afs.article_id
         LEFT JOIN feeds f ON f.id = afs.feed_id
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
            COALESCE(f.name, '(removed feed)') AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.discovered_at,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            {DISPLAY_STATE_SQL} AS display_state,
            a.extracted_content_type,
            (a.extracted_content IS NOT NULL AND a.extracted_content != '') AS has_extracted_content,
            a.created_at
         FROM articles a
         LEFT JOIN feeds f ON f.id = a.feed_id
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
            COALESCE(f.name, '(removed feed)') AS feed_name,
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
            a.extracted_content_type,
            {LOGICAL_STATUS_SQL} AS extraction_status,
            {DISPLAY_STATE_SQL} AS display_state,
            (SELECT j.status FROM article_extraction_jobs j WHERE j.article_id = a.id) AS job_status,
            COALESCE((SELECT j.max_attempts FROM article_extraction_jobs j WHERE j.article_id = a.id), 3) AS job_max_attempts,
            a.content_hash,
            a.extraction_attempts,
            a.last_extraction_attempt_at,
            a.last_successful_extraction_at,
            a.extraction_error,
            a.next_extraction_retry_at,
            a.created_at,
            a.updated_at
         FROM articles a
         LEFT JOIN feeds f ON f.id = a.feed_id
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
