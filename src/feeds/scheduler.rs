use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

use crate::articles::extractor::{ExtractionResult, fetch_and_extract_article};
use crate::articles::{
    self, ClaimedExtractionJob, claim_next_extraction_job, complete_extraction_job,
    enqueue_manual_article_extraction, persist_discovered_entries, recover_interrupted_extractions,
};
use crate::config::{AppConfig, RuntimeIngestionConfig};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::feeds::fetcher::{FetchOutcome, FetchRequestOptions, fetch_url_with_retries};
use crate::feeds::parser::parse_feed_bytes;
use crate::feeds::validation::{HostResolver, default_resolver};
use crate::feeds::{
    CompleteFeedRunUpdate, Feed, complete_feed_fetch_run, get_feed_by_id, list_due_feed_ids,
    list_feeds, recover_interrupted_fetches, try_claim_feed_fetch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualTriggerOutcome {
    Started { run_id: i64 },
    AlreadyRunning,
}

#[derive(Clone)]
pub struct IngestionCoordinator {
    pool: SqlitePool,
    config: Arc<AppConfig>,
    resolver: Arc<dyn HostResolver>,
    wake_notify: Arc<Notify>,
    shutdown_tx: Arc<watch::Sender<bool>>,
}

impl IngestionCoordinator {
    pub fn new(pool: SqlitePool, config: Arc<AppConfig>) -> Self {
        Self::with_resolver(pool, config, default_resolver())
    }

    pub fn with_resolver(
        pool: SqlitePool,
        config: Arc<AppConfig>,
        resolver: Arc<dyn HostResolver>,
    ) -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            pool,
            config,
            resolver,
            wake_notify: Arc::new(Notify::new()),
            shutdown_tx: Arc::new(shutdown_tx),
        }
    }

    /// Recover any interrupted fetches or extraction jobs from a previous process
    /// run before starting background workers.
    pub async fn recover_startup_state(&self) -> AppResult<()> {
        let recovered_fetches = recover_interrupted_fetches(&self.pool).await?;
        let recovered_extractions = recover_interrupted_extractions(&self.pool).await?;
        if recovered_fetches > 0 || recovered_extractions > 0 {
            info!(
                recovered_fetches,
                recovered_extractions, "Recovered interrupted ingestion state on startup"
            );
        }
        Ok(())
    }

    /// Wake the background scheduler and extraction loop immediately (for example,
    /// after settings changes or manual job queueing).
    pub fn notify_workers(&self) {
        self.wake_notify.notify_waiters();
    }

    /// Signal background scheduler loops to stop accepting new scheduled work.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        self.wake_notify.notify_waiters();
    }

    /// Load current effective runtime ingestion settings from `AppConfig` + SQLite `settings`.
    pub async fn load_runtime_config(&self) -> AppResult<RuntimeIngestionConfig> {
        let db_settings = db::get_all_settings(&self.pool).await?;
        Ok(self.config.resolve_runtime_ingestion(&db_settings))
    }

    /// Enqueue and spawn a manual feed fetch in the background, returning immediately
    /// so the HTTP handler does not block on network I/O.
    pub async fn trigger_manual_feed_fetch(&self, feed_id: i64) -> AppResult<ManualTriggerOutcome> {
        // Ensure the feed exists first (returns NotFound if missing)
        let _ = get_feed_by_id(&self.pool, feed_id).await?;

        let Some((feed, run_id)) = try_claim_feed_fetch(&self.pool, feed_id, "manual").await?
        else {
            return Ok(ManualTriggerOutcome::AlreadyRunning);
        };

        let coordinator = self.clone();
        tokio::spawn(async move {
            let rt_cfg = match coordinator.load_runtime_config().await {
                Ok(c) => c,
                Err(e) => {
                    error!(feed_id = feed.id, error = ?e, "Failed to load runtime config for manual fetch");
                    return;
                }
            };
            coordinator
                .execute_claimed_feed_fetch(feed, run_id, &rt_cfg)
                .await;
            coordinator.notify_workers();
        });

        Ok(ManualTriggerOutcome::Started { run_id })
    }

    /// Trigger manual fetches for all enabled feeds that are not currently running.
    pub async fn trigger_manual_fetch_all_enabled(&self) -> AppResult<usize> {
        let all_feeds = list_feeds(&self.pool).await?;
        let mut started = 0usize;
        for feed in all_feeds
            .into_iter()
            .filter(|f| f.enabled && !f.fetch_in_progress)
        {
            if let Ok(ManualTriggerOutcome::Started { .. }) =
                self.trigger_manual_feed_fetch(feed.id).await
            {
                started += 1;
            }
        }
        Ok(started)
    }

    /// Execute a single feed fetch and persist discovered articles + extraction jobs
    /// synchronously (useful for deterministic tests and background worker tasks).
    pub async fn run_feed_now_and_wait(&self, feed_id: i64, trigger_type: &str) -> AppResult<bool> {
        let _ = get_feed_by_id(&self.pool, feed_id).await?;
        let Some((feed, run_id)) = try_claim_feed_fetch(&self.pool, feed_id, trigger_type).await?
        else {
            return Ok(false);
        };
        let rt_cfg = self.load_runtime_config().await?;
        self.execute_claimed_feed_fetch(feed, run_id, &rt_cfg).await;
        self.notify_workers();
        Ok(true)
    }

    /// Enqueue a manual article extraction retry/refresh and wake extraction workers.
    pub async fn trigger_manual_article_extraction(&self, article_id: i64) -> AppResult<bool> {
        let rt_cfg = self.load_runtime_config().await?;
        let queued =
            enqueue_manual_article_extraction(&self.pool, article_id, rt_cfg.fetch_max_attempts)
                .await?;
        if queued {
            self.notify_workers();
        }
        Ok(queued)
    }

    /// Process all currently eligible extraction jobs in the SQLite queue using
    /// bounded concurrency (`extraction_concurrency`).
    pub async fn drain_eligible_extractions(&self) -> AppResult<usize> {
        let rt_cfg = self.load_runtime_config().await?;
        let concurrency = rt_cfg.extraction_concurrency.clamp(1, 32);
        let mut total_processed = 0usize;

        loop {
            let mut join_set = JoinSet::new();
            for _ in 0..concurrency {
                let Some(job) = claim_next_extraction_job(&self.pool).await? else {
                    break;
                };
                let coord = self.clone();
                let cfg_clone = rt_cfg.clone();
                join_set.spawn(async move {
                    coord.execute_claimed_extraction_job(job, &cfg_clone).await;
                });
            }

            if join_set.is_empty() {
                break;
            }

            while let Some(res) = join_set.join_next().await {
                if res.is_ok() {
                    total_processed += 1;
                }
            }
        }

        Ok(total_processed)
    }

    /// Execute one scheduler tick: check if automatic fetching is enabled, claim up to
    /// `fetch_concurrency` due feeds, execute them concurrently, and drain eligible
    /// extraction jobs.
    pub async fn run_scheduler_tick(&self) -> AppResult<(usize, usize)> {
        let rt_cfg = self.load_runtime_config().await?;
        let mut feeds_executed = 0usize;

        if rt_cfg.fetch_enabled {
            let due_ids =
                list_due_feed_ids(&self.pool, rt_cfg.fetch_concurrency.clamp(1, 32) as i64).await?;

            if !due_ids.is_empty() {
                let mut join_set = JoinSet::new();
                for (idx, feed_id) in due_ids.into_iter().enumerate() {
                    let Some((feed, run_id)) =
                        try_claim_feed_fetch(&self.pool, feed_id, "scheduled").await?
                    else {
                        continue;
                    };
                    let coord = self.clone();
                    let cfg_clone = rt_cfg.clone();
                    join_set.spawn(async move {
                        // Stagger concurrent startup bursts slightly (50ms per slot)
                        if idx > 0 {
                            tokio::time::sleep(Duration::from_millis((idx as u64) * 50)).await;
                        }
                        coord
                            .execute_claimed_feed_fetch(feed, run_id, &cfg_clone)
                            .await;
                    });
                }

                while let Some(res) = join_set.join_next().await {
                    if res.is_ok() {
                        feeds_executed += 1;
                    }
                }
            }
        }

        let extractions_executed = self.drain_eligible_extractions().await?;
        Ok((feeds_executed, extractions_executed))
    }

    /// Spawn the long-running background scheduler and extraction worker loop.
    pub fn spawn_background_loop(&self, poll_interval: Duration) -> tokio::task::JoinHandle<()> {
        let coord = self.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            info!(
                poll_interval_ms = poll_interval.as_millis() as u64,
                "scheduler_started"
            );

            loop {
                if *shutdown_rx.borrow() {
                    break;
                }

                if let Err(err) = coord.run_scheduler_tick().await {
                    warn!(error = ?err, "Scheduler tick encountered an error");
                }

                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = coord.wake_notify.notified() => {}
                    _ = tokio::time::sleep(poll_interval) => {}
                }
            }

            info!("scheduler_stopped");
        })
    }

    pub async fn execute_claimed_feed_fetch(
        &self,
        feed: Feed,
        run_id: i64,
        rt_cfg: &RuntimeIngestionConfig,
    ) {
        let started = Instant::now();
        info!(
            feed_id = feed.id,
            run_id,
            feed_url = %feed.feed_url,
            "feed_fetch_started"
        );

        let options = FetchRequestOptions {
            connect_timeout: Duration::from_secs(rt_cfg.connect_timeout_seconds),
            request_timeout: Duration::from_secs(rt_cfg.feed_request_timeout_seconds),
            max_response_bytes: rt_cfg.max_feed_response_bytes,
            max_redirects: rt_cfg.max_redirects,
            max_attempts: rt_cfg.fetch_max_attempts,
            retry_initial_backoff: Duration::from_secs(rt_cfg.fetch_retry_initial_seconds),
            retry_max_backoff: Duration::from_secs(rt_cfg.fetch_retry_max_seconds),
            etag: feed.etag.clone(),
            last_modified: feed.last_modified.clone(),
            accept_header: "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        };

        let fetch_res = fetch_url_with_retries(&feed.feed_url, &options, &self.resolver).await;

        match fetch_res {
            Ok(FetchOutcome::NotModified {
                http_status,
                etag,
                last_modified,
                duration_ms,
                ..
            }) => {
                info!(
                    feed_id = feed.id,
                    run_id, http_status, duration_ms, "feed_fetch_completed (unchanged 304)"
                );
                let _ = complete_feed_fetch_run(
                    &self.pool,
                    CompleteFeedRunUpdate {
                        feed_id: feed.id,
                        run_id,
                        status: "unchanged",
                        http_status: Some(http_status),
                        duration_ms,
                        etag,
                        last_modified,
                        metadata: None,
                        entries_seen: 0,
                        entries_skipped: 0,
                        entries_truncated: 0,
                        articles_inserted: 0,
                        articles_updated: 0,
                        articles_unchanged: 0,
                        extraction_queued: 0,
                        error_category: None,
                        error_message: None,
                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                        retention_limit: rt_cfg.fetch_history_retention,
                    },
                )
                .await;
            }
            Ok(FetchOutcome::Fetched {
                http_status,
                final_url,
                bytes,
                content_type,
                etag,
                last_modified,
                duration_ms,
            }) => {
                let effective_max_entries = feed
                    .max_entries_override
                    .and_then(|v| usize::try_from(v).ok())
                    .unwrap_or(rt_cfg.max_feed_entries);

                match parse_feed_bytes(
                    &bytes,
                    content_type.as_deref(),
                    &final_url,
                    effective_max_entries,
                ) {
                    Ok(parsed) => {
                        match persist_discovered_entries(
                            &self.pool,
                            feed.id,
                            &parsed.entries,
                            rt_cfg.fetch_max_attempts,
                        )
                        .await
                        {
                            Ok(stats) => {
                                let total_duration = started.elapsed().as_millis() as u64;
                                info!(
                                    feed_id = feed.id,
                                    run_id,
                                    http_status,
                                    duration_ms = total_duration,
                                    entries_seen = parsed.entries_seen,
                                    articles_inserted = stats.inserted,
                                    articles_updated = stats.updated,
                                    articles_unchanged = stats.unchanged,
                                    extraction_queued = stats.extraction_queued,
                                    "feed_fetch_completed"
                                );
                                let _ = complete_feed_fetch_run(
                                    &self.pool,
                                    CompleteFeedRunUpdate {
                                        feed_id: feed.id,
                                        run_id,
                                        status: "success",
                                        http_status: Some(http_status),
                                        duration_ms: total_duration.max(duration_ms),
                                        etag,
                                        last_modified,
                                        metadata: Some(parsed.metadata),
                                        entries_seen: parsed.entries_seen,
                                        entries_skipped: parsed.entries_skipped,
                                        entries_truncated: parsed.entries_truncated,
                                        articles_inserted: stats.inserted,
                                        articles_updated: stats.updated,
                                        articles_unchanged: stats.unchanged,
                                        extraction_queued: stats.extraction_queued,
                                        error_category: None,
                                        error_message: None,
                                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                        retention_limit: rt_cfg.fetch_history_retention,
                                    },
                                )
                                .await;
                            }
                            Err(db_err) => {
                                let msg = match db_err {
                                    AppError::Validation(m) => m,
                                    _ => "Database error while persisting discovered articles."
                                        .to_string(),
                                };
                                warn!(
                                    feed_id = feed.id,
                                    run_id,
                                    error_category = "database",
                                    "feed_fetch_failed"
                                );
                                let _ = complete_feed_fetch_run(
                                    &self.pool,
                                    CompleteFeedRunUpdate {
                                        feed_id: feed.id,
                                        run_id,
                                        status: "failed",
                                        http_status: Some(http_status),
                                        duration_ms: started.elapsed().as_millis() as u64,
                                        etag: None,
                                        last_modified: None,
                                        metadata: None,
                                        entries_seen: parsed.entries_seen,
                                        entries_skipped: parsed.entries_skipped,
                                        entries_truncated: parsed.entries_truncated,
                                        articles_inserted: 0,
                                        articles_updated: 0,
                                        articles_unchanged: 0,
                                        extraction_queued: 0,
                                        error_category: Some("database".to_string()),
                                        error_message: Some(msg),
                                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                        retention_limit: rt_cfg.fetch_history_retention,
                                    },
                                )
                                .await;
                            }
                        }
                    }
                    Err(parse_err) => {
                        let msg = parse_err.to_string();
                        warn!(
                            feed_id = feed.id,
                            run_id,
                            http_status,
                            error_category = "parse",
                            error = %msg,
                            "feed_parse_failed"
                        );
                        let _ = complete_feed_fetch_run(
                            &self.pool,
                            CompleteFeedRunUpdate {
                                feed_id: feed.id,
                                run_id,
                                status: "failed",
                                http_status: Some(http_status),
                                duration_ms: started.elapsed().as_millis() as u64,
                                etag: None,
                                last_modified: None,
                                metadata: None,
                                entries_seen: 0,
                                entries_skipped: 0,
                                entries_truncated: 0,
                                articles_inserted: 0,
                                articles_updated: 0,
                                articles_unchanged: 0,
                                extraction_queued: 0,
                                error_category: Some("parse".to_string()),
                                error_message: Some(msg),
                                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                retention_limit: rt_cfg.fetch_history_retention,
                            },
                        )
                        .await;
                    }
                }
            }
            Err(fetch_err) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                warn!(
                    feed_id = feed.id,
                    run_id,
                    http_status = ?fetch_err.http_status,
                    duration_ms,
                    error_category = fetch_err.category.as_str(),
                    error = %fetch_err.message,
                    "feed_fetch_failed"
                );
                let _ = complete_feed_fetch_run(
                    &self.pool,
                    CompleteFeedRunUpdate {
                        feed_id: feed.id,
                        run_id,
                        status: "failed",
                        http_status: fetch_err.http_status,
                        duration_ms,
                        etag: None,
                        last_modified: None,
                        metadata: None,
                        entries_seen: 0,
                        entries_skipped: 0,
                        entries_truncated: 0,
                        articles_inserted: 0,
                        articles_updated: 0,
                        articles_unchanged: 0,
                        extraction_queued: 0,
                        error_category: Some(fetch_err.category.as_str().to_string()),
                        error_message: Some(fetch_err.message),
                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                        retention_limit: rt_cfg.fetch_history_retention,
                    },
                )
                .await;
            }
        }
    }

    pub async fn execute_claimed_extraction_job(
        &self,
        job: ClaimedExtractionJob,
        rt_cfg: &RuntimeIngestionConfig,
    ) {
        let started = Instant::now();
        info!(
            article_id = job.article_id,
            job_id = job.job_id,
            attempt = job.attempts,
            "article_extraction_started"
        );

        let options = articles::extractor::default_article_fetch_options(
            rt_cfg.connect_timeout_seconds,
            rt_cfg.article_request_timeout_seconds,
            rt_cfg.max_article_response_bytes,
            rt_cfg.fetch_max_attempts,
            rt_cfg.fetch_retry_initial_seconds,
            rt_cfg.fetch_retry_max_seconds,
        );

        let outcome = fetch_and_extract_article(
            &job.canonical_url,
            job.description.as_deref(),
            &options,
            &self.resolver,
        )
        .await;

        let duration_ms = started.elapsed().as_millis() as u64;
        match &outcome {
            ExtractionResult::Completed { .. } => {
                info!(
                    article_id = job.article_id,
                    job_id = job.job_id,
                    duration_ms,
                    status = "completed",
                    "article_extraction_completed"
                );
            }
            ExtractionResult::Fallback { reason, .. } => {
                info!(
                    article_id = job.article_id,
                    job_id = job.job_id,
                    duration_ms,
                    status = "fallback",
                    reason = %reason,
                    "article_extraction_completed"
                );
            }
            ExtractionResult::Failed { error_message, .. } => {
                warn!(
                    article_id = job.article_id,
                    job_id = job.job_id,
                    duration_ms,
                    error = %error_message,
                    "article_extraction_failed"
                );
            }
        }

        if let Err(err) = complete_extraction_job(&self.pool, &job, &outcome).await {
            error!(
                article_id = job.article_id,
                job_id = job.job_id,
                error = ?err,
                "Failed to persist article extraction outcome"
            );
        }
    }
}
