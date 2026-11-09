use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tracing::{error, info, warn};

use crate::articles::extractor::{ExtractionResult, fetch_and_extract_article_cancellable};
use crate::articles::{
    self, ClaimedExtractionJob, claim_next_extraction_job, complete_extraction_job,
    enqueue_manual_article_extraction, recover_interrupted_extractions, release_extraction_job,
};
use crate::config::{AppConfig, RuntimeIngestionConfig};
use crate::db;
use crate::error::{AppError, AppResult};
use crate::feeds::fetcher::{
    FetchErrorCategory, FetchOutcome, FetchRequestOptions, fetch_url_with_retries_cancellable,
};
use crate::feeds::parser::parse_feed_bytes_async;
use crate::feeds::validation::{HostResolver, default_resolver};
use crate::feeds::{
    CompleteFeedRunUpdate, Feed, claim_next_due_feed_fetch, complete_feed_fetch_run,
    get_feed_by_id, mark_all_enabled_feeds_manual_fetch_due, mark_feed_manual_fetch_due,
    recover_interrupted_fetches, release_feed_fetch_run, try_claim_feed_fetch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualTriggerOutcome {
    Started { run_id: i64 },
    AlreadyRunning,
}

/// Process-level single-instance lock held on `{QUIRE_DATA_DIR}/quire.lock`
/// for the lifetime of the process (Revision 2 §2.1).
pub struct InstanceLock {
    _file: File,
}

/// Acquire an exclusive non-blocking OS lock (`std::fs::File::try_lock`) on
/// `{data_dir}/quire.lock`. Returns an error if another Quire instance holds
/// the lock. The lock is released automatically by the OS when the `InstanceLock`
/// is dropped or the process exits (including on `SIGKILL`).
pub fn acquire_instance_lock(data_dir: &Path) -> AppResult<InstanceLock> {
    std::fs::create_dir_all(data_dir).map_err(|e| {
        AppError::Config(format!(
            "Failed to create data directory {}: {e}",
            data_dir.display()
        ))
    })?;
    let lock_path = data_dir.join("quire.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| {
            AppError::Config(format!(
                "Failed to open instance lock file {}: {e}",
                lock_path.display()
            ))
        })?;

    file.try_lock().map_err(|_| {
        AppError::Config(format!(
            "Another Quire instance is using {}",
            data_dir.display()
        ))
    })?;

    Ok(InstanceLock { _file: file })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskEndReason {
    CompletedNormally,
    DeadlineExceeded,
    CancelledByShutdown,
    CompletionWriteFailed,
}

#[derive(Debug, Clone)]
struct ActiveFeedMeta {
    feed_id: i64,
    run_id: i64,
    default_interval_minutes: u32,
}

#[derive(Debug, Clone)]
struct ActiveExtractionMeta {
    job_id: i64,
    article_id: i64,
    claim_token: String,
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
    /// run before starting background dispatchers.
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

    /// Wake both background dispatchers immediately (for example, after settings
    /// changes or durable manual job queueing).
    pub fn notify_workers(&self) {
        self.wake_notify.notify_waiters();
    }

    /// Signal background dispatchers and in-flight HTTP operations to stop.
    pub fn signal_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        self.wake_notify.notify_waiters();
    }

    /// Subscribe to the coordinator's cancellation signal.
    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    /// Load current effective runtime ingestion settings from `AppConfig` + SQLite `settings`.
    pub async fn load_runtime_config(&self) -> AppResult<RuntimeIngestionConfig> {
        let db_settings = db::get_all_settings(&self.pool).await?;
        Ok(self.config.resolve_runtime_ingestion(&db_settings))
    }

    /// Durable manual feed fetch request (Revision 2 §2.2 / QUA-003):
    /// Marks the feed due (`next_scheduled_fetch_at = now`, `manual_requested_at = now`)
    /// in SQLite and wakes the coordinator-owned feed dispatcher. Never spawns a
    /// detached task from the HTTP handler.
    pub async fn trigger_manual_feed_fetch(&self, feed_id: i64) -> AppResult<ManualTriggerOutcome> {
        let marked = mark_feed_manual_fetch_due(&self.pool, feed_id).await?;
        if !marked {
            return Ok(ManualTriggerOutcome::AlreadyRunning);
        }
        self.notify_workers();
        Ok(ManualTriggerOutcome::Started { run_id: 0 })
    }

    /// Durable manual fetch-all request for all enabled feeds not currently running
    /// (Revision 2 §2.2 / QUA-003). Marks all eligible feeds due in SQLite and wakes
    /// the feed dispatcher so they execute under `fetch_concurrency`.
    pub async fn trigger_manual_fetch_all_enabled(&self) -> AppResult<usize> {
        let marked = mark_all_enabled_feeds_manual_fetch_due(&self.pool).await?;
        if marked > 0 {
            self.notify_workers();
        }
        Ok(marked)
    }

    /// Execute a single feed fetch and persist discovered articles + extraction jobs
    /// synchronously.
    pub async fn run_feed_now_and_wait(&self, feed_id: i64, trigger_type: &str) -> AppResult<bool> {
        let _ = get_feed_by_id(&self.pool, feed_id).await?;
        let Some((feed, run_id)) = try_claim_feed_fetch(&self.pool, feed_id, trigger_type).await?
        else {
            return Ok(false);
        };
        let rt_cfg = self.load_runtime_config().await?;
        let _ = self.execute_claimed_feed_fetch(feed, run_id, &rt_cfg).await;
        self.notify_workers();
        Ok(true)
    }

    /// Enqueue a manual article extraction retry/refresh and wake the extraction dispatcher.
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

    /// Process eligible extraction jobs in the SQLite queue using a continuous
    /// worker pool (`active.len() < limit`, refilling as each task finishes)
    /// until no currently eligible jobs remain.
    pub async fn drain_eligible_extractions(&self) -> AppResult<usize> {
        let mut active: JoinSet<(ActiveExtractionMeta, TaskEndReason)> = JoinSet::new();
        let mut active_meta: HashMap<tokio::task::Id, ActiveExtractionMeta> = HashMap::new();
        let mut total_processed = 0usize;

        loop {
            let rt_cfg = self.load_runtime_config().await?;
            let limit = rt_cfg.extraction_concurrency.clamp(1, 32);

            while active.len() < limit {
                let Some(job) = claim_next_extraction_job(&self.pool).await? else {
                    break;
                };
                let meta = ActiveExtractionMeta {
                    job_id: job.job_id,
                    article_id: job.article_id,
                    claim_token: job.claim_token.clone(),
                };
                let meta_for_map = meta.clone();
                let coord = self.clone();
                let cfg_clone = rt_cfg.clone();
                let abort_handle: AbortHandle = active.spawn(async move {
                    let reason = coord.run_extraction_with_deadline(job, &cfg_clone).await;
                    (meta, reason)
                });
                active_meta.insert(abort_handle.id(), meta_for_map);
            }

            if active.is_empty() {
                break;
            }

            if let Some(join_res) = active.join_next_with_id().await {
                total_processed += 1;
                self.handle_finished_extraction(join_res, &mut active_meta)
                    .await;
            }
        }

        Ok(total_processed)
    }

    /// Execute one synchronous scheduler pass for deterministic integration tests:
    /// claims and runs up to `fetch_concurrency` due feeds, then drains eligible
    /// extraction jobs.
    pub async fn run_scheduler_tick(&self) -> AppResult<(usize, usize)> {
        let rt_cfg = self.load_runtime_config().await?;
        let limit = rt_cfg.fetch_concurrency.clamp(1, 32);
        let mut feeds_executed = 0usize;

        let mut active: JoinSet<(ActiveFeedMeta, TaskEndReason)> = JoinSet::new();
        let mut active_meta: HashMap<tokio::task::Id, ActiveFeedMeta> = HashMap::new();

        while active.len() < limit {
            let Some((feed, run_id)) =
                claim_next_due_feed_fetch(&self.pool, rt_cfg.fetch_enabled).await?
            else {
                break;
            };
            let meta = ActiveFeedMeta {
                feed_id: feed.id,
                run_id,
                default_interval_minutes: rt_cfg.fetch_interval_minutes,
            };
            let meta_for_map = meta.clone();
            let coord = self.clone();
            let cfg_clone = rt_cfg.clone();
            let abort_handle = active.spawn(async move {
                let reason = coord.run_feed_with_deadline(feed, run_id, &cfg_clone).await;
                (meta, reason)
            });
            active_meta.insert(abort_handle.id(), meta_for_map);
        }

        while let Some(join_res) = active.join_next_with_id().await {
            feeds_executed += 1;
            self.handle_finished_feed(join_res, &mut active_meta).await;
        }

        let extractions_executed = self.drain_eligible_extractions().await?;
        Ok((feeds_executed, extractions_executed))
    }

    /// Spawn the two independent, coordinator-owned dispatchers (Revision 2 §2.3):
    /// 1. Feed dispatcher loop
    /// 2. Extraction dispatcher loop
    ///
    /// Both dispatchers poll every `poll_interval` (default 5s) or wake immediately
    /// on `notify_workers()`, re-read runtime concurrency limits before every claim,
    /// enforce hard deadlines, observe panics, and release in-flight items on
    /// graceful shutdown.
    pub fn spawn_background_loop(&self, poll_interval: Duration) -> JoinHandle<bool> {
        let feed_coord = self.clone();
        let extract_coord = self.clone();
        let poll = poll_interval.min(Duration::from_secs(5));

        tokio::spawn(async move {
            info!(
                poll_interval_ms = poll.as_millis() as u64,
                "scheduler_started"
            );

            let feed_handle =
                tokio::spawn(async move { feed_coord.run_feed_dispatcher_loop(poll).await });
            let extract_handle =
                tokio::spawn(
                    async move { extract_coord.run_extraction_dispatcher_loop(poll).await },
                );

            let (f_res, e_res) = tokio::join!(feed_handle, extract_handle);
            let f_clean = f_res.unwrap_or(false);
            let e_clean = e_res.unwrap_or(false);
            let clean = f_clean && e_clean;

            if clean {
                info!("scheduler_stopped");
            } else {
                warn!("scheduler_stopped_with_unfinished_work");
            }
            clean
        })
    }

    async fn run_feed_dispatcher_loop(&self, poll_interval: Duration) -> bool {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut active: JoinSet<(ActiveFeedMeta, TaskEndReason)> = JoinSet::new();
        let mut active_meta: HashMap<tokio::task::Id, ActiveFeedMeta> = HashMap::new();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if let Ok(rt_cfg) = self.load_runtime_config().await {
                let limit = rt_cfg.fetch_concurrency.clamp(1, 32);
                while active.len() < limit {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match claim_next_due_feed_fetch(&self.pool, rt_cfg.fetch_enabled).await {
                        Ok(Some((feed, run_id))) => {
                            let meta = ActiveFeedMeta {
                                feed_id: feed.id,
                                run_id,
                                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                            };
                            let meta_for_map = meta.clone();
                            let coord = self.clone();
                            let cfg_clone = rt_cfg.clone();
                            let abort_handle = active.spawn(async move {
                                let reason =
                                    coord.run_feed_with_deadline(feed, run_id, &cfg_clone).await;
                                (meta, reason)
                            });
                            active_meta.insert(abort_handle.id(), meta_for_map);
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = ?err, "Feed dispatcher failed to claim due feed");
                            break;
                        }
                    }
                }
            }

            tokio::select! {
                Some(join_res) = active.join_next_with_id(), if !active.is_empty() => {
                    self.handle_finished_feed(join_res, &mut active_meta).await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = self.wake_notify.notified() => {}
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }

        // Graceful shutdown: wait up to 5s for cancelled in-flight feed tasks to release
        let shutdown_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !active.is_empty() {
            match tokio::time::timeout_at(shutdown_deadline, active.join_next_with_id()).await {
                Ok(Some(join_res)) => {
                    self.handle_finished_feed(join_res, &mut active_meta).await;
                }
                Ok(None) => break,
                Err(_) => {
                    active.abort_all();
                    for (_, meta) in active_meta.drain() {
                        let _ = release_feed_fetch_run(
                            &self.pool,
                            meta.feed_id,
                            meta.run_id,
                            "cancelled",
                            "cancelled",
                            "Cancelled during application shutdown.",
                            meta.default_interval_minutes,
                        )
                        .await;
                    }
                    return false;
                }
            }
        }
        true
    }

    async fn run_extraction_dispatcher_loop(&self, poll_interval: Duration) -> bool {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut active: JoinSet<(ActiveExtractionMeta, TaskEndReason)> = JoinSet::new();
        let mut active_meta: HashMap<tokio::task::Id, ActiveExtractionMeta> = HashMap::new();

        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            if let Ok(rt_cfg) = self.load_runtime_config().await {
                let limit = rt_cfg.extraction_concurrency.clamp(1, 32);
                while active.len() < limit {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    match claim_next_extraction_job(&self.pool).await {
                        Ok(Some(job)) => {
                            let meta = ActiveExtractionMeta {
                                job_id: job.job_id,
                                article_id: job.article_id,
                                claim_token: job.claim_token.clone(),
                            };
                            let meta_for_map = meta.clone();
                            let coord = self.clone();
                            let cfg_clone = rt_cfg.clone();
                            let abort_handle = active.spawn(async move {
                                let reason =
                                    coord.run_extraction_with_deadline(job, &cfg_clone).await;
                                (meta, reason)
                            });
                            active_meta.insert(abort_handle.id(), meta_for_map);
                        }
                        Ok(None) => break,
                        Err(err) => {
                            warn!(error = ?err, "Extraction dispatcher failed to claim job");
                            break;
                        }
                    }
                }
            }

            tokio::select! {
                Some(join_res) = active.join_next_with_id(), if !active.is_empty() => {
                    self.handle_finished_extraction(join_res, &mut active_meta).await;
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                _ = self.wake_notify.notified() => {}
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }

        // Graceful shutdown: wait up to 5s for cancelled in-flight extraction tasks to release
        let shutdown_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !active.is_empty() {
            match tokio::time::timeout_at(shutdown_deadline, active.join_next_with_id()).await {
                Ok(Some(join_res)) => {
                    self.handle_finished_extraction(join_res, &mut active_meta)
                        .await;
                }
                Ok(None) => break,
                Err(_) => {
                    active.abort_all();
                    for (_, meta) in active_meta.drain() {
                        let _ = release_extraction_job(
                            &self.pool,
                            meta.job_id,
                            meta.article_id,
                            &meta.claim_token,
                            true,
                            Some("Cancelled during application shutdown."),
                        )
                        .await;
                    }
                    return false;
                }
            }
        }
        true
    }

    async fn handle_finished_feed(
        &self,
        join_res: Result<
            (tokio::task::Id, (ActiveFeedMeta, TaskEndReason)),
            tokio::task::JoinError,
        >,
        active_meta: &mut HashMap<tokio::task::Id, ActiveFeedMeta>,
    ) {
        match join_res {
            Ok((task_id, (meta, reason))) => {
                active_meta.remove(&task_id);
                match reason {
                    TaskEndReason::CompletedNormally => {}
                    TaskEndReason::CancelledByShutdown => {
                        let _ = release_feed_fetch_run(
                            &self.pool,
                            meta.feed_id,
                            meta.run_id,
                            "cancelled",
                            "cancelled",
                            "Cancelled during application shutdown.",
                            meta.default_interval_minutes,
                        )
                        .await;
                    }
                    TaskEndReason::DeadlineExceeded => {
                        warn!(
                            feed_id = meta.feed_id,
                            run_id = meta.run_id,
                            "Feed fetch exceeded hard execution deadline; releasing run"
                        );
                        let _ = release_feed_fetch_run(
                            &self.pool,
                            meta.feed_id,
                            meta.run_id,
                            "failed",
                            "timeout",
                            "Feed fetch exceeded hard execution deadline.",
                            meta.default_interval_minutes,
                        )
                        .await;
                    }
                    TaskEndReason::CompletionWriteFailed => {
                        let _ = release_feed_fetch_run(
                            &self.pool,
                            meta.feed_id,
                            meta.run_id,
                            "failed",
                            "database",
                            "Failed to persist feed fetch completion.",
                            meta.default_interval_minutes,
                        )
                        .await;
                    }
                }
            }
            Err(join_err) => {
                let task_id = join_err.id();
                error!(task_id = %task_id, error = ?join_err, "Feed worker task panicked or was aborted");
                if let Some(meta) = active_meta.remove(&task_id) {
                    let _ = release_feed_fetch_run(
                        &self.pool,
                        meta.feed_id,
                        meta.run_id,
                        "failed",
                        "internal",
                        "Feed worker panicked during execution.",
                        meta.default_interval_minutes,
                    )
                    .await;
                }
            }
        }
    }

    async fn handle_finished_extraction(
        &self,
        join_res: Result<
            (tokio::task::Id, (ActiveExtractionMeta, TaskEndReason)),
            tokio::task::JoinError,
        >,
        active_meta: &mut HashMap<tokio::task::Id, ActiveExtractionMeta>,
    ) {
        match join_res {
            Ok((task_id, (meta, reason))) => {
                active_meta.remove(&task_id);
                match reason {
                    TaskEndReason::CompletedNormally => {}
                    TaskEndReason::CancelledByShutdown => {
                        let _ = release_extraction_job(
                            &self.pool,
                            meta.job_id,
                            meta.article_id,
                            &meta.claim_token,
                            true,
                            None,
                        )
                        .await;
                    }
                    TaskEndReason::DeadlineExceeded => {
                        warn!(
                            job_id = meta.job_id,
                            article_id = meta.article_id,
                            "Article extraction exceeded hard execution deadline; releasing job"
                        );
                        let _ = release_extraction_job(
                            &self.pool,
                            meta.job_id,
                            meta.article_id,
                            &meta.claim_token,
                            false,
                            Some("deadline exceeded"),
                        )
                        .await;
                    }
                    TaskEndReason::CompletionWriteFailed => {
                        let _ = release_extraction_job(
                            &self.pool,
                            meta.job_id,
                            meta.article_id,
                            &meta.claim_token,
                            false,
                            Some("Database error while saving extraction outcome."),
                        )
                        .await;
                    }
                }
            }
            Err(join_err) => {
                let task_id = join_err.id();
                error!(task_id = %task_id, error = ?join_err, "Article extraction worker task panicked or was aborted");
                if let Some(meta) = active_meta.remove(&task_id)
                    && meta.job_id > 0
                {
                    let _ = release_extraction_job(
                        &self.pool,
                        meta.job_id,
                        meta.article_id,
                        &meta.claim_token,
                        false,
                        Some("Extraction worker panicked."),
                    )
                    .await;
                }
            }
        }
    }

    async fn run_feed_with_deadline(
        &self,
        feed: Feed,
        run_id: i64,
        rt_cfg: &RuntimeIngestionConfig,
    ) -> TaskEndReason {
        let attempts = u64::from(rt_cfg.fetch_max_attempts.max(1));
        let deadline_secs = rt_cfg
            .feed_request_timeout_seconds
            .saturating_mul(attempts)
            .saturating_add(
                rt_cfg
                    .fetch_retry_max_seconds
                    .saturating_mul(attempts.saturating_sub(1)),
            )
            .saturating_add(30);

        match tokio::time::timeout(
            Duration::from_secs(deadline_secs),
            self.execute_claimed_feed_fetch_inner(feed, run_id, rt_cfg),
        )
        .await
        {
            Ok(reason) => reason,
            Err(_) => TaskEndReason::DeadlineExceeded,
        }
    }

    async fn run_extraction_with_deadline(
        &self,
        job: ClaimedExtractionJob,
        rt_cfg: &RuntimeIngestionConfig,
    ) -> TaskEndReason {
        let http_attempts = u64::from(rt_cfg.fetch_max_attempts.clamp(1, 2));
        let deadline_secs = rt_cfg
            .article_request_timeout_seconds
            .saturating_mul(http_attempts)
            .saturating_add(30);

        match tokio::time::timeout(
            Duration::from_secs(deadline_secs),
            self.execute_claimed_extraction_job_inner(job, rt_cfg),
        )
        .await
        {
            Ok(reason) => reason,
            Err(_) => TaskEndReason::DeadlineExceeded,
        }
    }

    pub async fn execute_claimed_feed_fetch(
        &self,
        feed: Feed,
        run_id: i64,
        rt_cfg: &RuntimeIngestionConfig,
    ) {
        let meta = ActiveFeedMeta {
            feed_id: feed.id,
            run_id,
            default_interval_minutes: rt_cfg.fetch_interval_minutes,
        };
        let reason = self.run_feed_with_deadline(feed, run_id, rt_cfg).await;
        if reason != TaskEndReason::CompletedNormally {
            let _ = match reason {
                TaskEndReason::CancelledByShutdown => {
                    release_feed_fetch_run(
                        &self.pool,
                        meta.feed_id,
                        meta.run_id,
                        "cancelled",
                        "cancelled",
                        "Cancelled during application shutdown.",
                        meta.default_interval_minutes,
                    )
                    .await
                }
                TaskEndReason::DeadlineExceeded => {
                    release_feed_fetch_run(
                        &self.pool,
                        meta.feed_id,
                        meta.run_id,
                        "failed",
                        "timeout",
                        "Feed fetch exceeded hard execution deadline.",
                        meta.default_interval_minutes,
                    )
                    .await
                }
                TaskEndReason::CompletionWriteFailed => {
                    release_feed_fetch_run(
                        &self.pool,
                        meta.feed_id,
                        meta.run_id,
                        "failed",
                        "database",
                        "Failed to persist feed fetch completion.",
                        meta.default_interval_minutes,
                    )
                    .await
                }
                TaskEndReason::CompletedNormally => Ok(true),
            };
        }
    }

    async fn execute_claimed_feed_fetch_inner(
        &self,
        feed: Feed,
        run_id: i64,
        rt_cfg: &RuntimeIngestionConfig,
    ) -> TaskEndReason {
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
            retry_connect_only: false,
            etag: feed.etag.clone(),
            last_modified: feed.last_modified.clone(),
            accept_header: "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        };

        let cancel_rx = self.subscribe_shutdown();
        let fetch_res = fetch_url_with_retries_cancellable(
            &feed.feed_url,
            &options,
            &self.resolver,
            Some(cancel_rx),
        )
        .await;

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
                match complete_feed_fetch_run(
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
                        discovered_entries: Vec::new(),
                        max_extraction_attempts: rt_cfg.fetch_max_attempts,
                        entries_seen: 0,
                        entries_skipped: 0,
                        entries_truncated: 0,
                        articles_inserted: 0,
                        articles_updated: 0,
                        articles_unchanged: 0,
                        extraction_queued: 0,
                        error_category: None,
                        error_message: None,
                        retry_after: None,
                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                        retention_limit: rt_cfg.fetch_history_retention,
                    },
                )
                .await
                {
                    Ok(_) => TaskEndReason::CompletedNormally,
                    Err(err) => {
                        error!(feed_id = feed.id, run_id, error = ?err, "Failed to complete 304 feed run");
                        TaskEndReason::CompletionWriteFailed
                    }
                }
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

                match parse_feed_bytes_async(bytes, content_type, final_url, effective_max_entries)
                    .await
                {
                    Ok(parsed) => {
                        let total_duration = started.elapsed().as_millis() as u64;
                        let entries_seen = parsed.entries_seen;
                        let entries_skipped = parsed.entries_skipped;
                        let entries_truncated = parsed.entries_truncated;

                        match complete_feed_fetch_run(
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
                                discovered_entries: parsed.entries,
                                max_extraction_attempts: rt_cfg.fetch_max_attempts,
                                entries_seen,
                                entries_skipped,
                                entries_truncated,
                                articles_inserted: 0,
                                articles_updated: 0,
                                articles_unchanged: 0,
                                extraction_queued: 0,
                                error_category: None,
                                error_message: None,
                                retry_after: None,
                                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                retention_limit: rt_cfg.fetch_history_retention,
                            },
                        )
                        .await
                        {
                            Ok(_) => {
                                info!(
                                    feed_id = feed.id,
                                    run_id,
                                    http_status,
                                    duration_ms = total_duration,
                                    entries_seen,
                                    "feed_fetch_completed"
                                );
                                self.notify_workers();
                                TaskEndReason::CompletedNormally
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
                                        discovered_entries: Vec::new(),
                                        max_extraction_attempts: rt_cfg.fetch_max_attempts,
                                        entries_seen,
                                        entries_skipped,
                                        entries_truncated,
                                        articles_inserted: 0,
                                        articles_updated: 0,
                                        articles_unchanged: 0,
                                        extraction_queued: 0,
                                        error_category: Some("database".to_string()),
                                        error_message: Some(msg),
                                        retry_after: None,
                                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                        retention_limit: rt_cfg.fetch_history_retention,
                                    },
                                )
                                .await;
                                TaskEndReason::CompletionWriteFailed
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
                        match complete_feed_fetch_run(
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
                                discovered_entries: Vec::new(),
                                max_extraction_attempts: rt_cfg.fetch_max_attempts,
                                entries_seen: 0,
                                entries_skipped: 0,
                                entries_truncated: 0,
                                articles_inserted: 0,
                                articles_updated: 0,
                                articles_unchanged: 0,
                                extraction_queued: 0,
                                error_category: Some("parse".to_string()),
                                error_message: Some(msg),
                                retry_after: None,
                                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                                retention_limit: rt_cfg.fetch_history_retention,
                            },
                        )
                        .await
                        {
                            Ok(_) => TaskEndReason::CompletedNormally,
                            Err(_) => TaskEndReason::CompletionWriteFailed,
                        }
                    }
                }
            }
            Err(fetch_err) => {
                if fetch_err.category == FetchErrorCategory::Cancelled {
                    return TaskEndReason::CancelledByShutdown;
                }
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
                match complete_feed_fetch_run(
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
                        discovered_entries: Vec::new(),
                        max_extraction_attempts: rt_cfg.fetch_max_attempts,
                        entries_seen: 0,
                        entries_skipped: 0,
                        entries_truncated: 0,
                        articles_inserted: 0,
                        articles_updated: 0,
                        articles_unchanged: 0,
                        extraction_queued: 0,
                        error_category: Some(fetch_err.category.as_str().to_string()),
                        error_message: Some(fetch_err.message),
                        retry_after: fetch_err.retry_after,
                        default_interval_minutes: rt_cfg.fetch_interval_minutes,
                        retention_limit: rt_cfg.fetch_history_retention,
                    },
                )
                .await
                {
                    Ok(_) => TaskEndReason::CompletedNormally,
                    Err(_) => TaskEndReason::CompletionWriteFailed,
                }
            }
        }
    }

    pub async fn execute_claimed_extraction_job(
        &self,
        job: ClaimedExtractionJob,
        rt_cfg: &RuntimeIngestionConfig,
    ) {
        let meta = ActiveExtractionMeta {
            job_id: job.job_id,
            article_id: job.article_id,
            claim_token: job.claim_token.clone(),
        };
        let reason = self.run_extraction_with_deadline(job, rt_cfg).await;
        if reason != TaskEndReason::CompletedNormally {
            let revert = reason == TaskEndReason::CancelledByShutdown;
            let err_msg = match reason {
                TaskEndReason::DeadlineExceeded => Some("deadline exceeded"),
                TaskEndReason::CompletionWriteFailed => {
                    Some("Database error while saving extraction outcome.")
                }
                _ => None,
            };
            let _ = release_extraction_job(
                &self.pool,
                meta.job_id,
                meta.article_id,
                &meta.claim_token,
                revert,
                err_msg,
            )
            .await;
        }
    }

    async fn execute_claimed_extraction_job_inner(
        &self,
        job: ClaimedExtractionJob,
        rt_cfg: &RuntimeIngestionConfig,
    ) -> TaskEndReason {
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

        let cancel_rx = self.subscribe_shutdown();
        let outcome = fetch_and_extract_article_cancellable(
            &job.canonical_url,
            job.description.as_deref(),
            &options,
            &self.resolver,
            Some(cancel_rx),
        )
        .await;

        if let ExtractionResult::Failed {
            cancelled: true, ..
        } = &outcome
        {
            return TaskEndReason::CancelledByShutdown;
        }

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

        match complete_extraction_job(&self.pool, &job, &outcome).await {
            Ok(_) => TaskEndReason::CompletedNormally,
            Err(err) => {
                error!(
                    article_id = job.article_id,
                    job_id = job.job_id,
                    error = ?err,
                    "Failed to persist article extraction outcome"
                );
                TaskEndReason::CompletionWriteFailed
            }
        }
    }
}
