use std::sync::Arc;

use askama::Template;
use axum::extract::{Form, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tower_http::services::ServeDir;
use url::Url;

use crate::articles::sanitizer::sanitize_article_html;
use crate::articles::{
    self, ArticleDetail, ArticleFeedSourceRow, ArticleFilter, ArticleSummaryRow,
};
use crate::config::{
    AppConfig, ConfigSource, ResolvedSetting, format_timestamp_for_tz, parse_bool_setting,
    validate_concurrency, validate_fetch_interval_minutes, validate_max_attempts,
    validate_max_entries, validate_max_response_bytes, validate_timeout_seconds, validate_timezone,
};
use crate::db::{self, DashboardCounts, DbHealth};
use crate::editions::{self, EditionRow};
use crate::error::{AppError, AppResult};
use crate::feeds::scheduler::{IngestionCoordinator, ManualTriggerOutcome};
use crate::feeds::validation::HostResolver;
use crate::feeds::{self, Feed, FeedFetchRun, NewFeedInput, UpdateFeedInput};

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const CSRF_COOKIE_NAME: &str = "quire_csrf";

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    pub config: Arc<AppConfig>,
    pub coordinator: IngestionCoordinator,
}

impl AppState {
    pub fn new(db: SqlitePool, config: AppConfig) -> Self {
        let cfg_arc = Arc::new(config);
        let coordinator = IngestionCoordinator::new(db.clone(), cfg_arc.clone());
        Self {
            db,
            config: cfg_arc,
            coordinator,
        }
    }

    pub fn with_resolver(
        db: SqlitePool,
        config: AppConfig,
        resolver: Arc<dyn HostResolver>,
    ) -> Self {
        let cfg_arc = Arc::new(config);
        let coordinator =
            IngestionCoordinator::with_resolver(db.clone(), cfg_arc.clone(), resolver);
        Self {
            db,
            config: cfg_arc,
            coordinator,
        }
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard_page))
        .route("/health", get(health_handler))
        .route("/feeds", get(feeds_page).post(create_feed_handler))
        .route("/feeds/fetch-all", post(fetch_all_feeds_handler))
        .route(
            "/feeds/{id}",
            get(feed_detail_page).post(update_feed_handler),
        )
        .route("/feeds/{id}/edit", get(edit_feed_page))
        .route("/feeds/{id}/fetch", post(fetch_single_feed_handler))
        .route("/feeds/{id}/toggle", post(toggle_feed_handler))
        .route("/feeds/{id}/delete", post(delete_feed_handler))
        .route("/articles", get(articles_page))
        .route("/articles/{id}", get(article_detail_page))
        .route("/articles/{id}/extract", post(extract_article_handler))
        .route("/editions", get(editions_page))
        .route(
            "/settings",
            get(settings_page).post(update_settings_handler),
        )
        .nest_service("/static", ServeDir::new("static"))
        .with_state(state)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub application: String,
    pub version: String,
    pub database: HealthDatabaseStatus,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthDatabaseStatus {
    pub connected: bool,
    pub journal_mode: String,
    pub foreign_keys_enabled: bool,
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    match db::check_health(&state.db).await {
        Ok(health) if health.ok && health.foreign_keys_enabled => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok".to_string(),
                application: "quire".to_string(),
                version: APP_VERSION.to_string(),
                database: HealthDatabaseStatus {
                    connected: true,
                    journal_mode: health.journal_mode,
                    foreign_keys_enabled: health.foreign_keys_enabled,
                },
            }),
        ),
        Ok(health) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "degraded".to_string(),
                application: "quire".to_string(),
                version: APP_VERSION.to_string(),
                database: HealthDatabaseStatus {
                    connected: health.ok,
                    journal_mode: health.journal_mode,
                    foreign_keys_enabled: health.foreign_keys_enabled,
                },
            }),
        ),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unhealthy".to_string(),
                application: "quire".to_string(),
                version: APP_VERSION.to_string(),
                database: HealthDatabaseStatus {
                    connected: false,
                    journal_mode: "unknown".to_string(),
                    foreign_keys_enabled: false,
                },
            }),
        ),
    }
}

struct DashboardFailureView {
    feed_id: i64,
    feed_name: String,
    started_at_display: String,
    error_category: Option<String>,
    error_message: String,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    counts: DashboardCounts,
    db_health: DbHealth,
    database_url: String,
    timezone: String,
    fetch_enabled: bool,
    fetch_interval_minutes: u32,
    last_ingestion_display: String,
    recent_failures: Vec<DashboardFailureView>,
}

async fn dashboard_page(State(state): State<AppState>) -> AppResult<Response> {
    let counts = db::get_dashboard_counts(&state.db).await?;
    let db_health = db::check_health(&state.db).await?;
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let raw_failures = db::get_recent_ingestion_failures(&state.db, 10).await?;

    let last_ingestion_display = counts
        .last_ingestion_activity_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "No fetches recorded yet".to_string());

    let recent_failures = raw_failures
        .into_iter()
        .map(|r| DashboardFailureView {
            feed_id: r.feed_id,
            feed_name: r.feed_name,
            started_at_display: format_timestamp_for_tz(&r.started_at, &rt_cfg.timezone),
            error_category: r.error_category,
            error_message: r.error_message,
        })
        .collect();

    let tpl = DashboardTemplate {
        app_version: APP_VERSION,
        active_nav: "dashboard",
        counts,
        db_health,
        database_url: state.config.database_url.clone(),
        timezone: rt_cfg.timezone,
        fetch_enabled: rt_cfg.fetch_enabled,
        fetch_interval_minutes: rt_cfg.fetch_interval_minutes,
        last_ingestion_display,
        recent_failures,
    };
    render_html(tpl, None)
}

#[derive(Debug, Default, Deserialize)]
struct FlashQuery {
    msg: Option<String>,
}

struct FeedRowView {
    feed: Feed,
    last_attempted_display: String,
    last_successful_display: String,
    next_scheduled_display: String,
}

#[derive(Template)]
#[template(path = "feeds.html")]
struct FeedsTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feeds: Vec<FeedRowView>,
    any_fetch_running: bool,
    default_interval_minutes: u32,
    default_max_entries: usize,
    csrf_token: String,
    flash_message: Option<String>,
    error_message: Option<String>,
    form_name: String,
    form_feed_url: String,
    form_website_url: String,
    form_description: String,
    form_fetch_interval: String,
    form_max_entries: String,
    form_enabled: bool,
}

fn map_feed_views(feeds: Vec<Feed>, tz: &str) -> (Vec<FeedRowView>, bool) {
    let any_running = feeds.iter().any(|f| f.fetch_in_progress);
    let views = feeds
        .into_iter()
        .map(|f| {
            let last_attempted_display = f
                .last_attempted_fetch_at
                .as_deref()
                .map(|ts| format_timestamp_for_tz(ts, tz))
                .unwrap_or_else(|| "Never".to_string());
            let last_successful_display = f
                .last_successful_fetch_at
                .as_deref()
                .map(|ts| format_timestamp_for_tz(ts, tz))
                .unwrap_or_else(|| "Never".to_string());
            let next_scheduled_display = if !f.enabled {
                "Disabled".to_string()
            } else {
                f.next_scheduled_fetch_at
                    .as_deref()
                    .map(|ts| format_timestamp_for_tz(ts, tz))
                    .unwrap_or_else(|| "Due now".to_string())
            };
            FeedRowView {
                feed: f,
                last_attempted_display,
                last_successful_display,
                next_scheduled_display,
            }
        })
        .collect();
    (views, any_running)
}

async fn feeds_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FlashQuery>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let raw_feeds = feeds::list_feeds(&state.db).await?;
    let (feeds, any_fetch_running) = map_feed_views(raw_feeds, &rt_cfg.timezone);
    let flash_message = map_flash_code(query.msg.as_deref());

    let tpl = FeedsTemplate {
        app_version: APP_VERSION,
        active_nav: "feeds",
        feeds,
        any_fetch_running,
        default_interval_minutes: rt_cfg.fetch_interval_minutes,
        default_max_entries: rt_cfg.max_feed_entries,
        csrf_token,
        flash_message,
        error_message: None,
        form_name: String::new(),
        form_feed_url: String::new(),
        form_website_url: String::new(),
        form_description: String::new(),
        form_fetch_interval: String::new(),
        form_max_entries: String::new(),
        form_enabled: true,
    };
    render_html(tpl, set_cookie)
}

#[derive(Debug, Deserialize)]
pub struct FeedFormPayload {
    pub csrf_token: String,
    pub name: String,
    pub feed_url: String,
    #[serde(default)]
    pub website_url: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub fetch_interval_minutes: String,
    #[serde(default)]
    pub max_entries_override: String,
    #[serde(default)]
    pub enabled: Option<String>,
}

fn parse_optional_i64_field(raw: &str, label: &str) -> AppResult<Option<i64>> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = trimmed
        .parse::<i64>()
        .map_err(|_| AppError::Validation(format!("{label} must be a valid integer.")))?;
    Ok(Some(parsed))
}

async fn create_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<FeedFormPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;

    let enabled = form.enabled.as_deref() == Some("true") || form.enabled.as_deref() == Some("on");

    let interval_res = parse_optional_i64_field(&form.fetch_interval_minutes, "Fetch interval");
    let max_entries_res =
        parse_optional_i64_field(&form.max_entries_override, "Max entries override");

    let create_outcome = match (interval_res, max_entries_res) {
        (Ok(fetch_interval_minutes), Ok(max_entries_override)) => {
            let input = NewFeedInput {
                name: form.name.clone(),
                feed_url: form.feed_url.clone(),
                website_url: Some(form.website_url.clone()),
                description: Some(form.description.clone()),
                enabled,
                fetch_interval_minutes,
                max_entries_override,
            };
            feeds::create_feed(&state.db, input).await
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    };

    match create_outcome {
        Ok(_) => {
            state.coordinator.notify_workers();
            Ok(Redirect::to("/feeds?msg=created").into_response())
        }
        Err(AppError::Validation(err) | AppError::Conflict(err)) => {
            let rt_cfg = state.coordinator.load_runtime_config().await?;
            let raw_feeds = feeds::list_feeds(&state.db).await?;
            let (feeds_list, any_fetch_running) = map_feed_views(raw_feeds, &rt_cfg.timezone);
            let tpl = FeedsTemplate {
                app_version: APP_VERSION,
                active_nav: "feeds",
                feeds: feeds_list,
                any_fetch_running,
                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                default_max_entries: rt_cfg.max_feed_entries,
                csrf_token: form.csrf_token,
                flash_message: None,
                error_message: Some(err),
                form_name: form.name,
                form_feed_url: form.feed_url,
                form_website_url: form.website_url,
                form_description: form.description,
                form_fetch_interval: form.fetch_interval_minutes,
                form_max_entries: form.max_entries_override,
                form_enabled: enabled,
            };
            let mut resp = render_html(tpl, None)?;
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
        Err(other) => Err(other),
    }
}

struct FeedRunView {
    run: FeedFetchRun,
    started_at_display: String,
}

struct ArticleRowView {
    row: ArticleSummaryRow,
    published_display: String,
    discovered_display: String,
}

#[derive(Template)]
#[template(path = "feed_detail.html")]
struct FeedDetailTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feed: Feed,
    default_interval_minutes: u32,
    default_max_entries: usize,
    last_attempted_display: String,
    last_successful_display: String,
    next_scheduled_display: String,
    runs: Vec<FeedRunView>,
    recent_articles: Vec<ArticleRowView>,
    csrf_token: String,
    flash_message: Option<String>,
}

async fn feed_detail_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(query): Query<FlashQuery>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let feed = feeds::get_feed_by_id(&state.db, id).await?;
    let raw_runs = feeds::list_feed_fetch_runs(&state.db, id, 30).await?;
    let raw_articles = articles::list_articles_for_feed(&state.db, id, 25).await?;

    let last_attempted_display = feed
        .last_attempted_fetch_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "Never".to_string());
    let last_successful_display = feed
        .last_successful_fetch_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "Never".to_string());
    let next_scheduled_display = if !feed.enabled {
        "Disabled".to_string()
    } else {
        feed.next_scheduled_fetch_at
            .as_deref()
            .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
            .unwrap_or_else(|| "Due now".to_string())
    };

    let runs = raw_runs
        .into_iter()
        .map(|r| FeedRunView {
            started_at_display: format_timestamp_for_tz(&r.started_at, &rt_cfg.timezone),
            run: r,
        })
        .collect();

    let recent_articles = raw_articles
        .into_iter()
        .map(|a| {
            let pub_ts = a.published_at.as_deref().unwrap_or(&a.discovered_at);
            ArticleRowView {
                published_display: format_timestamp_for_tz(pub_ts, &rt_cfg.timezone),
                discovered_display: format_timestamp_for_tz(&a.discovered_at, &rt_cfg.timezone),
                row: a,
            }
        })
        .collect();

    let tpl = FeedDetailTemplate {
        app_version: APP_VERSION,
        active_nav: "feeds",
        feed,
        default_interval_minutes: rt_cfg.fetch_interval_minutes,
        default_max_entries: rt_cfg.max_feed_entries,
        last_attempted_display,
        last_successful_display,
        next_scheduled_display,
        runs,
        recent_articles,
        csrf_token,
        flash_message: map_flash_code(query.msg.as_deref()),
    };
    render_html(tpl, set_cookie)
}

#[derive(Template)]
#[template(path = "feed_edit.html")]
struct FeedEditTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feed: Feed,
    default_interval_minutes: u32,
    default_max_entries: usize,
    csrf_token: String,
    error_message: Option<String>,
    form_name: String,
    form_feed_url: String,
    form_website_url: String,
    form_description: String,
    form_fetch_interval: String,
    form_max_entries: String,
    form_enabled: bool,
}

async fn edit_feed_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let feed = feeds::get_feed_by_id(&state.db, id).await?;

    let tpl = FeedEditTemplate {
        app_version: APP_VERSION,
        active_nav: "feeds",
        form_name: feed.name.clone(),
        form_feed_url: feed.feed_url.clone(),
        form_website_url: feed.website_url.clone().unwrap_or_default(),
        form_description: feed.description.clone().unwrap_or_default(),
        form_fetch_interval: feed
            .fetch_interval_minutes
            .map(|v| v.to_string())
            .unwrap_or_default(),
        form_max_entries: feed
            .max_entries_override
            .map(|v| v.to_string())
            .unwrap_or_default(),
        form_enabled: feed.enabled,
        feed,
        default_interval_minutes: rt_cfg.fetch_interval_minutes,
        default_max_entries: rt_cfg.max_feed_entries,
        csrf_token,
        error_message: None,
    };
    render_html(tpl, set_cookie)
}

async fn update_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<FeedFormPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;

    let enabled = form.enabled.as_deref() == Some("true") || form.enabled.as_deref() == Some("on");
    let interval_res = parse_optional_i64_field(&form.fetch_interval_minutes, "Fetch interval");
    let max_entries_res =
        parse_optional_i64_field(&form.max_entries_override, "Max entries override");

    let update_outcome = match (interval_res, max_entries_res) {
        (Ok(fetch_interval_minutes), Ok(max_entries_override)) => {
            let input = UpdateFeedInput {
                name: form.name.clone(),
                feed_url: form.feed_url.clone(),
                website_url: Some(form.website_url.clone()),
                description: Some(form.description.clone()),
                enabled,
                fetch_interval_minutes,
                max_entries_override,
            };
            feeds::update_feed(&state.db, id, input).await
        }
        (Err(e), _) | (_, Err(e)) => Err(e),
    };

    match update_outcome {
        Ok(_) => {
            state.coordinator.notify_workers();
            Ok(Redirect::to("/feeds?msg=updated").into_response())
        }
        Err(AppError::Validation(err) | AppError::Conflict(err)) => {
            let rt_cfg = state.coordinator.load_runtime_config().await?;
            let feed = feeds::get_feed_by_id(&state.db, id).await?;
            let tpl = FeedEditTemplate {
                app_version: APP_VERSION,
                active_nav: "feeds",
                feed,
                default_interval_minutes: rt_cfg.fetch_interval_minutes,
                default_max_entries: rt_cfg.max_feed_entries,
                csrf_token: form.csrf_token,
                error_message: Some(err),
                form_name: form.name,
                form_feed_url: form.feed_url,
                form_website_url: form.website_url,
                form_description: form.description,
                form_fetch_interval: form.fetch_interval_minutes,
                form_max_entries: form.max_entries_override,
                form_enabled: enabled,
            };
            let mut resp = render_html(tpl, None)?;
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
        Err(other) => Err(other),
    }
}

#[derive(Debug, Deserialize)]
pub struct CsrfOnlyPayload {
    pub csrf_token: String,
    #[serde(default)]
    pub confirm: Option<String>,
}

async fn fetch_single_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    match state.coordinator.trigger_manual_feed_fetch(id).await? {
        ManualTriggerOutcome::Started { .. } => {
            Ok(Redirect::to(&format!("/feeds/{id}?msg=fetch_started")).into_response())
        }
        ManualTriggerOutcome::AlreadyRunning => {
            Ok(Redirect::to(&format!("/feeds/{id}?msg=fetch_already_running")).into_response())
        }
    }
}

async fn fetch_all_feeds_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    let _started = state.coordinator.trigger_manual_fetch_all_enabled().await?;
    Ok(Redirect::to("/feeds?msg=fetch_all_started").into_response())
}

async fn toggle_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    let existing = feeds::get_feed_by_id(&state.db, id).await?;
    feeds::set_feed_enabled(&state.db, id, !existing.enabled).await?;
    state.coordinator.notify_workers();
    Ok(Redirect::to("/feeds?msg=toggled").into_response())
}

async fn delete_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    if form.confirm.as_deref() != Some("yes") {
        return Err(AppError::Validation(
            "Feed deletion requires explicit confirmation.".to_string(),
        ));
    }
    feeds::delete_feed(&state.db, id).await?;
    Ok(Redirect::to("/feeds?msg=deleted").into_response())
}

#[derive(Debug, Default, Deserialize)]
struct ArticlesQuery {
    feed_id: Option<String>,
    status: Option<String>,
    published_date: Option<String>,
    discovered_date: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Template)]
#[template(path = "articles.html")]
struct ArticlesTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feeds: Vec<Feed>,
    articles: Vec<ArticleRowView>,
    total_count: i64,
    page: usize,
    total_pages: usize,
    has_prev: bool,
    has_next: bool,
    prev_page_url: String,
    next_page_url: String,
    selected_feed_id: i64,
    selected_status: String,
    selected_published_date: String,
    selected_discovered_date: String,
}

async fn articles_page(
    State(state): State<AppState>,
    Query(query): Query<ArticlesQuery>,
) -> AppResult<Response> {
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let feeds_list = feeds::list_feeds(&state.db).await?;

    let feed_id = query
        .feed_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .and_then(|s| s.parse::<i64>().ok());

    let selected_status = query
        .status
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let selected_published_date = query
        .published_date
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let selected_discovered_date = query
        .discovered_date
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();

    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(25).clamp(1, 100);

    let filter = ArticleFilter {
        feed_id,
        extraction_status: if selected_status.is_empty() {
            None
        } else {
            Some(selected_status.clone())
        },
        published_date: if selected_published_date.is_empty() {
            None
        } else {
            Some(selected_published_date.clone())
        },
        discovered_date: if selected_discovered_date.is_empty() {
            None
        } else {
            Some(selected_discovered_date.clone())
        },
        page,
        page_size,
    };

    let paged = articles::query_articles_paginated(&state.db, &filter).await?;

    let build_page_url = |target_page: usize| -> String {
        let mut ser = url::form_urlencoded::Serializer::new(String::new());
        if let Some(fid) = feed_id {
            ser.append_pair("feed_id", &fid.to_string());
        }
        if !selected_status.is_empty() {
            ser.append_pair("status", &selected_status);
        }
        if !selected_published_date.is_empty() {
            ser.append_pair("published_date", &selected_published_date);
        }
        if !selected_discovered_date.is_empty() {
            ser.append_pair("discovered_date", &selected_discovered_date);
        }
        ser.append_pair("page", &target_page.to_string());
        ser.append_pair("page_size", &page_size.to_string());
        format!("/articles?{}", ser.finish())
    };

    let articles = paged
        .items
        .into_iter()
        .map(|a| {
            let pub_ts = a.published_at.as_deref().unwrap_or(&a.discovered_at);
            ArticleRowView {
                published_display: format_timestamp_for_tz(pub_ts, &rt_cfg.timezone),
                discovered_display: format_timestamp_for_tz(&a.discovered_at, &rt_cfg.timezone),
                row: a,
            }
        })
        .collect();

    let tpl = ArticlesTemplate {
        app_version: APP_VERSION,
        active_nav: "articles",
        feeds: feeds_list,
        articles,
        total_count: paged.total_count,
        page: paged.page,
        total_pages: paged.total_pages,
        has_prev: paged.page > 1,
        has_next: paged.page < paged.total_pages,
        prev_page_url: build_page_url(paged.page.saturating_sub(1).max(1)),
        next_page_url: build_page_url(paged.page + 1),
        selected_feed_id: feed_id.unwrap_or(0),
        selected_status,
        selected_published_date,
        selected_discovered_date,
    };
    render_html(tpl, None)
}

#[derive(Template)]
#[template(path = "article_detail.html")]
struct ArticleDetailTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    article: ArticleDetail,
    sanitized_body_html: Option<String>,
    published_display: String,
    discovered_display: String,
    last_attempt_display: String,
    next_retry_display: String,
    sources: Vec<ArticleFeedSourceRow>,
    csrf_token: String,
    flash_message: Option<String>,
}

async fn article_detail_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(query): Query<FlashQuery>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let rt_cfg = state.coordinator.load_runtime_config().await?;
    let article = articles::get_article_by_id(&state.db, id).await?;
    let sources = articles::list_article_sources(&state.db, id).await?;

    let base_url = Url::parse(&article.canonical_url).ok();
    // Re-sanitize stored HTML at render time on the blocking pool (QUA-018)
    let sanitized_body_html = if let Some(raw_html) = article
        .extracted_content
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
    {
        let base_clone = base_url.clone();
        let cleaned = tokio::task::spawn_blocking(move || {
            sanitize_article_html(&raw_html, base_clone.as_ref())
        })
        .await
        .map_err(|e| AppError::Internal(anyhow::anyhow!("Sanitizer task panicked: {e}")))?;
        Some(cleaned)
    } else {
        None
    };

    let published_display = article
        .published_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "Not provided by feed".to_string());
    let discovered_display = format_timestamp_for_tz(&article.discovered_at, &rt_cfg.timezone);
    let last_attempt_display = article
        .last_extraction_attempt_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "Never".to_string());
    let next_retry_display = article
        .next_extraction_retry_at
        .as_deref()
        .map(|ts| format_timestamp_for_tz(ts, &rt_cfg.timezone))
        .unwrap_or_else(|| "Not scheduled".to_string());

    let tpl = ArticleDetailTemplate {
        app_version: APP_VERSION,
        active_nav: "articles",
        article,
        sanitized_body_html,
        published_display,
        discovered_display,
        last_attempt_display,
        next_retry_display,
        sources,
        csrf_token,
        flash_message: map_flash_code(query.msg.as_deref()),
    };
    render_html(tpl, set_cookie)
}

async fn extract_article_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    let queued = state
        .coordinator
        .trigger_manual_article_extraction(id)
        .await?;
    let msg = if queued {
        "extraction_queued"
    } else {
        "extraction_already_running"
    };
    Ok(Redirect::to(&format!("/articles/{id}?msg={msg}")).into_response())
}

#[derive(Template)]
#[template(path = "editions.html")]
struct EditionsTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    editions: Vec<EditionRow>,
}

async fn editions_page(State(state): State<AppState>) -> AppResult<Response> {
    let editions = editions::list_editions(&state.db, 50).await?;
    let tpl = EditionsTemplate {
        app_version: APP_VERSION,
        active_nav: "editions",
        editions,
    };
    render_html(tpl, None)
}

#[derive(Template)]
#[template(path = "settings.html")]
struct SettingsTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    settings: Vec<ResolvedSetting>,
    csrf_token: String,
    flash_message: Option<String>,
    error_message: Option<String>,
    form_timezone: String,
    timezone_locked: bool,
    form_fetch_enabled: String,
    fetch_enabled_locked: bool,
    form_fetch_interval_minutes: String,
    fetch_interval_locked: bool,
    form_fetch_concurrency: String,
    fetch_concurrency_locked: bool,
    form_extraction_concurrency: String,
    extraction_concurrency_locked: bool,
    form_feed_timeout: String,
    feed_timeout_locked: bool,
    form_article_timeout: String,
    article_timeout_locked: bool,
    form_max_feed_bytes: String,
    max_feed_bytes_locked: bool,
    form_max_article_bytes: String,
    max_article_bytes_locked: bool,
    form_max_feed_entries: String,
    max_feed_entries_locked: bool,
    form_fetch_max_attempts: String,
    fetch_max_attempts_locked: bool,
    form_ai_provider: String,
    ai_provider_locked: bool,
    form_ai_base_url: String,
    ai_base_url_locked: bool,
    form_ai_model: String,
    ai_model_locked: bool,
}

async fn settings_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FlashQuery>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let db_settings = db::get_all_settings(&state.db).await?;
    let resolved = state.config.resolve_settings(&db_settings);
    let flash_message = map_flash_code(query.msg.as_deref());

    let tpl = build_settings_template(&state.config, resolved, csrf_token, flash_message, None);
    render_html(tpl, set_cookie)
}

#[derive(Debug, Deserialize)]
pub struct SettingsFormPayload {
    pub csrf_token: String,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub fetch_enabled: Option<String>,
    #[serde(default)]
    pub fetch_interval_minutes: Option<String>,
    #[serde(default)]
    pub fetch_concurrency: Option<String>,
    #[serde(default)]
    pub extraction_concurrency: Option<String>,
    #[serde(default)]
    pub feed_request_timeout_seconds: Option<String>,
    #[serde(default)]
    pub article_request_timeout_seconds: Option<String>,
    #[serde(default)]
    pub max_feed_response_bytes: Option<String>,
    #[serde(default)]
    pub max_article_response_bytes: Option<String>,
    #[serde(default)]
    pub max_feed_entries: Option<String>,
    #[serde(default)]
    pub fetch_max_attempts: Option<String>,
    #[serde(default)]
    pub ai_provider: Option<String>,
    #[serde(default)]
    pub ai_base_url: Option<String>,
    #[serde(default)]
    pub ai_model: Option<String>,
}

async fn update_settings_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<SettingsFormPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;

    // Reject attempts to submit overrides for environment-locked settings
    let locked_checks: &[(&Option<String>, ConfigSource, &str)] = &[
        (
            &form.timezone,
            state.config.timezone_source,
            "QUIRE_TIMEZONE",
        ),
        (
            &form.fetch_enabled,
            state.config.fetch_enabled_source,
            "QUIRE_FETCH_ENABLED",
        ),
        (
            &form.fetch_interval_minutes,
            state.config.fetch_interval_minutes_source,
            "QUIRE_FETCH_INTERVAL_MINUTES",
        ),
        (
            &form.fetch_concurrency,
            state.config.fetch_concurrency_source,
            "QUIRE_FETCH_CONCURRENCY",
        ),
        (
            &form.extraction_concurrency,
            state.config.extraction_concurrency_source,
            "QUIRE_EXTRACTION_CONCURRENCY",
        ),
        (
            &form.feed_request_timeout_seconds,
            state.config.feed_request_timeout_seconds_source,
            "QUIRE_FEED_REQUEST_TIMEOUT_SECONDS",
        ),
        (
            &form.article_request_timeout_seconds,
            state.config.article_request_timeout_seconds_source,
            "QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS",
        ),
        (
            &form.max_feed_response_bytes,
            state.config.max_feed_response_bytes_source,
            "QUIRE_MAX_FEED_RESPONSE_BYTES",
        ),
        (
            &form.max_article_response_bytes,
            state.config.max_article_response_bytes_source,
            "QUIRE_MAX_ARTICLE_RESPONSE_BYTES",
        ),
        (
            &form.max_feed_entries,
            state.config.max_feed_entries_source,
            "QUIRE_MAX_FEED_ENTRIES",
        ),
        (
            &form.fetch_max_attempts,
            state.config.fetch_max_attempts_source,
            "QUIRE_FETCH_MAX_ATTEMPTS",
        ),
        (
            &form.ai_provider,
            state.config.ai_provider_source,
            "QUIRE_AI_PROVIDER",
        ),
        (
            &form.ai_base_url,
            state.config.ai_base_url_source,
            "QUIRE_AI_BASE_URL",
        ),
        (
            &form.ai_model,
            state.config.ai_model_source,
            "QUIRE_AI_MODEL",
        ),
    ];

    for (submitted_opt, source, env_name) in locked_checks {
        if *source == ConfigSource::Environment && submitted_opt.is_some() {
            return render_settings_error(
                &state,
                form.csrf_token,
                format!("Setting controlled by {env_name} is locked by an environment variable and cannot be modified."),
            )
            .await;
        }
    }

    let mut updates: Vec<(&str, String)> = Vec::new();

    if state.config.timezone_source != ConfigSource::Environment
        && let Some(tz) = &form.timezone
    {
        let trimmed = tz.trim();
        if !trimmed.is_empty()
            && let Err(AppError::Validation(err)) = validate_timezone(trimmed)
        {
            return render_settings_error(&state, form.csrf_token, err).await;
        }
        updates.push(("timezone", trimmed.to_string()));
    }

    if state.config.fetch_enabled_source != ConfigSource::Environment
        && let Some(fe) = &form.fetch_enabled
    {
        let trimmed = fe.trim();
        if !trimmed.is_empty() {
            match parse_bool_setting(trimmed, "Automatic Fetching Enabled") {
                Ok(b) => updates.push(("fetch_enabled", b.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        }
    }

    if state.config.fetch_interval_minutes_source != ConfigSource::Environment
        && let Some(fi) = &form.fetch_interval_minutes
    {
        let trimmed = fi.trim();
        if !trimmed.is_empty() {
            match validate_fetch_interval_minutes(
                trimmed,
                state.config.fetch_min_interval_minutes,
                "Default Fetch Interval",
            ) {
                Ok(v) => updates.push(("fetch_interval_minutes", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("fetch_interval_minutes", String::new()));
        }
    }

    if state.config.fetch_concurrency_source != ConfigSource::Environment
        && let Some(fc) = &form.fetch_concurrency
    {
        let trimmed = fc.trim();
        if !trimmed.is_empty() {
            match validate_concurrency(trimmed, "Max Concurrent Feed Requests") {
                Ok(v) => updates.push(("fetch_concurrency", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("fetch_concurrency", String::new()));
        }
    }

    if state.config.extraction_concurrency_source != ConfigSource::Environment
        && let Some(ec) = &form.extraction_concurrency
    {
        let trimmed = ec.trim();
        if !trimmed.is_empty() {
            match validate_concurrency(trimmed, "Max Concurrent Extractions") {
                Ok(v) => updates.push(("extraction_concurrency", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("extraction_concurrency", String::new()));
        }
    }

    if state.config.feed_request_timeout_seconds_source != ConfigSource::Environment
        && let Some(ft) = &form.feed_request_timeout_seconds
    {
        let trimmed = ft.trim();
        if !trimmed.is_empty() {
            match validate_timeout_seconds(trimmed, "Feed Request Timeout") {
                Ok(v) => updates.push(("feed_request_timeout_seconds", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("feed_request_timeout_seconds", String::new()));
        }
    }

    if state.config.article_request_timeout_seconds_source != ConfigSource::Environment
        && let Some(at) = &form.article_request_timeout_seconds
    {
        let trimmed = at.trim();
        if !trimmed.is_empty() {
            match validate_timeout_seconds(trimmed, "Article Request Timeout") {
                Ok(v) => updates.push(("article_request_timeout_seconds", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("article_request_timeout_seconds", String::new()));
        }
    }

    if state.config.max_feed_response_bytes_source != ConfigSource::Environment
        && let Some(fb) = &form.max_feed_response_bytes
    {
        let trimmed = fb.trim();
        if !trimmed.is_empty() {
            match validate_max_response_bytes(trimmed, "Max Feed Response Bytes") {
                Ok(v) => updates.push(("max_feed_response_bytes", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("max_feed_response_bytes", String::new()));
        }
    }

    if state.config.max_article_response_bytes_source != ConfigSource::Environment
        && let Some(ab) = &form.max_article_response_bytes
    {
        let trimmed = ab.trim();
        if !trimmed.is_empty() {
            match validate_max_response_bytes(trimmed, "Max Article Response Bytes") {
                Ok(v) => updates.push(("max_article_response_bytes", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("max_article_response_bytes", String::new()));
        }
    }

    if state.config.max_feed_entries_source != ConfigSource::Environment
        && let Some(me) = &form.max_feed_entries
    {
        let trimmed = me.trim();
        if !trimmed.is_empty() {
            match validate_max_entries(trimmed, "Max Entries Per Feed Fetch") {
                Ok(v) => updates.push(("max_feed_entries", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("max_feed_entries", String::new()));
        }
    }

    if state.config.fetch_max_attempts_source != ConfigSource::Environment
        && let Some(ma) = &form.fetch_max_attempts
    {
        let trimmed = ma.trim();
        if !trimmed.is_empty() {
            match validate_max_attempts(trimmed, "Max HTTP Fetch Attempts") {
                Ok(v) => updates.push(("fetch_max_attempts", v.to_string())),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("fetch_max_attempts", String::new()));
        }
    }

    if state.config.ai_provider_source != ConfigSource::Environment
        && let Some(prov) = &form.ai_provider
    {
        let trimmed = prov.trim();
        if trimmed.chars().count() > 100 {
            return render_settings_error(
                &state,
                form.csrf_token,
                "AI Provider must not exceed 100 characters.".to_string(),
            )
            .await;
        }
        updates.push(("ai_provider", trimmed.to_string()));
    }

    if state.config.ai_base_url_source != ConfigSource::Environment
        && let Some(base_url) = &form.ai_base_url
    {
        let trimmed = base_url.trim();
        if !trimmed.is_empty() {
            match validate_http_url_allowing_local_ai(trimmed) {
                Ok(normalized) => updates.push(("ai_base_url", normalized)),
                Err(AppError::Validation(err)) => {
                    return render_settings_error(&state, form.csrf_token, err).await;
                }
                Err(other) => return Err(other),
            }
        } else {
            updates.push(("ai_base_url", String::new()));
        }
    }

    if state.config.ai_model_source != ConfigSource::Environment
        && let Some(model) = &form.ai_model
    {
        let trimmed = model.trim();
        if trimmed.chars().count() > 200 {
            return render_settings_error(
                &state,
                form.csrf_token,
                "AI Model must not exceed 200 characters.".to_string(),
            )
            .await;
        }
        updates.push(("ai_model", trimmed.to_string()));
    }

    let borrowed: Vec<(&str, &str)> = updates.iter().map(|(k, v)| (*k, v.as_str())).collect();
    db::upsert_settings(&state.db, &borrowed).await?;
    state.coordinator.notify_workers();

    Ok(Redirect::to("/settings?msg=settings_saved").into_response())
}

async fn render_settings_error(
    state: &AppState,
    csrf_token: String,
    error_message: String,
) -> AppResult<Response> {
    let db_settings = db::get_all_settings(&state.db).await?;
    let resolved = state.config.resolve_settings(&db_settings);
    let tpl = build_settings_template(
        &state.config,
        resolved,
        csrf_token,
        None,
        Some(error_message),
    );
    let mut resp = render_html(tpl, None)?;
    *resp.status_mut() = StatusCode::BAD_REQUEST;
    Ok(resp)
}

fn build_settings_template(
    config: &AppConfig,
    resolved: Vec<ResolvedSetting>,
    csrf_token: String,
    flash_message: Option<String>,
    error_message: Option<String>,
) -> SettingsTemplate {
    let find_val = |key: &str| -> String {
        resolved
            .iter()
            .find(|s| s.key == key)
            .map(|s| s.value.clone())
            .unwrap_or_default()
    };

    SettingsTemplate {
        app_version: APP_VERSION,
        active_nav: "settings",
        form_timezone: find_val("timezone"),
        timezone_locked: config.timezone_source == ConfigSource::Environment,
        form_fetch_enabled: find_val("fetch_enabled"),
        fetch_enabled_locked: config.fetch_enabled_source == ConfigSource::Environment,
        form_fetch_interval_minutes: find_val("fetch_interval_minutes"),
        fetch_interval_locked: config.fetch_interval_minutes_source == ConfigSource::Environment,
        form_fetch_concurrency: find_val("fetch_concurrency"),
        fetch_concurrency_locked: config.fetch_concurrency_source == ConfigSource::Environment,
        form_extraction_concurrency: find_val("extraction_concurrency"),
        extraction_concurrency_locked: config.extraction_concurrency_source
            == ConfigSource::Environment,
        form_feed_timeout: find_val("feed_request_timeout_seconds"),
        feed_timeout_locked: config.feed_request_timeout_seconds_source
            == ConfigSource::Environment,
        form_article_timeout: find_val("article_request_timeout_seconds"),
        article_timeout_locked: config.article_request_timeout_seconds_source
            == ConfigSource::Environment,
        form_max_feed_bytes: find_val("max_feed_response_bytes"),
        max_feed_bytes_locked: config.max_feed_response_bytes_source == ConfigSource::Environment,
        form_max_article_bytes: find_val("max_article_response_bytes"),
        max_article_bytes_locked: config.max_article_response_bytes_source
            == ConfigSource::Environment,
        form_max_feed_entries: find_val("max_feed_entries"),
        max_feed_entries_locked: config.max_feed_entries_source == ConfigSource::Environment,
        form_fetch_max_attempts: find_val("fetch_max_attempts"),
        fetch_max_attempts_locked: config.fetch_max_attempts_source == ConfigSource::Environment,
        form_ai_provider: find_val("ai_provider"),
        ai_provider_locked: config.ai_provider_source == ConfigSource::Environment,
        form_ai_base_url: find_val("ai_base_url"),
        ai_base_url_locked: config.ai_base_url_source == ConfigSource::Environment,
        form_ai_model: find_val("ai_model"),
        ai_model_locked: config.ai_model_source == ConfigSource::Environment,
        settings: resolved,
        csrf_token,
        flash_message,
        error_message,
    }
}

fn validate_http_url_allowing_local_ai(raw: &str) -> AppResult<String> {
    let parsed = url::Url::parse(raw.trim()).map_err(|_| {
        AppError::Validation("AI Base URL must be a valid http:// or https:// URL.".to_string())
    })?;
    match parsed.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(AppError::Validation(
                "AI Base URL must use http or https scheme.".to_string(),
            ));
        }
    }
    if parsed.host_str().is_none() {
        return Err(AppError::Validation(
            "AI Base URL must include a host.".to_string(),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AppError::Validation(
            "AI Base URL must not embed credentials.".to_string(),
        ));
    }
    Ok(parsed.to_string())
}

fn map_flash_code(code: Option<&str>) -> Option<String> {
    match code {
        Some("created") => Some("Feed added and persisted to SQLite.".to_string()),
        Some("updated") => Some("Feed updated.".to_string()),
        Some("toggled") => Some("Feed status updated.".to_string()),
        Some("deleted") => Some("Feed deleted.".to_string()),
        Some("fetch_started") => Some("Manual feed fetch request accepted and queued.".to_string()),
        Some("fetch_already_running") => {
            Some("A fetch is already running for this feed.".to_string())
        }
        Some("fetch_all_started") => {
            Some("Manual fetch request accepted for all enabled feeds.".to_string())
        }
        Some("extraction_queued") => Some("Article content extraction queued.".to_string()),
        Some("extraction_already_running") => {
            Some("Article extraction is already in progress.".to_string())
        }
        Some("settings_saved") => Some("Database settings saved.".to_string()),
        _ => None,
    }
}

fn render_html<T: Template>(tpl: T, set_cookie: Option<HeaderValue>) -> AppResult<Response> {
    let body = tpl.render()?;
    let mut resp = Html(body).into_response();
    if let Some(cookie) = set_cookie {
        resp.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    Ok(resp)
}

/// Extract the CSRF token from the `quire_csrf` cookie or generate a new 256-bit
/// hex token and return a `Set-Cookie` header value.
pub fn ensure_csrf_token(headers: &HeaderMap) -> (String, Option<HeaderValue>) {
    if let Some(existing) = extract_cookie(headers, CSRF_COOKIE_NAME)
        && is_valid_csrf_token_format(&existing)
    {
        return (existing, None);
    }

    let token = generate_csrf_token();
    let cookie_str = format!("{CSRF_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict");
    let header_val = HeaderValue::from_str(&cookie_str).ok();
    (token, header_val)
}

pub fn verify_csrf(headers: &HeaderMap, submitted_token: &str) -> AppResult<()> {
    let submitted = submitted_token.trim();
    if !is_valid_csrf_token_format(submitted) {
        return Err(AppError::Forbidden(
            "Invalid or missing CSRF token.".to_string(),
        ));
    }
    let Some(cookie_token) = extract_cookie(headers, CSRF_COOKIE_NAME) else {
        return Err(AppError::Forbidden(
            "Missing CSRF session cookie.".to_string(),
        ));
    };
    if cookie_token != submitted {
        return Err(AppError::Forbidden("CSRF token mismatch.".to_string()));
    }
    Ok(())
}

fn generate_csrf_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes);
    let mut out = String::with_capacity(64);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn is_valid_csrf_token_format(token: &str) -> bool {
    token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit())
}

fn extract_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for val in headers.get_all(header::COOKIE) {
        let Ok(raw) = val.to_str() else { continue };
        for part in raw.split(';') {
            let trimmed = part.trim();
            if let Some((k, v)) = trimmed.split_once('=')
                && k.trim() == name
            {
                return Some(v.trim().to_string());
            }
        }
    }
    None
}
