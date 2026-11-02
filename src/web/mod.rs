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

use crate::articles::{self, ArticleSummaryRow};
use crate::config::{AppConfig, ConfigSource, ResolvedSetting, validate_timezone};
use crate::db::{self, DashboardCounts, DbHealth};
use crate::editions::{self, EditionRow};
use crate::error::{AppError, AppResult};
use crate::feeds::{self, Feed, NewFeedInput, UpdateFeedInput, validate_http_url};

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const CSRF_COOKIE_NAME: &str = "quire_csrf";

#[derive(Clone)]
pub struct AppState {
    pub db: SqlitePool,
    pub config: Arc<AppConfig>,
}

impl AppState {
    pub fn new(db: SqlitePool, config: AppConfig) -> Self {
        Self {
            db,
            config: Arc::new(config),
        }
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard_page))
        .route("/health", get(health_handler))
        .route("/feeds", get(feeds_page).post(create_feed_handler))
        .route("/feeds/{id}/edit", get(edit_feed_page))
        .route("/feeds/{id}", post(update_feed_handler))
        .route("/feeds/{id}/toggle", post(toggle_feed_handler))
        .route("/feeds/{id}/delete", post(delete_feed_handler))
        .route("/articles", get(articles_page))
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

#[derive(Template)]
#[template(path = "dashboard.html")]
struct DashboardTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    counts: DashboardCounts,
    db_health: DbHealth,
    database_url: String,
    timezone: String,
}

async fn dashboard_page(State(state): State<AppState>) -> AppResult<Response> {
    let counts = db::get_dashboard_counts(&state.db).await?;
    let db_health = db::check_health(&state.db).await?;
    let db_settings = db::get_all_settings(&state.db).await?;
    let timezone = if state.config.timezone_source == ConfigSource::Environment {
        state.config.timezone.clone()
    } else {
        db_settings
            .get("timezone")
            .cloned()
            .unwrap_or_else(|| state.config.timezone.clone())
    };

    let tpl = DashboardTemplate {
        app_version: APP_VERSION,
        active_nav: "dashboard",
        counts,
        db_health,
        database_url: state.config.database_url.clone(),
        timezone,
    };
    render_html(tpl, None)
}

#[derive(Debug, Default, Deserialize)]
struct FlashQuery {
    msg: Option<String>,
}

#[derive(Template)]
#[template(path = "feeds.html")]
struct FeedsTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feeds: Vec<Feed>,
    csrf_token: String,
    flash_message: Option<String>,
    error_message: Option<String>,
    form_name: String,
    form_feed_url: String,
    form_website_url: String,
    form_description: String,
    form_enabled: bool,
}

async fn feeds_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FlashQuery>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let feeds = feeds::list_feeds(&state.db).await?;
    let flash_message = map_flash_code(query.msg.as_deref());

    let tpl = FeedsTemplate {
        app_version: APP_VERSION,
        active_nav: "feeds",
        feeds,
        csrf_token,
        flash_message,
        error_message: None,
        form_name: String::new(),
        form_feed_url: String::new(),
        form_website_url: String::new(),
        form_description: String::new(),
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
    pub enabled: Option<String>,
}

async fn create_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<FeedFormPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;

    let enabled = form.enabled.as_deref() == Some("true") || form.enabled.as_deref() == Some("on");
    let input = NewFeedInput {
        name: form.name.clone(),
        feed_url: form.feed_url.clone(),
        website_url: Some(form.website_url.clone()),
        description: Some(form.description.clone()),
        enabled,
    };

    match feeds::create_feed(&state.db, input).await {
        Ok(_) => Ok(Redirect::to("/feeds?msg=created").into_response()),
        Err(AppError::Validation(err) | AppError::Conflict(err)) => {
            let feeds_list = feeds::list_feeds(&state.db).await?;
            let tpl = FeedsTemplate {
                app_version: APP_VERSION,
                active_nav: "feeds",
                feeds: feeds_list,
                csrf_token: form.csrf_token,
                flash_message: None,
                error_message: Some(err),
                form_name: form.name,
                form_feed_url: form.feed_url,
                form_website_url: form.website_url,
                form_description: form.description,
                form_enabled: enabled,
            };
            let mut resp = render_html(tpl, None)?;
            *resp.status_mut() = StatusCode::BAD_REQUEST;
            Ok(resp)
        }
        Err(other) => Err(other),
    }
}

#[derive(Template)]
#[template(path = "feed_edit.html")]
struct FeedEditTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    feed: Feed,
    csrf_token: String,
    error_message: Option<String>,
    form_name: String,
    form_feed_url: String,
    form_website_url: String,
    form_description: String,
    form_enabled: bool,
}

async fn edit_feed_page(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> AppResult<Response> {
    let (csrf_token, set_cookie) = ensure_csrf_token(&headers);
    let feed = feeds::get_feed_by_id(&state.db, id).await?;

    let tpl = FeedEditTemplate {
        app_version: APP_VERSION,
        active_nav: "feeds",
        form_name: feed.name.clone(),
        form_feed_url: feed.feed_url.clone(),
        form_website_url: feed.website_url.clone().unwrap_or_default(),
        form_description: feed.description.clone().unwrap_or_default(),
        form_enabled: feed.enabled,
        feed,
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
    let input = UpdateFeedInput {
        name: form.name.clone(),
        feed_url: form.feed_url.clone(),
        website_url: Some(form.website_url.clone()),
        description: Some(form.description.clone()),
        enabled,
    };

    match feeds::update_feed(&state.db, id, input).await {
        Ok(_) => Ok(Redirect::to("/feeds?msg=updated").into_response()),
        Err(AppError::Validation(err) | AppError::Conflict(err)) => {
            let feed = feeds::get_feed_by_id(&state.db, id).await?;
            let tpl = FeedEditTemplate {
                app_version: APP_VERSION,
                active_nav: "feeds",
                feed,
                csrf_token: form.csrf_token,
                error_message: Some(err),
                form_name: form.name,
                form_feed_url: form.feed_url,
                form_website_url: form.website_url,
                form_description: form.description,
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

async fn toggle_feed_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Form(form): Form<CsrfOnlyPayload>,
) -> AppResult<Response> {
    verify_csrf(&headers, &form.csrf_token)?;
    let existing = feeds::get_feed_by_id(&state.db, id).await?;
    feeds::set_feed_enabled(&state.db, id, !existing.enabled).await?;
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

#[derive(Template)]
#[template(path = "articles.html")]
struct ArticlesTemplate {
    app_version: &'static str,
    active_nav: &'static str,
    articles: Vec<ArticleSummaryRow>,
}

async fn articles_page(State(state): State<AppState>) -> AppResult<Response> {
    let articles = articles::list_recent_articles(&state.db, 100).await?;
    let tpl = ArticlesTemplate {
        app_version: APP_VERSION,
        active_nav: "articles",
        articles,
    };
    render_html(tpl, None)
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
    let _ = validate_http_url;
    Ok(parsed.to_string())
}

fn map_flash_code(code: Option<&str>) -> Option<String> {
    match code {
        Some("created") => Some("Feed added and persisted to SQLite.".to_string()),
        Some("updated") => Some("Feed updated.".to_string()),
        Some("toggled") => Some("Feed status updated.".to_string()),
        Some("deleted") => Some("Feed deleted.".to_string()),
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
