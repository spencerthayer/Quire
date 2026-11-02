use std::collections::HashMap;
use std::path::PathBuf;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use quire::config::{AppConfig, ConfigSource};
use quire::db;
use quire::feeds::{self, NewFeedInput, UpdateFeedInput, validate_http_url};
use quire::web::{AppState, HealthResponse, create_router};
use tempfile::TempDir;
use tower::ServiceExt;

async fn setup_test_state() -> (TempDir, AppState) {
    let temp_dir = TempDir::new().expect("create temp dir");
    let db_path = temp_dir.path().join("test_quire.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let mut env_map = HashMap::new();
    env_map.insert(
        "QUIRE_DATA_DIR".to_string(),
        temp_dir.path().display().to_string(),
    );
    env_map.insert("QUIRE_DATABASE_URL".to_string(), db_url.clone());

    let config = AppConfig::from_map(&env_map).expect("load test config");
    let pool = db::init_pool(&config.database_url, &config.data_dir)
        .await
        .expect("init test pool");

    (temp_dir, AppState::new(pool, config))
}

#[test]
fn test_config_defaults_and_env_precedence() {
    let empty = HashMap::new();
    let cfg = AppConfig::from_map(&empty).expect("default config");
    assert_eq!(cfg.host, "0.0.0.0");
    assert_eq!(cfg.port, 8080);
    assert_eq!(cfg.log_level, "info");
    assert_eq!(cfg.timezone, "America/Los_Angeles");
    assert_eq!(cfg.timezone_source, ConfigSource::Default);
    assert!(cfg.ai_api_key.is_none());
    assert!(cfg.elevenreader_refresh_token.is_none());

    let mut env_vars = HashMap::new();
    env_vars.insert("QUIRE_PORT".to_string(), "9090".to_string());
    env_vars.insert("QUIRE_TIMEZONE".to_string(), "UTC".to_string());
    env_vars.insert(
        "QUIRE_AI_API_KEY".to_string(),
        "super-secret-key-value".to_string(),
    );

    let cfg_env = AppConfig::from_map(&env_vars).expect("env config");
    assert_eq!(cfg_env.port, 9090);
    assert_eq!(cfg_env.port_source, ConfigSource::Environment);
    assert_eq!(cfg_env.timezone, "UTC");
    assert_eq!(cfg_env.timezone_source, ConfigSource::Environment);

    // Ensure Debug output redacts secret values
    let debug_repr = format!("{cfg_env:?}");
    assert!(!debug_repr.contains("super-secret-key-value"));
    assert!(debug_repr.contains("[REDACTED]"));

    // Environment variable must override database setting
    let mut db_settings = HashMap::new();
    db_settings.insert("timezone".to_string(), "Europe/London".to_string());
    let resolved = cfg_env.resolve_settings(&db_settings);
    let tz_setting = resolved
        .iter()
        .find(|s| s.key == "timezone")
        .expect("timezone setting");
    assert_eq!(tz_setting.value, "UTC");
    assert_eq!(tz_setting.source, ConfigSource::Environment);
    assert!(!tz_setting.editable_in_ui);
}

#[test]
fn test_feed_url_validation_and_ssrf_protection() {
    assert!(
        validate_http_url(
            "https://feeds.arstechnica.com/arstechnica/index",
            "Feed URL"
        )
        .is_ok()
    );
    assert!(validate_http_url("http://example.com/rss.xml", "Feed URL").is_ok());

    // Reject invalid schemes
    assert!(validate_http_url("file:///etc/passwd", "Feed URL").is_err());
    assert!(validate_http_url("ftp://example.com/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("javascript:alert(1)", "Feed URL").is_err());

    // Reject localhost and private IPs (SSRF protection)
    assert!(validate_http_url("http://localhost:8080/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("http://127.0.0.1/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("http://169.254.169.254/latest/meta-data/", "Feed URL").is_err());
    assert!(validate_http_url("http://10.0.0.1/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("http://192.168.1.1/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("http://[::1]/feed.xml", "Feed URL").is_err());
    assert!(validate_http_url("https://user:pass@example.com/feed.xml", "Feed URL").is_err());
}

#[tokio::test]
async fn test_sqlite_initialization_and_idempotent_migrations() {
    let temp_dir = TempDir::new().expect("temp dir");
    let db_path: PathBuf = temp_dir.path().join("nested").join("quire.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let pool1 = db::init_pool(&db_url, temp_dir.path())
        .await
        .expect("first init_pool");
    let health1 = db::check_health(&pool1).await.expect("health 1");
    assert!(health1.ok);
    assert_eq!(health1.journal_mode, "wal");
    assert!(health1.foreign_keys_enabled);
    pool1.close().await;

    // Repeated startup against the existing database must succeed without corrupting data
    let pool2 = db::init_pool(&db_url, temp_dir.path())
        .await
        .expect("second init_pool");
    let counts = db::get_dashboard_counts(&pool2).await.expect("counts");
    assert_eq!(counts.total_feeds, 0);
    assert_eq!(counts.total_articles, 0);
    assert_eq!(counts.total_editions, 0);
    pool2.close().await;
}

#[tokio::test]
async fn test_feed_crud_operations() {
    let (_tmp, state) = setup_test_state().await;

    let created = feeds::create_feed(
        &state.db,
        NewFeedInput {
            name: "Ars Technica".to_string(),
            feed_url: "https://feeds.arstechnica.com/arstechnica/index".to_string(),
            website_url: Some("https://arstechnica.com".to_string()),
            description: Some("Tech news".to_string()),
            enabled: true,
        },
    )
    .await
    .expect("create feed");

    assert_eq!(created.name, "Ars Technica");
    assert!(created.enabled);

    // Duplicate feed_url must fail with Conflict
    let duplicate_err = feeds::create_feed(
        &state.db,
        NewFeedInput {
            name: "Duplicate Ars".to_string(),
            feed_url: "https://feeds.arstechnica.com/arstechnica/index".to_string(),
            website_url: None,
            description: None,
            enabled: true,
        },
    )
    .await;
    assert!(duplicate_err.is_err());

    // Update feed
    let updated = feeds::update_feed(
        &state.db,
        created.id,
        UpdateFeedInput {
            name: "Ars Technica Main".to_string(),
            feed_url: "https://feeds.arstechnica.com/arstechnica/index".to_string(),
            website_url: Some("https://arstechnica.com".to_string()),
            description: Some("Updated description".to_string()),
            enabled: false,
        },
    )
    .await
    .expect("update feed");

    assert_eq!(updated.name, "Ars Technica Main");
    assert_eq!(updated.description.as_deref(), Some("Updated description"));
    assert!(!updated.enabled);

    // Toggle feed
    let toggled = feeds::set_feed_enabled(&state.db, created.id, true)
        .await
        .expect("toggle feed");
    assert!(toggled.enabled);

    // List feeds
    let all = feeds::list_feeds(&state.db).await.expect("list feeds");
    assert_eq!(all.len(), 1);

    // Delete feed
    feeds::delete_feed(&state.db, created.id)
        .await
        .expect("delete feed");
    let remaining = feeds::list_feeds(&state.db)
        .await
        .expect("list feeds after delete");
    assert!(remaining.is_empty());
}

#[tokio::test]
async fn test_health_endpoint_and_basic_web_routes() {
    let (_tmp, state) = setup_test_state().await;
    let app = create_router(state.clone());

    // 1. GET /health
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("health request");
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let health_json: HealthResponse = serde_json::from_slice(&body_bytes).expect("parse health");
    assert_eq!(health_json.status, "ok");
    assert!(health_json.database.connected);
    assert!(health_json.database.foreign_keys_enabled);

    // 2. GET / (Dashboard)
    let resp = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .expect("dashboard request");
    assert_eq!(resp.status(), StatusCode::OK);
    let html = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Dashboard"));
    assert!(html.contains("Configured Feeds"));

    // 3. GET /feeds and extract CSRF cookie
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/feeds")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("feeds page request");
    assert_eq!(resp.status(), StatusCode::OK);
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .expect("csrf set-cookie")
        .to_str()
        .unwrap()
        .to_string();
    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();
    let csrf_token = cookie_pair.split('=').nth(1).unwrap().to_string();

    // 4. POST /feeds without CSRF token must return 403 Forbidden
    let bad_post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/feeds")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(
                    "csrf_token=invalid&name=Test&feed_url=https%3A%2F%2Fexample.com%2Frss.xml",
                ))
                .unwrap(),
        )
        .await
        .expect("bad post");
    assert_eq!(bad_post.status(), StatusCode::FORBIDDEN);

    // 5. POST /feeds with valid CSRF cookie and form token creates the feed
    let form_body = format!(
        "csrf_token={csrf_token}&name=Example+Feed&feed_url=https%3A%2F%2Fexample.com%2Ffeed.xml&website_url=https%3A%2F%2Fexample.com&description=Test+Feed&enabled=true"
    );
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/feeds")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::from(form_body))
                .unwrap(),
        )
        .await
        .expect("valid post");
    assert_eq!(create_resp.status(), StatusCode::SEE_OTHER);

    // 6. Verify /articles, /editions, and /settings load with 200 OK
    for route in ["/articles", "/editions", "/settings"] {
        let page_resp = app
            .clone()
            .oneshot(Request::builder().uri(route).body(Body::empty()).unwrap())
            .await
            .expect("route request");
        assert_eq!(page_resp.status(), StatusCode::OK, "failed route {route}");
    }
}
