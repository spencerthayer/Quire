use std::collections::HashMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use http_body_util::BodyExt;
use quire::articles::extractor::{ExtractionResult, extract_readable_html_from_str};
use quire::articles::normalizer::{canonicalize_article_url, compute_content_hash};
use quire::articles::sanitizer::{sanitize_article_html, sanitize_summary_fallback};
use quire::articles::{
    self, ArticleFilter, claim_next_extraction_job, complete_extraction_job,
    persist_discovered_entries, recover_interrupted_extractions,
};
use quire::config::{AppConfig, ConfigSource};
use quire::db;
use quire::error::{AppError, AppResult};
use quire::feeds::fetcher::{
    FetchErrorCategory, FetchOutcome, FetchRequestOptions, fetch_url_with_retries,
};
use quire::feeds::parser::{NormalizedFeedEntry, parse_feed_bytes};
use quire::feeds::validation::{HostResolver, is_disallowed_ip, validate_http_url};
use quire::feeds::{
    self, NewFeedInput, list_feed_fetch_runs, recover_interrupted_fetches, try_claim_feed_fetch,
};
use quire::web::{AppState, create_router};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tower::ServiceExt;
use url::Url;

/// Controlled test resolver that maps specific virtual public hostnames (such as
/// `fixture.example.com`) to a local test fixture `SocketAddr` while still enforcing
/// production SSRF IP validation on all other hostnames or simulated DNS-rebinding
/// addresses.
struct FixtureResolver {
    routes: HashMap<String, Vec<SocketAddr>>,
    enforce_ip_check_on_routes: bool,
}

impl FixtureResolver {
    fn new_local_fixture(host: &str, addr: SocketAddr) -> Arc<dyn HostResolver> {
        let mut routes = HashMap::new();
        routes.insert(host.to_ascii_lowercase(), vec![addr]);
        Arc::new(Self {
            routes,
            enforce_ip_check_on_routes: false,
        })
    }

    fn new_simulated_dns(routes: HashMap<String, Vec<SocketAddr>>) -> Arc<dyn HostResolver> {
        Arc::new(Self {
            routes,
            enforce_ip_check_on_routes: true,
        })
    }
}

impl HostResolver for FixtureResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let key = host.to_ascii_lowercase();
        let mapped = self.routes.get(&key).cloned();
        let enforce_check = self.enforce_ip_check_on_routes;
        Box::pin(async move {
            let Some(addrs) = mapped else {
                return Err(AppError::Validation(format!(
                    "DNS resolution failed for unknown host '{key}'."
                )));
            };
            if addrs.is_empty() {
                return Err(AppError::Validation(format!(
                    "DNS resolution returned no addresses for '{key}'."
                )));
            }
            if enforce_check {
                for addr in &addrs {
                    if is_disallowed_ip(&addr.ip()) {
                        return Err(AppError::Validation(format!(
                            "Host '{key}' resolved to restricted network address '{}'.",
                            addr.ip()
                        )));
                    }
                }
            }
            let _ = port;
            Ok(addrs)
        })
    }
}

async fn setup_test_state() -> (TempDir, AppState) {
    let temp_dir = TempDir::new().expect("create temp dir");
    let db_path = temp_dir.path().join("test_quire.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let mut env_map = HashMap::new();
    env_map.insert(
        "QUIRE_DATA_DIR".to_string(),
        temp_dir.path().display().to_string(),
    );
    env_map.insert("QUIRE_DATABASE_URL".to_string(), db_url);

    let config = AppConfig::from_map(&env_map).expect("load test config");
    let pool = db::init_pool(&config.database_url, &config.data_dir)
        .await
        .expect("init test pool");

    (temp_dir, AppState::new(pool, config))
}

async fn setup_test_state_with_resolver(
    resolver: Arc<dyn HostResolver>,
    extra_env: HashMap<String, String>,
) -> (TempDir, AppState) {
    let temp_dir = TempDir::new().expect("create temp dir");
    let db_path = temp_dir.path().join("test_quire.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let mut env_map = extra_env;
    env_map.insert(
        "QUIRE_DATA_DIR".to_string(),
        temp_dir.path().display().to_string(),
    );
    env_map.insert("QUIRE_DATABASE_URL".to_string(), db_url);

    let config = AppConfig::from_map(&env_map).expect("load test config");
    let pool = db::init_pool(&config.database_url, &config.data_dir)
        .await
        .expect("init test pool");

    (temp_dir, AppState::with_resolver(pool, config, resolver))
}

// =============================================================================
// 1. Configuration & Precedence Tests
// =============================================================================

#[test]
fn test_config_defaults_validation_and_env_precedence() {
    let empty = HashMap::new();
    let cfg = AppConfig::from_map(&empty).expect("default config");
    assert_eq!(cfg.host, "0.0.0.0");
    assert_eq!(cfg.port, 8080);
    assert_eq!(cfg.log_level, "info");
    assert_eq!(cfg.timezone, "America/Los_Angeles");
    assert!(cfg.fetch_enabled);
    assert_eq!(cfg.fetch_interval_minutes, 60);
    assert_eq!(cfg.fetch_concurrency, 4);
    assert_eq!(cfg.extraction_concurrency, 4);
    assert_eq!(cfg.feed_request_timeout_seconds, 30);
    assert_eq!(cfg.article_request_timeout_seconds, 45);
    assert_eq!(cfg.max_feed_response_bytes, 10 * 1024 * 1024);
    assert_eq!(cfg.max_article_response_bytes, 15 * 1024 * 1024);
    assert_eq!(cfg.max_feed_entries, 200);
    assert_eq!(cfg.fetch_max_attempts, 3);
    assert!(cfg.ai_api_key.is_none());
    assert!(cfg.elevenreader_refresh_token.is_none());

    // Invalid configuration values must be rejected
    let mut bad_concurrency = HashMap::new();
    bad_concurrency.insert("QUIRE_FETCH_CONCURRENCY".to_string(), "0".to_string());
    assert!(AppConfig::from_map(&bad_concurrency).is_err());

    let mut bad_interval = HashMap::new();
    bad_interval.insert("QUIRE_FETCH_INTERVAL_MINUTES".to_string(), "2".to_string());
    assert!(AppConfig::from_map(&bad_interval).is_err());

    let mut bad_bytes = HashMap::new();
    bad_bytes.insert(
        "QUIRE_MAX_FEED_RESPONSE_BYTES".to_string(),
        "100".to_string(),
    );
    assert!(AppConfig::from_map(&bad_bytes).is_err());

    // Environment variables override DB settings and redact secrets
    let mut env_vars = HashMap::new();
    env_vars.insert("QUIRE_PORT".to_string(), "9090".to_string());
    env_vars.insert("QUIRE_TIMEZONE".to_string(), "UTC".to_string());
    env_vars.insert("QUIRE_FETCH_ENABLED".to_string(), "false".to_string());
    env_vars.insert(
        "QUIRE_AI_API_KEY".to_string(),
        "super-secret-key-value".to_string(),
    );

    let cfg_env = AppConfig::from_map(&env_vars).expect("env config");
    let debug_repr = format!("{cfg_env:?}");
    assert!(!debug_repr.contains("super-secret-key-value"));
    assert!(debug_repr.contains("[REDACTED]"));

    let mut db_settings = HashMap::new();
    db_settings.insert("timezone".to_string(), "Europe/London".to_string());
    db_settings.insert("fetch_enabled".to_string(), "true".to_string());
    db_settings.insert("fetch_interval_minutes".to_string(), "15".to_string());

    let rt = cfg_env.resolve_runtime_ingestion(&db_settings);
    assert_eq!(rt.timezone, "UTC");
    assert!(!rt.fetch_enabled); // Env wins over DB
    assert_eq!(rt.fetch_interval_minutes, 15); // DB wins over Default

    let resolved = cfg_env.resolve_settings(&db_settings);
    let fe_setting = resolved
        .iter()
        .find(|s| s.key == "fetch_enabled")
        .expect("fetch_enabled");
    assert_eq!(fe_setting.source, ConfigSource::Environment);
    assert!(!fe_setting.editable_in_ui);
}

// =============================================================================
// 2. SSRF, DNS Rebinding, and URL Validation Tests (Section 16.3)
// =============================================================================

#[tokio::test]
async fn test_ssrf_url_and_dns_rebinding_protections() {
    assert!(
        validate_http_url(
            "https://feeds.arstechnica.com/arstechnica/index",
            "Feed URL"
        )
        .is_ok()
    );
    assert!(validate_http_url("http://example.com/rss.xml", "Feed URL").is_ok());

    // Unsupported schemes and embedded credentials
    for bad in [
        "file:///etc/passwd",
        "ftp://example.com/feed.xml",
        "gopher://example.com/1",
        "data:text/html,<h1>hi</h1>",
        "javascript:alert(1)",
        "https://user:pass@example.com/feed.xml",
    ] {
        assert!(
            validate_http_url(bad, "Feed URL").is_err(),
            "expected scheme/credential rejection for {bad}"
        );
    }

    // Localhost, internal hostnames, loopback, private IPv4/IPv6, link-local, metadata, IPv4-mapped IPv6
    for restricted in [
        "http://localhost:8080/feed.xml",
        "http://sub.localhost/feed.xml",
        "http://printer.local/feed.xml",
        "http://service.internal/feed.xml",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://127.0.0.1/feed.xml",
        "http://127.0.0.53:8080/feed.xml",
        "http://0.0.0.0/feed.xml",
        "http://10.0.0.1/feed.xml",
        "http://172.16.5.4/feed.xml",
        "http://192.168.1.1/feed.xml",
        "http://169.254.169.254/latest/meta-data/",
        "http://100.64.0.1/feed.xml",
        "http://[::1]/feed.xml",
        "http://[::]/feed.xml",
        "http://[fc00::1]/feed.xml",
        "http://[fd00:ec2::254]/latest/meta-data/",
        "http://[fe80::1]/feed.xml",
        "http://[::ffff:127.0.0.1]/feed.xml",
        "http://[::ffff:169.254.169.254]/feed.xml",
    ] {
        assert!(
            validate_http_url(restricted, "Feed URL").is_err(),
            "expected SSRF rejection for {restricted}"
        );
    }

    // DNS rebinding test: hostname passes string validation (`rebind.example.com`)
    // but resolves to a mix of public and private/loopback IP addresses.
    let mut dns_map = HashMap::new();
    dns_map.insert(
        "rebind.example.com".to_string(),
        vec![
            SocketAddr::new(IpAddr::from([93, 184, 216, 34]), 80),
            SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 80),
        ],
    );
    dns_map.insert(
        "metadata-rebind.example.com".to_string(),
        vec![SocketAddr::new(IpAddr::from([169, 254, 169, 254]), 80)],
    );
    let resolver = FixtureResolver::new_simulated_dns(dns_map);
    let opts = FetchRequestOptions {
        max_attempts: 1,
        ..Default::default()
    };

    let err1 = fetch_url_with_retries("http://rebind.example.com/feed.xml", &opts, &resolver)
        .await
        .expect_err("must block DNS rebinding to 127.0.0.1");
    assert_eq!(err1.category, FetchErrorCategory::SsrfBlocked);
    assert!(!err1.retryable);

    let err2 = fetch_url_with_retries(
        "http://metadata-rebind.example.com/feed.xml",
        &opts,
        &resolver,
    )
    .await
    .expect_err("must block DNS rebinding to metadata IP");
    assert_eq!(err2.category, FetchErrorCategory::SsrfBlocked);
}

// =============================================================================
// 3. Feed Parsing and Normalization Tests (Section 16.1)
// =============================================================================

#[test]
fn test_feed_parsing_rss_atom_rdf_and_edge_cases() {
    let base_url = Url::parse("https://news.example.com/feeds/main.xml").unwrap();

    // 1. Valid RSS 2.0 with relative URLs, tracking params, missing author/date on 2nd entry
    let rss_xml = br#"<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>Example Tech News</title>
        <link>https://news.example.com</link>
        <description>Daily technology updates</description>
        <language>en-us</language>
        <item>
          <guid>item-1</guid>
          <title>  First   Article   Title  </title>
          <link>/articles/first-post?id=42&amp;utm_source=rss&amp;utm_medium=feed#comments</link>
          <author>Alice Smith</author>
          <description>&lt;p&gt;Summary one&lt;/p&gt;</description>
          <pubDate>Sun, 01 Nov 2026 12:00:00 GMT</pubDate>
        </item>
        <item>
          <guid>item-2</guid>
          <link>//cdn.example.com/story-2?fbclid=abc123</link>
        </item>
        <item>
          <title>Entry Without Usable Link</title>
          <description>Should be skipped cleanly</description>
        </item>
      </channel>
    </rss>"#;

    let parsed_rss =
        parse_feed_bytes(rss_xml, Some("application/rss+xml"), &base_url, 200).expect("parse rss");
    assert_eq!(
        parsed_rss.metadata.title.as_deref(),
        Some("Example Tech News")
    );
    assert_eq!(
        parsed_rss.metadata.website_url.as_deref(),
        Some("https://news.example.com/")
    );
    assert_eq!(parsed_rss.entries_seen, 3);
    assert_eq!(parsed_rss.entries_skipped, 1);
    assert_eq!(parsed_rss.entries_truncated, 0);
    assert_eq!(parsed_rss.entries.len(), 2);

    let e1 = &parsed_rss.entries[0];
    assert_eq!(e1.title, "First Article Title");
    assert_eq!(
        e1.canonical_url,
        "https://news.example.com/articles/first-post?id=42"
    );
    assert_eq!(e1.published_at.as_deref(), Some("2026-11-01T12:00:00Z"));

    let e2 = &parsed_rss.entries[1];
    assert_eq!(e2.title, "Untitled Article");
    assert_eq!(e2.canonical_url, "https://cdn.example.com/story-2");
    assert!(e2.author.is_none());
    assert!(e2.published_at.is_none());

    // 2. Valid Atom 1.0
    let atom_xml = br#"<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Atom Example Feed</title>
      <link href="https://atom.example.org/"/>
      <updated>2026-11-01T15:30:00Z</updated>
      <id>urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6</id>
      <entry>
        <title>Atom Story</title>
        <link rel="alternate" href="/2026/atom-story"/>
        <id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
        <updated>2026-11-01T15:30:00Z</updated>
        <author><name>Bob Jones</name></author>
        <summary>Atom summary text</summary>
      </entry>
    </feed>"#;

    let parsed_atom = parse_feed_bytes(atom_xml, Some("application/atom+xml"), &base_url, 200)
        .expect("parse atom");
    assert_eq!(parsed_atom.entries.len(), 1);
    assert_eq!(
        parsed_atom.entries[0].canonical_url,
        "https://atom.example.org/2026/atom-story"
    );
    assert_eq!(parsed_atom.entries[0].author.as_deref(), Some("Bob Jones"));

    // 3. Valid RSS 1.0 / RDF
    let rdf_xml = br#"<?xml version="1.0" encoding="UTF-8"?>
    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
             xmlns="http://purl.org/rss/1.0/">
      <channel rdf:about="https://rdf.example.org/">
        <title>RDF Feed</title>
        <link>https://rdf.example.org/</link>
        <description>RSS 1.0 test</description>
      </channel>
      <item rdf:about="https://rdf.example.org/post-1">
        <title>RDF Item 1</title>
        <link>https://rdf.example.org/post-1</link>
        <description>RDF summary</description>
      </item>
    </rdf:RDF>"#;

    let parsed_rdf = parse_feed_bytes(rdf_xml, None, &base_url, 200).expect("parse rdf");
    assert_eq!(parsed_rdf.entries.len(), 1);
    assert_eq!(parsed_rdf.entries[0].title, "RDF Item 1");

    // 4. Entry limit truncation
    let truncated = parse_feed_bytes(rss_xml, None, &base_url, 1).expect("parse truncated");
    assert_eq!(truncated.entries_seen, 3);
    assert_eq!(truncated.entries_truncated, 2);
    assert_eq!(truncated.entries.len(), 1);

    // 5. Non-UTF8 charset (ISO-8859-1 / windows-1252)
    let latin1_xml = b"<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><rss version=\"2.0\"><channel><title>Caf\xe9 News</title><item><title>Caf\xe9 Story</title><link>https://news.example.com/cafe</link></item></channel></rss>";
    let parsed_latin1 = parse_feed_bytes(
        latin1_xml,
        Some("application/rss+xml; charset=ISO-8859-1"),
        &base_url,
        200,
    )
    .expect("parse latin1");
    assert_eq!(parsed_latin1.entries[0].title, "Café Story");

    // 6. Reject invalid XML and XXE `<!ENTITY` declarations
    assert!(parse_feed_bytes(b"not xml at all", None, &base_url, 200).is_err());
    let xxe_xml = br#"<?xml version="1.0"?>
    <!DOCTYPE foo [ <!ENTITY xxe SYSTEM "file:///etc/passwd"> ]>
    <rss version="2.0"><channel><title>&xxe;</title></channel></rss>"#;
    assert!(parse_feed_bytes(xxe_xml, None, &base_url, 200).is_err());
}

#[test]
fn test_canonical_url_resolution() {
    let base = Url::parse("https://blog.example.com/dir/feed.xml").unwrap();
    let c1 = canonicalize_article_url(
        "../posts/hello-world?utm_source= newsletter &page=2#top",
        Some(&base),
    )
    .unwrap();
    assert_eq!(c1, "https://blog.example.com/posts/hello-world?page=2");

    // Private IP in article link must be rejected
    assert!(canonicalize_article_url("http://127.0.0.1/secret", Some(&base)).is_err());
}

// =============================================================================
// 4. HTTP Fetching Tests (Section 16.2)
// =============================================================================

#[derive(Clone)]
struct MockHttpState {
    hits: Arc<AtomicUsize>,
}

async fn spawn_mock_http_server() -> (SocketAddr, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let state = MockHttpState { hits: hits.clone() };

    let app = Router::new()
        .route(
            "/ok-conditional",
            get(
                |State(st): State<MockHttpState>, headers: HeaderMap| async move {
                    st.hits.fetch_add(1, Ordering::SeqCst);
                    if headers
                        .get(header::IF_NONE_MATCH)
                        .and_then(|v| v.to_str().ok())
                        == Some("\"v1-etag\"")
                    {
                        return (
                            StatusCode::NOT_MODIFIED,
                            [(header::ETAG, "\"v1-etag\"")],
                            "",
                        )
                            .into_response();
                    }
                    (
                        StatusCode::OK,
                        [
                            (header::ETAG, "\"v1-etag\""),
                            (header::LAST_MODIFIED, "Sun, 01 Nov 2026 10:00:00 GMT"),
                        ],
                        "<rss version=\"2.0\"><channel><title>OK</title></channel></rss>",
                    )
                        .into_response()
                },
            ),
        )
        .route(
            "/redirect-valid",
            get(|| async {
                (
                    StatusCode::MOVED_PERMANENTLY,
                    [(header::LOCATION, "/ok-conditional")],
                    "",
                )
            }),
        )
        .route(
            "/redirect-ssrf",
            get(|| async {
                (
                    StatusCode::FOUND,
                    [(header::LOCATION, "http://127.0.0.1:8080/internal")],
                    "",
                )
            }),
        )
        .route(
            "/flaky-429",
            get(|State(st): State<MockHttpState>| async move {
                let count = st.hits.fetch_add(1, Ordering::SeqCst) + 1;
                if count < 3 {
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        [(header::RETRY_AFTER, "0")],
                        "rate limited",
                    )
                        .into_response()
                } else {
                    (StatusCode::OK, "recovered after 429").into_response()
                }
            }),
        )
        .route(
            "/always-500",
            get(|State(st): State<MockHttpState>| async move {
                st.hits.fetch_add(1, Ordering::SeqCst);
                (StatusCode::INTERNAL_SERVER_ERROR, "server error")
            }),
        )
        .route(
            "/oversized",
            get(|| async {
                let big = vec![b'a'; 128 * 1024];
                (StatusCode::OK, big)
            }),
        )
        .route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                (StatusCode::OK, "too slow")
            }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (addr, hits)
}

#[tokio::test]
async fn test_http_fetching_conditional_redirects_retries_and_limits() {
    let (addr, hits) = spawn_mock_http_server().await;
    let resolver = FixtureResolver::new_local_fixture("fixture.example.com", addr);
    let base = format!("http://fixture.example.com:{}", addr.port());

    let mut opts = FetchRequestOptions {
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(5),
        max_response_bytes: 64 * 1024,
        max_redirects: 5,
        max_attempts: 3,
        retry_initial_backoff: Duration::from_millis(10),
        retry_max_backoff: Duration::from_millis(50),
        etag: None,
        last_modified: None,
        accept_header: "*/*",
    };

    // 1. HTTP 200 + ETag/Last-Modified
    let out1 = fetch_url_with_retries(&format!("{base}/ok-conditional"), &opts, &resolver)
        .await
        .expect("fetch 200");
    match out1 {
        FetchOutcome::Fetched {
            http_status,
            etag,
            last_modified,
            ..
        } => {
            assert_eq!(http_status, 200);
            assert_eq!(etag.as_deref(), Some("\"v1-etag\""));
            assert!(last_modified.is_some());
            opts.etag = etag;
        }
        _ => panic!("expected Fetched"),
    }

    // 2. Conditional HTTP 304 Not Modified
    let out2 = fetch_url_with_retries(&format!("{base}/ok-conditional"), &opts, &resolver)
        .await
        .expect("fetch 304");
    assert!(matches!(
        out2,
        FetchOutcome::NotModified {
            http_status: 304,
            ..
        }
    ));

    // 3. Valid HTTP 301 redirect
    opts.etag = None;
    let out3 = fetch_url_with_retries(&format!("{base}/redirect-valid"), &opts, &resolver)
        .await
        .expect("redirect valid");
    assert!(matches!(
        out3,
        FetchOutcome::Fetched {
            http_status: 200,
            ..
        }
    ));

    // 4. Public-to-private redirect SSRF block
    let err_ssrf = fetch_url_with_retries(&format!("{base}/redirect-ssrf"), &opts, &resolver)
        .await
        .expect_err("redirect to 127.0.0.1 must fail");
    assert_eq!(err_ssrf.category, FetchErrorCategory::SsrfBlocked);

    // 5. HTTP 429 with Retry-After recovery
    hits.store(0, Ordering::SeqCst);
    let out_429 = fetch_url_with_retries(&format!("{base}/flaky-429"), &opts, &resolver)
        .await
        .expect("recover after 429");
    assert!(matches!(
        out_429,
        FetchOutcome::Fetched {
            http_status: 200,
            ..
        }
    ));
    assert_eq!(hits.load(Ordering::SeqCst), 3);

    // 6. HTTP 500 retry exhaustion
    hits.store(0, Ordering::SeqCst);
    let err_500 = fetch_url_with_retries(&format!("{base}/always-500"), &opts, &resolver)
        .await
        .expect_err("exhaust 500 retries");
    assert_eq!(err_500.category, FetchErrorCategory::HttpServerError);
    assert_eq!(hits.load(Ordering::SeqCst), 3);

    // 7. Oversized streaming response rejected
    let err_big = fetch_url_with_retries(&format!("{base}/oversized"), &opts, &resolver)
        .await
        .expect_err("reject oversized");
    assert_eq!(err_big.category, FetchErrorCategory::ResponseTooLarge);

    // 8. Request timeout enforced
    let mut timeout_opts = opts.clone();
    timeout_opts.request_timeout = Duration::from_millis(80);
    timeout_opts.max_attempts = 1;
    let err_timeout = fetch_url_with_retries(&format!("{base}/slow"), &timeout_opts, &resolver)
        .await
        .expect_err("timeout");
    assert_eq!(err_timeout.category, FetchErrorCategory::Timeout);
}

// =============================================================================
// 5. Article Extraction, Sanitization, Hashing & Fallbacks (Section 16.5)
// =============================================================================

#[test]
fn test_article_extraction_sanitization_and_content_hashing() {
    let doc_url = Url::parse("https://publisher.example.com/2026/rust-architecture").unwrap();
    let sample_html = r#"
    <!DOCTYPE html>
    <html>
      <head>
        <title>Rust Architecture Deep Dive</title>
        <style>body { color: red; }</style>
        <script> evilTracker(); </script>
      </head>
      <body>
        <nav><a href="/home">Home Navigation</a></nav>
        <div class="cookie-banner">Accept all cookies</div>
        <article>
          <h1>Rust Architecture Deep Dive</h1>
          <p onclick="stealCookies()">Quire is a self-hosted RSS aggregation and publication engine written in Rust.</p>
          <p>It combines asynchronous feed fetching with SQLite persistence and full-text readability extraction so readers can enjoy clean articles without clutter.</p>
          <h2>Key Capabilities</h2>
          <ul>
            <li>Asynchronous HTTP polling with SSRF defenses</li>
            <li>Readability extraction and HTML sanitization</li>
          </ul>
          <blockquote>Simplicity is a prerequisite for reliability.</blockquote>
          <pre><code>cargo test --all</code></pre>
          <img src="https://publisher.example.com/diagram.png" alt="Architecture Pipeline Diagram">
          <a href="/relative-guide" target="_blank">Read the guide</a>
        </article>
        <aside class="newsletter-signup"><form><input type="email"></form></aside>
        <footer>Copyright 2026</footer>
      </body>
    </html>
    "#;

    let (sanitized, hash1) =
        extract_readable_html_from_str(sample_html, &doc_url).expect("extract readable html");

    assert!(sanitized.contains("Quire is a self-hosted RSS aggregation"));
    assert!(sanitized.contains("<ul>"));
    assert!(sanitized.contains("<blockquote>"));
    assert!(sanitized.contains("<pre><code>cargo test --all</code></pre>"));
    assert!(sanitized.contains("[Image: Architecture Pipeline Diagram]"));
    assert!(sanitized.contains("https://publisher.example.com/relative-guide"));
    assert!(!sanitized.contains("<script"));
    assert!(!sanitized.contains("evilTracker"));
    assert!(!sanitized.contains("onclick"));
    assert!(!sanitized.contains("<form"));

    // Content hash stability across insignificant whitespace changes
    let hash2 = compute_content_hash(&format!("  \n  {sanitized}  \t "));
    assert_eq!(hash1, hash2);

    // Empty HTML fails extraction cleanly
    assert!(extract_readable_html_from_str("   ", &doc_url).is_err());

    // Sanitizer directly strips dangerous payloads
    let dirty = r#"<div><p>Safe text</p><script>alert(1)</script><a href="javascript:evil()">bad link</a></div>"#;
    let cleaned = sanitize_article_html(dirty, Some(&doc_url));
    assert!(cleaned.contains("<p>Safe text</p>"));
    assert!(!cleaned.contains("script"));
    assert!(!cleaned.contains("javascript:"));

    // Summary fallback sanitization
    let fb = sanitize_summary_fallback("Plain text summary & notes", Some(&doc_url)).unwrap();
    assert_eq!(fb, "<p>Plain text summary &amp; notes</p>");
}

// =============================================================================
// 6. Deduplication, Cross-Feed Associations, and Safe Feed Deletion (Section 16.4 & 26.4)
// =============================================================================

#[tokio::test]
async fn test_deduplication_guid_changes_and_cross_feed_safe_deletion() {
    let (_tmp, state) = setup_test_state().await;

    let feed_a = feeds::create_feed(
        &state.db,
        NewFeedInput {
            name: "Feed A".to_string(),
            feed_url: "https://example.com/feed-a.xml".to_string(),
            enabled: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let feed_b = feeds::create_feed(
        &state.db,
        NewFeedInput {
            name: "Feed B".to_string(),
            feed_url: "https://example.org/feed-b.xml".to_string(),
            enabled: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let entry_v1 = NormalizedFeedEntry {
        external_guid: Some("guid-1".to_string()),
        canonical_url: "https://shared.example.com/story-1".to_string(),
        original_source_url: "https://shared.example.com/story-1".to_string(),
        title: "Shared Story".to_string(),
        author: Some("Author A".to_string()),
        summary: Some("Initial summary".to_string()),
        published_at: Some("2026-11-01T10:00:00Z".to_string()),
        entry_updated_at: None,
    };

    // 1. Initial insert on Feed A
    let stats1 =
        persist_discovered_entries(&state.db, feed_a.id, std::slice::from_ref(&entry_v1), 3)
            .await
            .unwrap();
    assert_eq!(stats1.inserted, 1);
    assert_eq!(stats1.extraction_queued, 1);

    // 2. Repeated fetch of identical entry -> Unchanged, no duplicate
    let stats2 =
        persist_discovered_entries(&state.db, feed_a.id, std::slice::from_ref(&entry_v1), 3)
            .await
            .unwrap();
    assert_eq!(stats2.inserted, 0);
    assert_eq!(stats2.unchanged, 1);

    // 3. Publisher changes GUID while keeping the same canonical URL -> Updated, no duplicate
    let mut entry_guid_changed = entry_v1.clone();
    entry_guid_changed.external_guid = Some("guid-1-revised".to_string());
    let stats3 = persist_discovered_entries(&state.db, feed_a.id, &[entry_guid_changed], 3)
        .await
        .unwrap();
    assert_eq!(stats3.inserted, 0);
    assert_eq!(stats3.updated, 1);

    // 4. Publisher reuses an old GUID for a brand-new distinct URL -> Inserts second article without merging
    let entry_reused_guid = NormalizedFeedEntry {
        external_guid: Some("guid-1-revised".to_string()),
        canonical_url: "https://shared.example.com/story-2-distinct".to_string(),
        original_source_url: "https://shared.example.com/story-2-distinct".to_string(),
        title: "Distinct Story 2".to_string(),
        author: None,
        summary: None,
        published_at: None,
        entry_updated_at: None,
    };
    let stats4 = persist_discovered_entries(&state.db, feed_a.id, &[entry_reused_guid], 3)
        .await
        .unwrap();
    assert_eq!(stats4.inserted, 1);

    // 5. Feed B discovers the same canonical URL `story-1` -> Recorded in `article_feed_sources` without duplicating `articles`
    let mut entry_from_b = entry_v1.clone();
    entry_from_b.external_guid = Some("feed-b-guid-1".to_string());
    let stats5 = persist_discovered_entries(&state.db, feed_b.id, &[entry_from_b], 3)
        .await
        .unwrap();
    assert_eq!(stats5.inserted, 0);

    let all_articles = articles::list_recent_articles(&state.db, 50).await.unwrap();
    assert_eq!(all_articles.len(), 2);

    let story1 = all_articles
        .iter()
        .find(|a| a.canonical_url == "https://shared.example.com/story-1")
        .unwrap();
    let sources = articles::list_article_sources(&state.db, story1.id)
        .await
        .unwrap();
    assert_eq!(sources.len(), 2);

    // 6. Deleting Feed A must reassign `story-1` to Feed B (preserving the shared article)
    //    while deleting `story-2-distinct` (which belonged exclusively to Feed A).
    feeds::delete_feed(&state.db, feed_a.id).await.unwrap();

    let remaining = articles::list_recent_articles(&state.db, 50).await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(
        remaining[0].canonical_url,
        "https://shared.example.com/story-1"
    );
    assert_eq!(remaining[0].feed_id, feed_b.id);
}

// =============================================================================
// 7. Phase 1 -> Phase 2 Migration & Recovery Tests (Section 9.8 & 16.7)
// =============================================================================

#[tokio::test]
async fn test_phase1_to_phase2_migration_preserves_existing_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path: PathBuf = temp_dir.path().join("upgrade_test.db");
    let db_url = format!("sqlite://{}", db_path.display());

    // Create a raw SQLite pool and apply ONLY 0001_initial_schema.sql
    let opts = SqliteConnectOptions::from_str(&db_url)
        .unwrap()
        .create_if_missing(true)
        .foreign_keys(true);
    let raw_pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();

    let migration_0001 = include_str!("../migrations/0001_initial_schema.sql");
    sqlx::raw_sql(migration_0001)
        .execute(&raw_pool)
        .await
        .unwrap();

    // Seed Phase 1 data (including an article with Phase 1 status `'extracted'`)
    sqlx::query("INSERT INTO feeds (id, name, feed_url) VALUES (1, 'Phase 1 Feed', 'https://p1.example.com/rss.xml')")
        .execute(&raw_pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO articles (id, feed_id, external_guid, canonical_url, title, extracted_content, extraction_status)
         VALUES (10, 1, 'p1-guid', 'https://p1.example.com/post-1', 'Phase 1 Post', '<p>Hello</p>', 'extracted')",
    )
    .execute(&raw_pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO settings (key, value) VALUES ('timezone', 'UTC')")
        .execute(&raw_pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO editions (id, title, publication_date, status) VALUES (5, 'Daily 1', '2026-11-01', 'completed')")
        .execute(&raw_pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO edition_articles (edition_id, article_id, position) VALUES (5, 10, 0)",
    )
    .execute(&raw_pool)
    .await
    .unwrap();
    raw_pool.close().await;

    // Now run `db::init_pool` which runs SQLx migrations (0001 + 0002)
    let upgraded_pool = db::init_pool(&db_url, temp_dir.path())
        .await
        .expect("upgrade pool");

    let health = db::check_health(&upgraded_pool).await.unwrap();
    assert!(health.ok);
    assert!(health.foreign_keys_enabled);

    let feed = feeds::get_feed_by_id(&upgraded_pool, 1).await.unwrap();
    assert_eq!(feed.name, "Phase 1 Feed");
    assert_eq!(feed.article_count, 1);

    let article = articles::get_article_by_id(&upgraded_pool, 10)
        .await
        .unwrap();
    assert_eq!(article.title, "Phase 1 Post");
    assert_eq!(article.extraction_status, "completed");

    let editions = quire::editions::list_editions(&upgraded_pool, 10)
        .await
        .unwrap();
    assert_eq!(editions.len(), 1);
    assert_eq!(editions[0].article_count, 1);

    upgraded_pool.close().await;
}

use std::str::FromStr;

// =============================================================================
// 8. Persistent Extraction Queue, Concurrency, Retries & Restart Recovery (Section 16.6 & 16.7)
// =============================================================================

#[tokio::test]
async fn test_persistent_extraction_queue_recovery_and_retry_schedule() {
    let (_tmp, state) = setup_test_state().await;

    let feed = feeds::create_feed(
        &state.db,
        NewFeedInput {
            name: "Queue Feed".to_string(),
            feed_url: "https://queue.example.com/rss.xml".to_string(),
            enabled: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Claim feed fetch and simulate crash while running
    let (_claimed_feed, _run_id) = try_claim_feed_fetch(&state.db, feed.id, "scheduled")
        .await
        .unwrap()
        .expect("claim feed");
    assert!(
        try_claim_feed_fetch(&state.db, feed.id, "manual")
            .await
            .unwrap()
            .is_none(),
        "duplicate active fetch must be prevented"
    );

    // Recover interrupted fetch on startup
    let rec_fetches = recover_interrupted_fetches(&state.db).await.unwrap();
    assert_eq!(rec_fetches, 1);
    let runs = list_feed_fetch_runs(&state.db, feed.id, 10).await.unwrap();
    assert_eq!(runs[0].status, "cancelled");

    // Persist an article and claim its extraction job
    let entry = NormalizedFeedEntry {
        external_guid: Some("q-1".to_string()),
        canonical_url: "https://queue.example.com/art-1".to_string(),
        original_source_url: "https://queue.example.com/art-1".to_string(),
        title: "Queued Article".to_string(),
        author: None,
        summary: None,
        published_at: None,
        entry_updated_at: None,
    };
    persist_discovered_entries(&state.db, feed.id, &[entry], 3)
        .await
        .unwrap();

    let job1 = claim_next_extraction_job(&state.db)
        .await
        .unwrap()
        .expect("claim job 1");
    assert_eq!(job1.attempts, 1);

    // Second worker attempting to claim simultaneously gets None
    assert!(
        claim_next_extraction_job(&state.db)
            .await
            .unwrap()
            .is_none()
    );

    // Simulate crash recovery while job1 was `processing`
    let rec_jobs = recover_interrupted_extractions(&state.db).await.unwrap();
    assert_eq!(rec_jobs, 1);

    // Claim again after recovery (attempts becomes 2) and record a transient failure
    let job2 = claim_next_extraction_job(&state.db)
        .await
        .unwrap()
        .expect("re-claim recovered job");
    assert_eq!(job2.attempts, 2);

    complete_extraction_job(
        &state.db,
        &job2,
        &ExtractionResult::Failed {
            error_message: "Temporary timeout".to_string(),
            retryable: true,
        },
    )
    .await
    .unwrap();

    let art_after_fail = articles::get_article_by_id(&state.db, job2.article_id)
        .await
        .unwrap();
    assert_eq!(art_after_fail.extraction_status, "pending");
    assert!(art_after_fail.next_extraction_retry_at.is_some());
}

// =============================================================================
// 9. End-to-End Deterministic Ingestion, Extraction, Web UI & Restart Test (Section 16.8 & 16.9)
// =============================================================================

async fn spawn_e2e_fixture_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();

    let rss_doc = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>Fixture Engineering Blog</title>
        <link>http://fixture.example.com:{port}/</link>
        <description>Deterministic E2E test feed</description>
        <item>
          <guid>e2e-post-1</guid>
          <title>Building Reliable Rust Ingestion Pipelines</title>
          <link>/posts/reliable-rust-ingestion?utm_source=rss</link>
          <author>Spencer Thayer</author>
          <description>Short RSS summary for post 1.</description>
          <pubDate>Sun, 01 Nov 2026 16:00:00 GMT</pubDate>
        </item>
        <item>
          <guid>e2e-post-fallback</guid>
          <title>Post With Broken Article Page</title>
          <link>/posts/broken-page</link>
          <description>&lt;p&gt;Usable feed summary fallback when full page 404s.&lt;/p&gt;</description>
          <pubDate>Sun, 01 Nov 2026 15:00:00 GMT</pubDate>
        </item>
      </channel>
    </rss>"#
    );

    let article_html = r#"<!DOCTYPE html>
    <html>
      <head><title>Building Reliable Rust Ingestion Pipelines</title></head>
      <body>
        <header>Site Header</header>
        <article>
          <h1>Building Reliable Rust Ingestion Pipelines</h1>
          <p>This full article body was extracted by dom_smoothie from the local fixture server during end-to-end testing.</p>
          <p>It includes multiple paragraphs of technical detail to ensure readability heuristics score the main article container cleanly.</p>
          <pre><code>SELECT * FROM articles WHERE extraction_status = 'completed';</code></pre>
        </article>
        <script>console.log("untrusted script removed");</script>
      </body>
    </html>"#;

    let app = Router::new()
        .route(
            "/feed.xml",
            get(move || {
                let body = rss_doc.clone();
                async move {
                    (
                        StatusCode::OK,
                        [(header::CONTENT_TYPE, "application/rss+xml; charset=utf-8")],
                        body,
                    )
                }
            }),
        )
        .route(
            "/posts/reliable-rust-ingestion",
            get(move || async move {
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                    article_html,
                )
            }),
        )
        .route(
            "/posts/broken-page",
            get(|| async { (StatusCode::NOT_FOUND, "not found") }),
        );

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test]
async fn test_end_to_end_ingestion_extraction_web_routes_and_restart() {
    let fixture_addr = spawn_e2e_fixture_server().await;
    let resolver = FixtureResolver::new_local_fixture("fixture.example.com", fixture_addr);

    let mut extra_env = HashMap::new();
    extra_env.insert("QUIRE_TIMEZONE".to_string(), "UTC".to_string());

    let (temp_dir, state) = setup_test_state_with_resolver(resolver.clone(), extra_env).await;
    let app = create_router(state.clone());

    // 1. Obtain CSRF cookie from GET /feeds
    let feeds_get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/feeds")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(feeds_get.status(), StatusCode::OK);
    let set_cookie = feeds_get
        .headers()
        .get(header::SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let cookie_pair = set_cookie.split(';').next().unwrap().to_string();
    let csrf_token = cookie_pair.split('=').nth(1).unwrap().to_string();

    // 2. Add a feed pointing to the local HTTP fixture server via POST /feeds
    let feed_url = format!(
        "http://fixture.example.com:{}/feed.xml",
        fixture_addr.port()
    );
    let encoded_url: String = url::form_urlencoded::byte_serialize(feed_url.as_bytes()).collect();
    let form_body = format!(
        "csrf_token={csrf_token}&name=Fixture+Blog&feed_url={encoded_url}&fetch_interval_minutes=15&max_entries_override=50&enabled=true"
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
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::SEE_OTHER);

    let configured_feeds = feeds::list_feeds(&state.db).await.unwrap();
    assert_eq!(configured_feeds.len(), 1);
    let feed_id = configured_feeds[0].id;

    // 3. Execute scheduler tick (fetches due feed + drains SQLite extraction queue)
    let (feeds_ran, extractions_ran) = state.coordinator.run_scheduler_tick().await.unwrap();
    assert_eq!(feeds_ran, 1);
    assert_eq!(extractions_ran, 2);

    // 4. Verify persisted articles and full-text vs fallback extraction
    let paged = articles::query_articles_paginated(
        &state.db,
        &ArticleFilter {
            feed_id: Some(feed_id),
            extraction_status: None,
            published_date: None,
            discovered_date: None,
            page: 1,
            page_size: 25,
        },
    )
    .await
    .unwrap();
    assert_eq!(paged.total_count, 2);

    let full_art = paged
        .items
        .iter()
        .find(|a| a.title == "Building Reliable Rust Ingestion Pipelines")
        .expect("full article");
    assert_eq!(full_art.extraction_status, "completed");
    assert!(full_art.has_extracted_content);

    let fallback_art = paged
        .items
        .iter()
        .find(|a| a.title == "Post With Broken Article Page")
        .expect("fallback article");
    assert_eq!(fallback_art.extraction_status, "fallback");

    // 5. Render GET /articles/{id} and verify sanitized HTML output
    let detail_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/articles/{}", full_art.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(detail_resp.status(), StatusCode::OK);
    let detail_html = String::from_utf8(
        detail_resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(detail_html.contains("extracted by dom_smoothie from the local fixture server"));
    assert!(!detail_html.contains("untrusted script removed"));

    // 6. Render GET /feeds/{id} and verify fetch history is displayed
    let feed_detail_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/feeds/{feed_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(feed_detail_resp.status(), StatusCode::OK);
    let feed_detail_html = String::from_utf8(
        feed_detail_resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(feed_detail_html.contains("Recent Fetch History"));
    assert!(feed_detail_html.contains("2 seen / +2 new"));

    // 7. Fetch the same feed again via `run_feed_now_and_wait` -> Verify 0 duplicates created
    assert!(
        state
            .coordinator
            .run_feed_now_and_wait(feed_id, "manual")
            .await
            .unwrap()
    );
    let paged_after = articles::query_articles_paginated(
        &state.db,
        &ArticleFilter {
            feed_id: None,
            extraction_status: None,
            published_date: None,
            discovered_date: None,
            page: 1,
            page_size: 25,
        },
    )
    .await
    .unwrap();
    assert_eq!(paged_after.total_count, 2);

    // 8. TestPOST /settings attempting to override environment-locked `QUIRE_TIMEZONE` -> 400 Bad Request
    let locked_settings_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/settings")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::from(format!(
                    "csrf_token={csrf_token}&timezone=Europe%2FLondon"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(locked_settings_resp.status(), StatusCode::BAD_REQUEST);

    // 9. Test POST /settings updating unlocked `fetch_interval_minutes` -> 303 See Other
    let valid_settings_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/settings")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header(header::COOKIE, &cookie_pair)
                .body(Body::from(format!(
                    "csrf_token={csrf_token}&fetch_interval_minutes=30&fetch_concurrency=6"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(valid_settings_resp.status(), StatusCode::SEE_OTHER);

    // 10. Simulate application restart by closing pool and re-initializing against the same SQLite file
    let db_url = state.config.database_url.clone();
    let data_dir = temp_dir.path().to_path_buf();
    state.db.close().await;

    let pool_restarted = db::init_pool(&db_url, &data_dir).await.unwrap();
    let counts_restarted = db::get_dashboard_counts(&pool_restarted).await.unwrap();
    assert_eq!(counts_restarted.total_feeds, 1);
    assert_eq!(counts_restarted.total_articles, 2);
    assert_eq!(counts_restarted.extracted_articles, 1);
    assert_eq!(counts_restarted.fallback_articles, 1);
    pool_restarted.close().await;
}
