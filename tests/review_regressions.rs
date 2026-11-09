//! Reviewer regression suite ported from `docs/reviews/phase-2-artifacts/review_tests.rs`.
//! In Stage 1, baseline defects are marked `#[ignore = "QUA-xxx: fixed in stage N"]`
//! so `cargo test --test review_regressions -- --ignored` reproduces the baseline
//! failures before remediation, and each stage un-ignores its tests as fixes land.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::{Command, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection};

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use flate2::Compression;
use flate2::write::GzEncoder;
use quire::articles::extractor::{ExtractionResult, fetch_and_extract_article};
use quire::articles::normalizer::canonicalize_article_url;
use quire::articles::sanitizer::sanitize_article_html;
use quire::articles::{
    claim_next_extraction_job, complete_extraction_job, persist_discovered_entries,
    recover_interrupted_extractions,
};
use quire::config::AppConfig;
use quire::db;
use quire::error::AppResult;
use quire::feeds::fetcher::{FetchErrorCategory, FetchRequestOptions, fetch_url_with_retries};
use quire::feeds::parser::{NormalizedFeedEntry, parse_feed_bytes};
use quire::feeds::validation::{HostResolver, SystemDnsResolver, is_disallowed_ip};
use quire::feeds::{self, NewFeedInput};
use quire::web::AppState;
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use url::Url;

/// Resolver that maps every hostname to the fixture address WITHOUT any IP
/// checks (models a public DNS answer pointing at the fixture).
struct AnyHost(SocketAddr);
impl HostResolver for AnyHost {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let a = self.0;
        Box::pin(async move { Ok(vec![a]) })
    }
}

/// Resolver that maps every hostname to the local fixture and permits exactly
/// that fixture address through the fetcher's test hook (models a public DNS
/// answer for tests that must reach the fixture).
struct FixtureHost(SocketAddr);
impl HostResolver for FixtureHost {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        let a = self.0;
        Box::pin(async move { Ok(vec![a]) })
    }

    fn allows_test_fixture_addr(&self, addr: &SocketAddr) -> bool {
        *addr == self.0
    }
}

async fn state_with(
    resolver: Arc<dyn HostResolver>,
    extra: &[(&str, &str)],
) -> (TempDir, AppState) {
    let tmp = TempDir::new().unwrap();
    let mut env = HashMap::new();
    env.insert(
        "QUIRE_DATA_DIR".to_string(),
        tmp.path().display().to_string(),
    );
    env.insert(
        "QUIRE_DATABASE_URL".to_string(),
        format!("sqlite://{}", tmp.path().join("r.db").display()),
    );
    for (k, v) in extra {
        env.insert(k.to_string(), v.to_string());
    }
    let cfg = AppConfig::from_map(&env).unwrap();
    let pool = db::init_pool(&cfg.database_url, &cfg.data_dir)
        .await
        .unwrap();
    (tmp, AppState::with_resolver(pool, cfg, resolver))
}

async fn new_feed(pool: &SqlitePool, url: &str) -> i64 {
    feeds::create_feed(
        pool,
        NewFeedInput {
            name: url.to_string(),
            feed_url: url.to_string(),
            enabled: true,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .id
}

fn entry(url: &str, guid: Option<&str>, summary: Option<&str>) -> NormalizedFeedEntry {
    NormalizedFeedEntry {
        external_guid: guid.map(str::to_string),
        canonical_url: url.to_string(),
        original_source_url: url.to_string(),
        title: format!("T {url}"),
        author: None,
        summary: summary.map(str::to_string),
        published_at: None,
        entry_updated_at: None,
    }
}

fn build_gzip_bomb_bytes(uncompressed_len: usize) -> Vec<u8> {
    let mut enc = GzEncoder::new(Vec::new(), Compression::best());
    let chunk = [0u8; 64 * 1024];
    let mut remaining = uncompressed_len;
    while remaining > 0 {
        let n = remaining.min(chunk.len());
        enc.write_all(&chunk[..n]).unwrap();
        remaining -= n;
    }
    enc.finish().unwrap()
}

struct Fixture {
    addr: SocketAddr,
    secret_hits: Arc<AtomicUsize>,
    hits: Arc<AtomicUsize>,
    max_inflight: Arc<AtomicUsize>,
}

async fn spawn_fixture() -> Fixture {
    let secret_hits = Arc::new(AtomicUsize::new(0));
    let hits = Arc::new(AtomicUsize::new(0));
    let inflight = Arc::new(AtomicUsize::new(0));
    let max_inflight = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let bomb = Arc::new(build_gzip_bomb_bytes(8 * 1024 * 1024));

    let sh = secret_hits.clone();
    let hits_slow = hits.clone();
    let (inf, mx) = (inflight.clone(), max_inflight.clone());
    let mut app = Router::new()
        .route(
            "/secret",
            get(move || {
                let sh = sh.clone();
                async move {
                    sh.fetch_add(1, Ordering::SeqCst);
                    "INTERNAL SECRET"
                }
            }),
        )
        .route(
            "/bomb",
            get(move || {
                let b = (*bomb).clone();
                async move { ([(header::CONTENT_ENCODING, "gzip")], b) }
            }),
        )
        .route(
            "/drip",
            get(|| async {
                let s = futures_util::stream::unfold(0u32, |n| async move {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    Some((Ok::<_, std::io::Error>(bytes_of(b"x")), n + 1))
                });
                Body::from_stream(s)
            }),
        )
        .route(
            "/a503",
            get({
                let hits_503 = hits.clone();
                move || {
                    let hits_503 = hits_503.clone();
                    async move {
                        hits_503.fetch_add(1, Ordering::SeqCst);
                        (StatusCode::SERVICE_UNAVAILABLE, "down")
                    }
                }
            }),
        )
        .route(
            "/pdf",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/pdf")],
                    b"%PDF-1.4\n1 0 obj << /Type /Catalog >> endobj\nstream\nBT /F1 12 Tf (Hello binary world this is a long body of pdf text that might look like prose to a readability scorer, with commas, and more commas, and sentences.) Tj ET\nendstream\n%%EOF".to_vec(),
                )
            }),
        )
        .route(
            "/slowfeed",
            get(move || {
                let (inf, mx) = (inf.clone(), mx.clone());
                let hits_slow = hits_slow.clone();
                async move {
                    hits_slow.fetch_add(1, Ordering::SeqCst);
                    let now = inf.fetch_add(1, Ordering::SeqCst) + 1;
                    mx.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(800)).await;
                    inf.fetch_sub(1, Ordering::SeqCst);
                    (
                        [(header::CONTENT_TYPE, "application/rss+xml")],
                        "<rss version=\"2.0\"><channel><title>s</title></channel></rss>",
                    )
                }
            }),
        );

    for (path, target) in [
        ("/r-loop", format!("http://127.0.0.1:{port}/secret")),
        ("/r-loop6", format!("http://[::1]:{port}/secret")),
        ("/r-mapped", format!("http://[::ffff:7f00:1]:{port}/secret")),
        ("/r-hex", format!("http://0x7f000001:{port}/secret")),
        ("/r-int", format!("http://2130706433:{port}/secret")),
        ("/r-short", format!("http://127.1:{port}/secret")),
        ("/r-localhost", format!("http://localhost:{port}/secret")),
        ("/r-lan", "http://192.168.1.1/".to_string()),
        (
            "/r-meta",
            "http://169.254.169.254/latest/meta-data/".to_string(),
        ),
        (
            "/r-nat64",
            format!("http://[64:ff9b::7f00:1]:{port}/secret"),
        ),
        ("/r-6to4", format!("http://[2002:7f00:1::]:{port}/secret")),
        ("/r-teredo", format!("http://[2001::1]:{port}/secret")),
        ("/r-fec0", format!("http://[fec0::1]:{port}/secret")),
        ("/r-3fff", format!("http://[3fff::1]:{port}/secret")),
        (
            "/r-mapped4",
            format!("http://[::ffff:127.0.0.1]:{port}/secret"),
        ),
    ] {
        app = app.route(
            path,
            get(move || {
                let t = target.clone();
                async move { (StatusCode::FOUND, [(header::LOCATION, t)], "") }
            }),
        );
    }

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Fixture {
        addr,
        secret_hits,
        hits,
        max_inflight,
    }
}

fn bytes_of(b: &'static [u8]) -> axum::body::Bytes {
    axum::body::Bytes::from_static(b)
}

fn opts() -> FetchRequestOptions {
    FetchRequestOptions {
        max_attempts: 1,
        connect_timeout: Duration::from_secs(2),
        request_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- SSRF ----

#[tokio::test]
async fn r01_redirects_to_restricted_literals_are_blocked() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let port = fx.addr.port();
    for path in [
        "/r-loop",
        "/r-loop6",
        "/r-mapped",
        "/r-hex",
        "/r-int",
        "/r-short",
        "/r-localhost",
        "/r-lan",
        "/r-meta",
        "/r-nat64",
        "/r-6to4",
        "/r-teredo",
        "/r-fec0",
        "/r-3fff",
        "/r-mapped4",
    ] {
        let url = format!("http://public.example.com:{port}{path}");
        let res = fetch_url_with_retries(&url, &opts(), &resolver).await;
        match res {
            Err(e) => {
                assert_eq!(e.category, FetchErrorCategory::SsrfBlocked, "{path}");
            }
            Ok(_) => panic!("{path}: redirect followed to restricted target"),
        }
    }
    assert_eq!(fx.secret_hits.load(Ordering::SeqCst), 0);
}

#[test]
fn r02_ipv6_embedded_ipv4_transition_prefixes_blocked() {
    let mut leaks = vec![];
    for s in [
        "64:ff9b::7f00:1",
        "64:ff9b::a9fe:a9fe",
        "2002:7f00:1::",
        "2002:c0a8:101::",
        "fec0::1",
        "2001::1",
        "3fff::1",
        "::ffff:127.0.0.1",
    ] {
        let ip: std::net::IpAddr = s.parse().unwrap();
        if !is_disallowed_ip(&ip) {
            leaks.push(s);
        }
    }
    assert!(leaks.is_empty(), "not blocked: {leaks:?}");
}

#[tokio::test]
async fn r03_system_resolver_handles_bracketed_ipv6_literal() {
    let r = SystemDnsResolver;
    let res = r.resolve("[2606:4700:4700::1111]", 80).await;
    assert!(
        res.is_ok(),
        "public IPv6 literal feed URLs cannot be resolved: {res:?}"
    );
}

#[tokio::test]
async fn r04_fetcher_trusts_resolver_output() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(AnyHost(fx.addr));
    let url = format!("http://rebind.example.com:{}/secret", fx.addr.port());
    let res = fetch_url_with_retries(&url, &opts(), &resolver).await;
    assert!(
        res.is_err(),
        "fetcher connected to 127.0.0.1 because the injected resolver did not filter"
    );
    assert_eq!(res.unwrap_err().category, FetchErrorCategory::SsrfBlocked);
    assert_eq!(fx.secret_hits.load(Ordering::SeqCst), 0);
}

// ------------------------------------------------------ resource limits ----

#[tokio::test]
async fn r05_gzip_bomb_is_bounded_by_decompressed_limit() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let mut o = opts();
    o.max_response_bytes = 1024 * 1024;
    o.request_timeout = Duration::from_secs(20);
    let res = fetch_url_with_retries(
        &format!("http://pub.example.com:{}/bomb", fx.addr.port()),
        &o,
        &resolver,
    )
    .await;
    let e = res.expect_err("bomb must be rejected");
    assert_eq!(e.category, FetchErrorCategory::ResponseTooLarge);
}

#[tokio::test]
async fn r06_slow_drip_body_times_out() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let mut o = opts();
    o.request_timeout = Duration::from_secs(2);
    let t = Instant::now();
    let res = fetch_url_with_retries(
        &format!("http://pub.example.com:{}/drip", fx.addr.port()),
        &o,
        &resolver,
    )
    .await;
    let el = t.elapsed();
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().category, FetchErrorCategory::Timeout);
    assert!(el < Duration::from_secs(4), "timeout not enforced: {el:?}");
}

// ---------------------------------------------------------------- XML ------

#[test]
fn r07_legit_feed_containing_public_quote_text_parses() {
    let xml = r#"<?xml version="1.0"?><rss version="2.0"><channel><title>News</title>
<item><title>IPO</title><link>https://news.example.com/ipo</link>
<description>The company went PUBLIC "today" after years of delay.</description></item>
</channel></rss>"#;
    let base = Url::parse("https://news.example.com/feed").unwrap();
    let res = parse_feed_bytes(xml.as_bytes(), Some("application/rss+xml"), &base, 50);
    assert!(res.is_ok(), "legit feed rejected: {:?}", res.err());
}

#[test]
fn r08_rss091_with_public_doctype_parses() {
    let xml = r#"<?xml version="1.0"?>
<!DOCTYPE rss PUBLIC "-//Netscape Communications//DTD RSS 0.91//EN" "http://my.netscape.com/publish/formats/rss-0.91.dtd">
<rss version="0.91"><channel><title>Old</title><link>https://old.example.com/</link><description>d</description>
<item><title>A</title><link>https://old.example.com/a</link></item></channel></rss>"#;
    let base = Url::parse("https://old.example.com/rss").unwrap();
    let res = parse_feed_bytes(xml.as_bytes(), None, &base, 50);
    assert!(res.is_ok(), "RSS 0.91 feed rejected: {:?}", res.err());
}

#[test]
fn r09_entity_declaration_past_64k_not_expanded() {
    let pad = "<!--".to_string() + &"p".repeat(70_000) + "-->";
    let xml = format!(
        r#"<?xml version="1.0"?>{pad}<!DOCTYPE rss [<!ENTITY a "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;"><!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;"><!ENTITY d "&c;&c;&c;&c;&c;&c;&c;&c;&c;&c;"><!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<rss version="2.0"><channel><title>&d;</title><item><title>&xxe;</title><link>https://x.example.com/a</link></item></channel></rss>"#
    );
    let base = Url::parse("https://x.example.com/rss").unwrap();
    let res = parse_feed_bytes(xml.as_bytes(), None, &base, 50);
    if let Ok(doc) = res {
        let t = doc.metadata.title.unwrap_or_default();
        assert!(t.len() < 1000, "entity expanded");
        assert!(
            !doc.entries.iter().any(|e| e.title.contains("root:")),
            "XXE resolved"
        );
    }
}

#[test]
fn r10_entry_limit_keeps_newest_entries() {
    let xml = r#"<rss version="2.0"><channel><title>c</title>
<item><title>old1</title><link>https://o.example.com/1</link><pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate></item>
<item><title>old2</title><link>https://o.example.com/2</link><pubDate>Tue, 02 Jan 2024 00:00:00 GMT</pubDate></item>
<item><title>new3</title><link>https://o.example.com/3</link><pubDate>Mon, 01 Jan 2026 00:00:00 GMT</pubDate></item>
<item><title>new4</title><link>https://o.example.com/4</link><pubDate>Tue, 02 Jan 2026 00:00:00 GMT</pubDate></item>
</channel></rss>"#;
    let base = Url::parse("https://o.example.com/rss").unwrap();
    let doc = parse_feed_bytes(xml.as_bytes(), None, &base, 2).unwrap();
    let titles: Vec<_> = doc.entries.iter().map(|e| e.title.clone()).collect();
    assert!(
        titles.contains(&"new4".to_string()),
        "newest entries dropped: {titles:?}"
    );
}

// ----------------------------------------------------------- normalizer ----

#[test]
fn r11_canonicalization_preserves_identity() {
    let a = canonicalize_article_url("https://e.example.com/article?id=123", None).unwrap();
    let b = canonicalize_article_url("https://e.example.com/article?id=456", None).unwrap();
    assert_ne!(a, b);
    let c =
        canonicalize_article_url("https://e.example.com/a?utm_source=x&id=1#frag", None).unwrap();
    assert_eq!(c, "https://e.example.com/a?id=1");
    let bare = canonicalize_article_url("https://e.example.com/p?123", None).unwrap();
    assert_eq!(
        bare, "https://e.example.com/p?123",
        "bare query key rewritten"
    );
}

// ----------------------------------------------------------------- HTML ----

#[test]
fn r12_sanitizer_blocks_xss_vectors() {
    let payload = r#"<p>ok</p><script>alert(1)</script><img src=x onerror="alert(1)">
<a href="javascript:alert(1)">j</a><a href="jav&#x09;ascript:alert(1)">j2</a>
<a href="data:text/html,<script>alert(1)</script>">d</a><iframe src="https://e.com"></iframe>
<svg onload=alert(1)><script>alert(1)</script></svg><math><mi xlink:href="javascript:alert(1)">m</mi></math>
<form action="https://evil"><input name=x></form><div style="background:url(javascript:alert(1))">s</div>
<style>body{display:none}</style><object data="x"></object><embed src="x"><base href="https://evil/">
<meta http-equiv="refresh" content="0;url=https://evil"><a href="https://ok.example.com" onclick="x()">fine</a>
<noscript><p title="</noscript><img src=x onerror=alert(1)>"></p></noscript>"#;
    let out = sanitize_article_html(
        payload,
        Some(&Url::parse("https://e.example.com/").unwrap()),
    );
    let lower = out.to_ascii_lowercase();
    for bad in [
        "<script",
        "onerror",
        "onload",
        "onclick",
        "javascript:",
        "data:text",
        "<iframe",
        "<svg",
        "<math",
        "<form",
        "<input",
        "style=",
        "<style",
        "<object",
        "<embed",
        "<base",
        "<meta",
        "<img",
    ] {
        assert!(!lower.contains(bad), "sanitized output contains {bad}");
    }
    assert!(lower.contains("https://ok.example.com"));
}

// ------------------------------------------------------ extraction ----------

#[tokio::test]
async fn r13_transient_article_failure_with_summary_is_retried_not_finalized() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let url = format!("http://pub.example.com:{}/a503", fx.addr.port());
    let res = fetch_and_extract_article(&url, Some("Feed summary text"), &opts(), &resolver).await;
    assert!(
        matches!(
            res,
            ExtractionResult::Failed {
                retryable: true,
                ..
            }
        ),
        "transient 503 was finalized as {res:?}"
    );
}

#[tokio::test]
async fn r14_transient_failure_end_to_end_queue_state() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let (_t, st) = state_with(
        resolver,
        &[
            ("QUIRE_FETCH_MAX_ATTEMPTS", "3"),
            ("QUIRE_FETCH_RETRY_INITIAL_SECONDS", "1"),
            ("QUIRE_FETCH_RETRY_MAX_SECONDS", "1"),
        ],
    )
    .await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    let url = format!("http://pub.example.com:{}/a503", fx.addr.port());
    persist_discovered_entries(&st.db, fid, &[entry(&url, Some("g"), Some("Summary"))], 3)
        .await
        .unwrap();
    st.coordinator.drain_eligible_extractions().await.unwrap();
    let (status, _attempts, _art_status, _retry_at): (String, i64, String, Option<String>) =
        sqlx::query_as(
            "SELECT j.status, j.attempts, a.extraction_status, a.next_extraction_retry_at FROM article_extraction_jobs j JOIN articles a ON a.id=j.article_id",
        )
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(
        status, "queued",
        "transient failure was not scheduled for retry"
    );
}

#[tokio::test]
async fn r15_non_html_content_type_not_marked_full_text() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let url = format!("http://pub.example.com:{}/pdf", fx.addr.port());
    let res = fetch_and_extract_article(&url, None, &opts(), &resolver).await;
    assert!(
        !matches!(res, ExtractionResult::Completed { .. }),
        "PDF bytes extracted as article"
    );
}

// --------------------------------------------------------------- queue -----

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn r16_concurrent_claims_are_unique_and_error_free() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    let entries: Vec<_> = (0..300)
        .map(|i| {
            entry(
                &format!("https://a.example.com/{i}"),
                Some(&format!("g{i}")),
                None,
            )
        })
        .collect();
    persist_discovered_entries(&st.db, fid, &entries, 3)
        .await
        .unwrap();

    let mut handles = vec![];
    for _ in 0..16 {
        let pool = st.db.clone();
        handles.push(tokio::spawn(async move {
            let mut ids = vec![];
            let mut errs = vec![];
            loop {
                match claim_next_extraction_job(&pool).await {
                    Ok(Some(j)) => ids.push(j.job_id),
                    Ok(None) => break,
                    Err(e) => {
                        errs.push(format!("{e:?}"));
                        if errs.len() > 50 {
                            break;
                        }
                    }
                }
            }
            (ids, errs)
        }));
    }
    let mut all = vec![];
    let mut errs = vec![];
    for h in handles {
        let (i, e) = h.await.unwrap();
        all.extend(i);
        errs.extend(e);
    }
    let uniq: HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), uniq.len(), "duplicate claims");
    assert_eq!(uniq.len(), 300);
    assert!(errs.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn r17_concurrent_feed_persists_with_queue_writes_do_not_fail() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let mut fids = vec![];
    for i in 0..6 {
        fids.push(new_feed(&st.db, &format!("https://feed{i}.example.com/rss")).await);
    }
    let seed: Vec<_> = (0..400)
        .map(|i| entry(&format!("https://seed.example.com/{i}"), None, None))
        .collect();
    persist_discovered_entries(&st.db, fids[0], &seed, 3)
        .await
        .unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut workers = vec![];
    for _ in 0..4 {
        let pool = st.db.clone();
        let stop = stop.clone();
        workers.push(tokio::spawn(async move {
            let mut n = 0;
            while !stop.load(Ordering::SeqCst) {
                if let Ok(Some(j)) = claim_next_extraction_job(&pool).await {
                    let _ = complete_extraction_job(
                        &pool,
                        &j,
                        &ExtractionResult::Failed {
                            error_message: "x".into(),
                            retryable: false,
                            retry_after: None,
                            interim_fallback: None,
                            cancelled: false,
                        },
                    )
                    .await;
                    n += 1;
                } else {
                    tokio::task::yield_now().await;
                }
            }
            n
        }));
    }

    let mut persists = vec![];
    for round in 0..3 {
        for (k, fid) in fids.iter().enumerate() {
            let pool = st.db.clone();
            let fid = *fid;
            let entries: Vec<_> = (0..150)
                .map(|i| {
                    entry(
                        &format!("https://f{k}.example.com/r{round}/{i}"),
                        Some(&format!("g{round}-{i}")),
                        None,
                    )
                })
                .collect();
            persists.push(tokio::spawn(async move {
                persist_discovered_entries(&pool, fid, &entries, 3).await
            }));
        }
    }
    let mut failures = vec![];
    for p in persists {
        if let Err(e) = p.await.unwrap() {
            failures.push(format!("{e:?}"));
        }
    }
    stop.store(true, Ordering::SeqCst);
    for w in workers {
        let _ = w.await.unwrap();
    }
    assert!(
        failures.is_empty(),
        "feed batches lost to SQLITE_BUSY: {}",
        failures.len()
    );
}

#[tokio::test]
async fn r18_stuck_processing_job_blocks_manual_retry_until_restart() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry("https://a.example.com/1", None, None)],
        3,
    )
    .await
    .unwrap();
    let j = claim_next_extraction_job(&st.db).await.unwrap().unwrap();
    let token = j.claim_token.clone();
    let job_id = j.job_id;
    let article_id = j.article_id;
    let panicked = tokio::spawn(async { panic!("injected extraction panic") });
    assert!(panicked.await.unwrap_err().is_panic());
    let released = quire::articles::release_extraction_job(
        &st.db,
        job_id,
        article_id,
        &token,
        false,
        Some("injected extraction panic"),
    )
    .await
    .unwrap();
    assert!(released, "panic release did not match the claim token");
    sqlx::query(
        "UPDATE article_extraction_jobs SET next_eligible_at = '2000-01-01T00:00:00Z' WHERE id = ?",
    )
    .bind(job_id)
    .execute(&st.db)
    .await
    .unwrap();
    let again = claim_next_extraction_job(&st.db)
        .await
        .unwrap()
        .expect("released job is claimable without a process restart");
    assert_ne!(again.claim_token, token);
}

#[tokio::test]
async fn r19_stale_worker_cannot_overwrite_reclaimed_job() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry("https://a.example.com/1", None, None)],
        3,
    )
    .await
    .unwrap();
    let old = claim_next_extraction_job(&st.db).await.unwrap().unwrap();
    recover_interrupted_extractions(&st.db).await.unwrap();
    let new = claim_next_extraction_job(&st.db).await.unwrap().unwrap();
    complete_extraction_job(
        &st.db,
        &new,
        &ExtractionResult::Completed {
            sanitized_html: "<p>fresh</p>".into(),
            content_hash: "h".into(),
            final_url: "https://a.example.com/1".into(),
        },
    )
    .await
    .unwrap();
    let _ = complete_extraction_job(
        &st.db,
        &old,
        &ExtractionResult::Failed {
            error_message: "stale".into(),
            retryable: false,
            retry_after: None,
            interim_fallback: None,
            cancelled: false,
        },
    )
    .await;
    let (js, _as): (String, String) = sqlx::query_as(
        "SELECT j.status, a.extraction_status FROM article_extraction_jobs j JOIN articles a ON a.id=j.article_id",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert_eq!(js, "completed", "stale worker overwrote newer result");
}

#[tokio::test]
async fn r20_crash_loop_job_attempts_are_bounded() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry("https://a.example.com/1", None, None)],
        3,
    )
    .await
    .unwrap();
    let mut claims = 0;
    for _ in 0..10 {
        if claim_next_extraction_job(&st.db).await.unwrap().is_some() {
            claims += 1;
        }
        recover_interrupted_extractions(&st.db).await.unwrap();
    }
    assert!(claims <= 3, "poison job reclaimed {claims} times");
    let (status,): (String,) = sqlx::query_as("SELECT status FROM article_extraction_jobs")
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(status, "failed");
}

// -------------------------------------------------------- feed deletion ----

#[tokio::test]
async fn r21_feed_delete_preserves_edition_membership() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://feed.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry("https://a.example.com/1", None, None)],
        3,
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO editions (title, publication_date) VALUES ('E', '2026-11-01')")
        .execute(&st.db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO edition_articles (edition_id, article_id, position) VALUES (1, 1, 0)")
        .execute(&st.db)
        .await
        .unwrap();
    feeds::delete_feed(&st.db, fid).await.unwrap();
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM edition_articles")
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(n, 1, "deleting a feed removed an edition member");
    let (feed_id, name): (Option<i64>, String) = sqlx::query_as(
        "SELECT a.feed_id, COALESCE(f.name, '(removed feed)')
         FROM articles a LEFT JOIN feeds f ON f.id = a.feed_id
         WHERE a.id = 1",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert!(feed_id.is_none());
    assert_eq!(name, "(removed feed)");
}

#[tokio::test]
async fn r22_feed_delete_reassignment_guid_conflict() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let a = new_feed(&st.db, "https://feeda.example.com/rss").await;
    let b = new_feed(&st.db, "https://feedb.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        b,
        &[entry("https://x.example.com/x", Some("g"), None)],
        3,
    )
    .await
    .unwrap();
    persist_discovered_entries(
        &st.db,
        a,
        &[entry("https://x.example.com/y", Some("h"), None)],
        3,
    )
    .await
    .unwrap();
    persist_discovered_entries(
        &st.db,
        b,
        &[entry("https://x.example.com/y", Some("g"), None)],
        3,
    )
    .await
    .unwrap();
    let res = feeds::delete_feed(&st.db, a).await;
    assert!(res.is_ok(), "feed cannot be deleted: {res:?}");
}

// ------------------------------------------------------- concurrency ------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r23_manual_fetch_all_respects_fetch_concurrency() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let (_t, st) = state_with(
        resolver,
        &[
            ("QUIRE_FETCH_CONCURRENCY", "2"),
            ("QUIRE_FETCH_ENABLED", "false"),
        ],
    )
    .await;
    for i in 0..10 {
        new_feed(
            &st.db,
            &format!("http://pub{i}.example.com:{}/slowfeed", fx.addr.port()),
        )
        .await;
    }
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(100));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _started = st
        .coordinator
        .trigger_manual_fetch_all_enabled()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let mx = fx.max_inflight.load(Ordering::SeqCst);
    let hits = fx.hits.load(Ordering::SeqCst);
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(6), handle).await;
    assert!(hits > 0, "fetch-all never reached the fixture");
    assert!(mx <= 2, "fetch_concurrency=2 exceeded: {mx}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r24_default_fetch_concurrency_persists_only() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let mut fids = vec![];
    for i in 0..4 {
        fids.push(new_feed(&st.db, &format!("https://feed{i}.example.com/rss")).await);
    }
    let mut failed = 0;
    for round in 0..10 {
        let mut hs = vec![];
        for (k, fid) in fids.iter().enumerate() {
            let pool = st.db.clone();
            let fid = *fid;
            let entries: Vec<_> = (0..30)
                .map(|i| {
                    entry(
                        &format!("https://f{k}.example.com/r{round}/{i}"),
                        Some(&format!("g{round}-{i}")),
                        Some("s"),
                    )
                })
                .collect();
            hs.push(tokio::spawn(async move {
                persist_discovered_entries(&pool, fid, &entries, 3).await
            }));
        }
        for h in hs {
            if h.await.unwrap().is_err() {
                failed += 1;
            }
        }
    }
    assert_eq!(failed, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r25_scheduler_tick_e2e_concurrent_feeds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let app = Router::new().route(
        "/feed/{n}",
        get(move |axum::extract::Path(n): axum::extract::Path<u32>| {
            let hits_route = hits_route.clone();
            async move {
            hits_route.fetch_add(1, Ordering::SeqCst);
            let mut items = String::new();
            for i in 0..40 {
                items.push_str(&format!("<item><guid>n{n}-{i}</guid><title>t{i}</title><link>https://site{n}.example.com/a/{i}</link><description>summary {i}</description></item>"));
            }
            (
                [(header::CONTENT_TYPE, "application/rss+xml")],
                format!("<rss version=\"2.0\"><channel><title>f{n}</title>{items}</channel></rss>"),
            )
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(addr));
    let (_t, st) = state_with(
        resolver,
        &[
            ("QUIRE_FETCH_CONCURRENCY", "8"),
            ("QUIRE_EXTRACTION_CONCURRENCY", "1"),
        ],
    )
    .await;
    for n in 0..8 {
        new_feed(
            &st.db,
            &format!("http://feeds.example.com:{}/feed/{n}", addr.port()),
        )
        .await;
    }
    let ids = feeds::list_due_feed_ids(&st.db, 8).await.unwrap();
    let mut hs = vec![];
    for id in ids {
        let (feed, run) = feeds::try_claim_feed_fetch(&st.db, id, "scheduled")
            .await
            .unwrap()
            .unwrap();
        let c = st.coordinator.clone();
        let cfg = c.load_runtime_config().await.unwrap();
        hs.push(tokio::spawn(async move {
            c.execute_claimed_feed_fetch(feed, run, &cfg).await
        }));
    }
    for h in hs {
        h.await.unwrap();
    }
    let rows: Vec<(String, Option<String>, Option<String>)> =
        sqlx::query_as("SELECT status, error_category, error_message FROM feed_fetch_runs")
            .fetch_all(&st.db)
            .await
            .unwrap();
    let failed: Vec<_> = rows.iter().filter(|r| r.0 != "success").collect();
    assert!(failed.is_empty(), "failed runs: {failed:?}");
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "feeds never reached the fixture"
    );
}

#[tokio::test]
async fn r26_real_phase1_db_upgrade_and_restart() {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;
    let tmp = TempDir::new().unwrap();
    let mig1 = tmp.path().join("m1");
    std::fs::create_dir_all(&mig1).unwrap();
    std::fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/migrations/0001_initial_schema.sql"
        ),
        mig1.join("0001_initial_schema.sql"),
    )
    .unwrap();
    let url = format!("sqlite://{}", tmp.path().join("p1.db").display());
    let p1 = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(&url)
                .unwrap()
                .create_if_missing(true)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::migrate::Migrator::new(mig1.as_path())
        .await
        .unwrap()
        .run(&p1)
        .await
        .unwrap();
    for sql in [
        "INSERT INTO feeds (id,name,feed_url,enabled,created_at,updated_at,last_error) VALUES (1,'A','https://a.example.com/rss',1,'2025-01-01T00:00:00Z','2025-01-02T00:00:00Z','old err')",
        "INSERT INTO feeds (id,name,feed_url,enabled) VALUES (2,'B','https://b.example.com/rss',0)",
        "INSERT INTO articles (id,feed_id,external_guid,canonical_url,title,extraction_status,published_at,discovered_at,content_hash) VALUES (10,1,'g1','https://a.example.com/1','t1','extracted','2025-01-01T00:00:00Z','2025-01-01T01:00:00Z','abc')",
        "INSERT INTO articles (id,feed_id,external_guid,canonical_url,title,extraction_status) VALUES (11,1,NULL,'https://a.example.com/2','t2','pending')",
        "INSERT INTO articles (id,feed_id,external_guid,canonical_url,title,extraction_status) VALUES (12,2,'g3','https://b.example.com/3','t3','failed')",
        "INSERT INTO articles (id,feed_id,external_guid,canonical_url,title,extraction_status) VALUES (13,2,'g4','https://b.example.com/4','t4','skipped')",
        "INSERT INTO settings (key,value) VALUES ('timezone','UTC')",
        "INSERT INTO editions (id,title,publication_date,status) VALUES (5,'E','2025-01-03','completed')",
        "INSERT INTO edition_articles (edition_id,article_id,position) VALUES (5,10,0),(5,12,1)",
    ] {
        sqlx::query(sql).execute(&p1).await.unwrap();
    }
    p1.close().await;

    for _restart in 0..2 {
        let pool = db::init_pool(&url, tmp.path())
            .await
            .expect("upgrade/restart");
        let counts: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT COUNT(*) FROM feeds),(SELECT COUNT(*) FROM articles),(SELECT COUNT(*) FROM settings),(SELECT COUNT(*) FROM editions),(SELECT COUNT(*) FROM edition_articles),(SELECT COUNT(*) FROM article_feed_sources)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        let fk: Vec<(String,)> = sqlx::query_as("SELECT 'x' FROM pragma_foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap();
        let ts: (String, Option<String>) =
            sqlx::query_as("SELECT created_at, last_error FROM feeds WHERE id=1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(counts, (2, 4, 1, 1, 2, 4));
        assert!(fk.is_empty());
        assert_eq!(ts.0, "2025-01-01T00:00:00Z");
        let (pending_jobs,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM article_extraction_jobs WHERE article_id = 11")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            pending_jobs, 1,
            "legacy pending article must have exactly one job"
        );
        pool.close().await;
    }
}

async fn big_feed_fixture(entries: u32) -> (SocketAddr, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let app = Router::new().route(
        "/feed/{n}",
        get(move |axum::extract::Path(n): axum::extract::Path<u32>| {
            let hits_route = hits_route.clone();
            async move {
            hits_route.fetch_add(1, Ordering::SeqCst);
            let mut items = String::new();
            for i in 0..entries {
                items.push_str(&format!("<item><guid>n{n}-{i}</guid><title>t{i}</title><link>http://site{n}.example.com/a/{i}</link><description>summary {i}</description></item>"));
            }
            (
                [(header::CONTENT_TYPE, "application/rss+xml")],
                format!("<rss version=\"2.0\"><channel><title>f{n}</title>{items}</channel></rss>"),
            )
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (addr, hits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn r29_production_tick_and_fetch_all_contention() {
    let (addr, hits) = big_feed_fixture(30).await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(addr));
    let (_tmp, st) = state_with(resolver, &[("QUIRE_EXTRACTION_CONCURRENCY", "1")]).await;
    for n in 0..12 {
        new_feed(
            &st.db,
            &format!("http://feeds.example.com:{}/feed/{n}", addr.port()),
        )
        .await;
    }
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(100));
    st.coordinator
        .trigger_manual_fetch_all_enabled()
        .await
        .unwrap();
    let mut success = 0i64;
    for _ in 0..150 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (n,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM feed_fetch_runs WHERE status = 'success'")
                .fetch_one(&st.db)
                .await
                .unwrap();
        success = n;
        if success == 12 {
            break;
        }
    }
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(8), handle).await;
    let failed: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM feed_fetch_runs WHERE status != 'success'")
            .fetch_one(&st.db)
            .await
            .unwrap();
    assert_eq!(failed.0, 0, "fetch-all runs failed under contention");
    assert_eq!(success, 12, "fetch-all did not finish every feed");
    assert!(
        hits.load(Ordering::SeqCst) > 0,
        "fetch-all never reached the fixture"
    );
}

fn feed_completion(
    feed_id: i64,
    run_id: i64,
    entries: Vec<NormalizedFeedEntry>,
    etag: Option<&str>,
) -> quire::feeds::CompleteFeedRunUpdate {
    let n = entries.len();
    quire::feeds::CompleteFeedRunUpdate {
        feed_id,
        run_id,
        status: "success",
        http_status: Some(200),
        duration_ms: 1,
        etag: etag.map(str::to_string),
        last_modified: None,
        metadata: None,
        discovered_entries: entries,
        max_extraction_attempts: 3,
        entries_seen: n,
        entries_skipped: 0,
        entries_truncated: 0,
        articles_inserted: n,
        articles_updated: 0,
        articles_unchanged: 0,
        extraction_queued: n,
        error_category: None,
        error_message: None,
        retry_after: None,
        default_interval_minutes: 60,
        retention_limit: 100,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn s2_concurrent_persists_and_claims_stay_under_budget() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let mut fids = vec![];
    for i in 0..8 {
        fids.push(new_feed(&st.db, &format!("https://s2-{i}.example.com/rss")).await);
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut claimers = vec![];
    for _ in 0..4 {
        let pool = st.db.clone();
        let stop = stop.clone();
        claimers.push(tokio::spawn(async move {
            let mut slow = false;
            while !stop.load(Ordering::SeqCst) {
                let started = Instant::now();
                let claimed = claim_next_extraction_job(&pool).await;
                if started.elapsed() >= Duration::from_secs(3) {
                    slow = true;
                }
                if let Ok(Some(job)) = claimed {
                    let started = Instant::now();
                    let _ = complete_extraction_job(
                        &pool,
                        &job,
                        &ExtractionResult::Failed {
                            error_message: "x".into(),
                            retryable: false,
                            retry_after: None,
                            interim_fallback: None,
                            cancelled: false,
                        },
                    )
                    .await;
                    if started.elapsed() >= Duration::from_secs(3) {
                        slow = true;
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
            slow
        }));
    }
    let mut persists = vec![];
    for (k, fid) in fids.iter().enumerate() {
        let pool = st.db.clone();
        let fid = *fid;
        let entries: Vec<_> = (0..200)
            .map(|i| entry(&format!("https://s2-{k}.example.com/{i}"), None, None))
            .collect();
        persists.push(tokio::spawn(async move {
            let started = Instant::now();
            let res = persist_discovered_entries(&pool, fid, &entries, 3).await;
            (res, started.elapsed())
        }));
    }
    let mut failures = Vec::new();
    for (idx, p) in persists.into_iter().enumerate() {
        let (res, elapsed) = p.await.unwrap();
        if let Err(err) = res {
            failures.push(format!("persist {idx} err={err} elapsed={elapsed:?}"));
        } else if elapsed >= Duration::from_secs(3) {
            failures.push(format!("persist {idx} slow elapsed={elapsed:?}"));
        }
    }
    stop.store(true, Ordering::SeqCst);
    for (idx, c) in claimers.into_iter().enumerate() {
        if c.await.unwrap() {
            failures.push(format!("claimer {idx} exceeded 3s"));
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

#[tokio::test]
async fn s2_persist_fails_cleanly_when_writer_lock_held() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("busy.db");
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", db_path.display()))
        .unwrap()
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(1));
    db::run_migrations(&sqlx::migrate!("./migrations"), &options)
        .await
        .unwrap();
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .unwrap();
    let fid = new_feed(&pool, "https://busy.example.com/rss").await;
    let mut holder = options
        .clone()
        .busy_timeout(Duration::from_secs(30))
        .connect()
        .await
        .unwrap();
    let mut hold_tx = holder.begin().await.unwrap();
    sqlx::query("UPDATE feeds SET name = name")
        .execute(&mut *hold_tx)
        .await
        .unwrap();
    let entries: Vec<_> = (0..5)
        .map(|i| entry(&format!("https://busy.example.com/{i}"), None, None))
        .collect();
    let err = persist_discovered_entries(&pool, fid, &entries, 3)
        .await
        .expect_err("persist must fail while the writer lock is held");
    drop(hold_tx);
    let _ = holder.close().await;
    assert!(db::is_retryable_sqlite_busy(&err), "{err:?}");
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    let (etag, modified): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT etag, last_modified FROM feeds WHERE id = ?")
            .bind(fid)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(etag.is_none() && modified.is_none());
    persist_discovered_entries(&pool, fid, &entries, 3)
        .await
        .expect("persist succeeds after the lock is released");
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 5);
}

#[tokio::test]
async fn s2_dropped_claim_future_releases_connection() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://drop.example.com/rss").await;
    let entries: Vec<_> = (0..5)
        .map(|i| entry(&format!("https://drop.example.com/{i}"), None, None))
        .collect();
    persist_discovered_entries(&st.db, fid, &entries, 3)
        .await
        .unwrap();
    for _ in 0..50 {
        let _ =
            tokio::time::timeout(Duration::from_micros(1), claim_next_extraction_job(&st.db)).await;
    }
    let started = Instant::now();
    let more: Vec<_> = (0..3)
        .map(|i| entry(&format!("https://drop.example.com/more/{i}"), None, None))
        .collect();
    persist_discovered_entries(&st.db, fid, &more, 3)
        .await
        .expect("pool still accepts writes after dropped claims");
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_lowering_fetch_concurrency_admits_no_new_work() {
    let inflight = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let inf = inflight.clone();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let app = Router::new().route(
        "/hold",
        get(move || {
            let inf = inf.clone();
            let hits_route = hits_route.clone();
            async move {
                hits_route.fetch_add(1, Ordering::SeqCst);
                inf.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(3)).await;
                inf.fetch_sub(1, Ordering::SeqCst);
                "ok"
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[("QUIRE_FETCH_ENABLED", "true")],
    )
    .await;
    db::upsert_settings(&st.db, &[("fetch_concurrency", "8")])
        .await
        .unwrap();
    for i in 0..8 {
        new_feed(
            &st.db,
            &format!("http://hold{i}.example.com:{}/hold", addr.port()),
        )
        .await;
    }
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(50));
    let started = Instant::now();
    while inflight.load(Ordering::SeqCst) < 8 && started.elapsed() < Duration::from_secs(8) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        inflight.load(Ordering::SeqCst),
        8,
        "did not reach 8 in flight"
    );
    db::upsert_settings(&st.db, &[("fetch_concurrency", "2")])
        .await
        .unwrap();
    st.coordinator.notify_workers();
    let mut prev = inflight.load(Ordering::SeqCst);
    let mut below = prev < 2;
    let watch_until = Instant::now() + Duration::from_secs(8);
    while Instant::now() < watch_until {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = inflight.load(Ordering::SeqCst);
        if !below {
            assert!(
                now <= prev,
                "in-flight rose from {prev} to {now} before dropping below 2"
            );
            if now < 2 {
                below = true;
            }
        } else {
            assert!(now <= 2, "in-flight {now} exceeded 2 after the lower limit");
        }
        prev = now;
        if below && now == 0 && hits.load(Ordering::SeqCst) >= 8 {
            break;
        }
    }
    assert!(hits.load(Ordering::SeqCst) > 0);
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(6), handle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s3_due_feed_starts_during_extraction_backlog() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let app = Router::new()
        .route(
            "/slow",
            get(move || {
                let hits_route = hits_route.clone();
                async move {
                    hits_route.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    (
                        [(header::CONTENT_TYPE, "text/html")],
                        "<html><body><p>slow article body for extraction.</p></body></html>",
                    )
                }
            }),
        )
        .route(
            "/feed",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/rss+xml")],
                    "<rss version=\"2.0\"><channel><title>due</title></channel></rss>",
                )
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let port = addr.port();
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[
            ("QUIRE_EXTRACTION_CONCURRENCY", "2"),
            ("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS", "30"),
        ],
    )
    .await;
    let slow_feed = new_feed(&st.db, &format!("http://slow.example.com:{port}/feed")).await;
    let entries: Vec<_> = (0..20)
        .map(|i| {
            entry(
                &format!("http://slow.example.com:{port}/slow?i={i}"),
                None,
                None,
            )
        })
        .collect();
    persist_discovered_entries(&st.db, slow_feed, &entries, 3)
        .await
        .unwrap();
    sqlx::query("UPDATE feeds SET next_scheduled_fetch_at = '2099-01-01T00:00:00Z' WHERE id = ?")
        .bind(slow_feed)
        .execute(&st.db)
        .await
        .unwrap();
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(50));
    tokio::time::sleep(Duration::from_millis(200)).await;
    new_feed(&st.db, &format!("http://due.example.com:{port}/feed")).await;
    let started = Instant::now();
    let mut runs = 0i64;
    while started.elapsed() < Duration::from_secs(6) {
        let (n,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM feed_fetch_runs WHERE feed_id != ?")
                .bind(slow_feed)
                .fetch_one(&st.db)
                .await
                .unwrap();
        runs = n;
        if runs > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(6), handle).await;
    assert!(runs > 0, "due feed did not start within 6s");
    assert!(hits.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn s3_second_process_exits_nonzero() {
    let tmp = TempDir::new().unwrap();
    let p1 = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let p2 = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let data = tmp.path().join("data");
    let db = data.join("quire.db");
    let bin = env!("CARGO_BIN_EXE_quire");
    let mut first = Command::new(bin)
        .env("QUIRE_DATA_DIR", &data)
        .env("QUIRE_DATABASE_URL", format!("sqlite://{}", db.display()))
        .env("QUIRE_FETCH_ENABLED", "false")
        .env("QUIRE_HOST", "127.0.0.1")
        .env("QUIRE_PORT", p1.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let mut healthy = false;
    for _ in 0..50 {
        if client
            .get(format!("http://127.0.0.1:{p1}/health"))
            .send()
            .await
            .ok()
            .is_some_and(|r| r.status().is_success())
        {
            healthy = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(healthy, "first process did not become healthy");
    let mut second = Command::new(bin)
        .env("QUIRE_DATA_DIR", &data)
        .env("QUIRE_DATABASE_URL", format!("sqlite://{}", db.display()))
        .env("QUIRE_FETCH_ENABLED", "false")
        .env("QUIRE_HOST", "127.0.0.1")
        .env("QUIRE_PORT", p2.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = second.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(10) {
            let _ = second.kill();
            panic!("second process did not exit within 10s");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let mut err_buf = String::new();
    second
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err_buf)
        .unwrap();
    let _ = first.kill();
    let _ = first.wait();
    assert!(!status.success(), "second process exited 0: {err_buf}");
    assert!(
        err_buf.contains("Another Quire instance is using"),
        "{err_buf}"
    );
}

#[tokio::test]
async fn s3_manual_request_survives_restart() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let app = Router::new().route(
        "/feed",
        get(move || {
            let hits_route = hits_route.clone();
            async move {
                hits_route.fetch_add(1, Ordering::SeqCst);
                (
                    [(header::CONTENT_TYPE, "application/rss+xml")],
                    "<rss version=\"2.0\"><channel><title>m</title></channel></rss>",
                )
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let tmp = TempDir::new().unwrap();
    let url = format!("sqlite://{}", tmp.path().join("m.db").display());
    let mut env = HashMap::new();
    env.insert("QUIRE_DATA_DIR".into(), tmp.path().display().to_string());
    env.insert("QUIRE_DATABASE_URL".into(), url.clone());
    env.insert("QUIRE_FETCH_ENABLED".into(), "false".into());
    let cfg = quire::config::AppConfig::from_map(&env).unwrap();
    let pool = db::init_pool(&cfg.database_url, &cfg.data_dir)
        .await
        .unwrap();
    let fid = new_feed(
        &pool,
        &format!("http://manual.example.com:{}/feed", addr.port()),
    )
    .await;
    assert!(
        quire::feeds::mark_feed_manual_fetch_due(&pool, fid)
            .await
            .unwrap()
    );
    pool.close().await;
    let pool = db::init_pool(&url, tmp.path()).await.unwrap();
    let st = quire::web::AppState::with_resolver(pool, cfg, Arc::new(FixtureHost(addr)));
    let (feeds_run, _) = st.coordinator.run_scheduler_tick().await.unwrap();
    assert!(feeds_run >= 1);
    let (trigger,): (String,) =
        sqlx::query_as("SELECT trigger_type FROM feed_fetch_runs ORDER BY id DESC LIMIT 1")
            .fetch_one(&st.db)
            .await
            .unwrap();
    assert_eq!(trigger, "manual");
    assert!(hits.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn s4_stale_feed_run_writes_nothing() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://stale.example.com/rss").await;
    let (feed, run_a) = quire::feeds::try_claim_feed_fetch(&st.db, fid, "scheduled")
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE feeds SET fetch_in_progress = 0, current_run_id = NULL WHERE id = ?")
        .bind(feed.id)
        .execute(&st.db)
        .await
        .unwrap();
    let (_feed_b, run_b) = quire::feeds::try_claim_feed_fetch(&st.db, fid, "scheduled")
        .await
        .unwrap()
        .unwrap();
    let entries_a: Vec<_> = (0..3)
        .map(|i| entry(&format!("https://stale.example.com/a/{i}"), None, None))
        .collect();
    let wrote_a = quire::feeds::complete_feed_fetch_run(
        &st.db,
        feed_completion(fid, run_a, entries_a, Some("etag-a")),
    )
    .await
    .unwrap();
    assert!(!wrote_a);
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(n, 0);
    let entries_b = vec![entry("https://stale.example.com/b", None, None)];
    let wrote_b = quire::feeds::complete_feed_fetch_run(
        &st.db,
        feed_completion(fid, run_b, entries_b, Some("etag-b")),
    )
    .await
    .unwrap();
    assert!(wrote_b);
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(n, 1);
    let (etag,): (Option<String>,) = sqlx::query_as("SELECT etag FROM feeds WHERE id = ?")
        .bind(fid)
        .fetch_one(&st.db)
        .await
        .unwrap();
    assert_eq!(etag.as_deref(), Some("etag-b"));
}

#[tokio::test]
async fn s4_deadline_release_after_future_dropped() {
    let closed = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mark = closed.clone();
    let app = Router::new().route(
        "/hang",
        get(move || {
            let mark = mark.clone();
            async move {
                let _drop = HangDrop(mark);
                tokio::time::sleep(Duration::from_secs(60)).await;
                "late"
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS", "5")],
    )
    .await;
    let fid = new_feed(&st.db, "https://hang.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry(
            &format!("http://hang.example.com:{}/hang", addr.port()),
            None,
            Some("interim"),
        )],
        3,
    )
    .await
    .unwrap();
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(50));
    let mut requeued_at = None;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let (status,): (String,) = sqlx::query_as("SELECT status FROM article_extraction_jobs")
            .fetch_one(&st.db)
            .await
            .unwrap();
        if status == "queued" {
            requeued_at = Some(Instant::now());
            break;
        }
    }
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(6), handle).await;
    let closed_at = closed.lock().unwrap().expect("connection was not closed");
    let requeued_at = requeued_at.expect("job was not requeued");
    assert!(closed_at <= requeued_at);
}

struct HangDrop(Arc<std::sync::Mutex<Option<Instant>>>);
impl Drop for HangDrop {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = Some(Instant::now());
    }
}

#[tokio::test]
async fn s4_dropped_feed_completion_is_atomic() {
    for delay_ms in [1u64, 5, 20, 50] {
        let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
        let fid = new_feed(&st.db, "https://atomic.example.com/rss").await;
        let (_, run_id) = quire::feeds::try_claim_feed_fetch(&st.db, fid, "scheduled")
            .await
            .unwrap()
            .unwrap();
        let entries: Vec<_> = (0..200)
            .map(|i| {
                entry(
                    &format!("https://atomic.example.com/{i}"),
                    Some(&format!("g{i}")),
                    None,
                )
            })
            .collect();
        let _ = tokio::time::timeout(
            Duration::from_millis(delay_ms),
            quire::feeds::complete_feed_fetch_run(
                &st.db,
                feed_completion(fid, run_id, entries, None),
            ),
        )
        .await;
        let (articles, orphans): (i64, i64) = sqlx::query_as(
            "SELECT
                (SELECT COUNT(*) FROM articles),
                (SELECT COUNT(*) FROM articles a
                 WHERE NOT EXISTS (
                     SELECT 1 FROM article_feed_sources s WHERE s.article_id = a.id
                 ))",
        )
        .fetch_one(&st.db)
        .await
        .unwrap();
        assert!(
            articles == 0 || articles == 200,
            "partial articles: {articles}"
        );
        assert_eq!(orphans, 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s5_backlog_does_not_stall_polling() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route(
            "/hang",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                "x"
            }),
        )
        .route(
            "/feed",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/rss+xml")],
                    "<rss version=\"2.0\"><channel><title>poll</title></channel></rss>",
                )
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let port = addr.port();
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[
            ("QUIRE_EXTRACTION_CONCURRENCY", "2"),
            ("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS", "30"),
        ],
    )
    .await;
    let src = new_feed(&st.db, &format!("http://src.example.com:{port}/feed")).await;
    let entries: Vec<_> = (0..500)
        .map(|i| {
            entry(
                &format!("http://src.example.com:{port}/hang?i={i}"),
                None,
                None,
            )
        })
        .collect();
    persist_discovered_entries(&st.db, src, &entries, 3)
        .await
        .unwrap();
    sqlx::query("UPDATE feeds SET enabled = 0, next_scheduled_fetch_at = '2099-01-01T00:00:00Z' WHERE id = ?")
        .bind(src)
        .execute(&st.db)
        .await
        .unwrap();
    let poll = new_feed(&st.db, &format!("http://poll.example.com:{port}/feed")).await;
    let handle = st
        .coordinator
        .spawn_background_loop(Duration::from_millis(50));
    let mut saw_processing = false;
    let mut age_a = None;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(7) {
        let counts = db::get_dashboard_counts(&st.db).await.unwrap();
        if counts.processing_extractions == 2 && counts.pending_extractions >= 498 {
            if age_a.is_none() {
                age_a = counts.oldest_queued_age_seconds;
                tokio::time::sleep(Duration::from_millis(1100)).await;
                let again = db::get_dashboard_counts(&st.db).await.unwrap();
                let age_b = again.oldest_queued_age_seconds;
                assert!(
                    age_a.is_some() && age_b > age_a,
                    "age {age_a:?} -> {age_b:?}"
                );
            }
            saw_processing = true;
        }
        sqlx::query(
            "UPDATE feeds SET next_scheduled_fetch_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE id = ?",
        )
        .bind(poll)
            .execute(&st.db)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    let (runs,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM feed_fetch_runs WHERE feed_id = ?")
        .bind(poll)
        .fetch_one(&st.db)
        .await
        .unwrap();
    st.coordinator.signal_shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(6), handle).await;
    assert!(saw_processing, "dashboard did not show the backlog");
    assert!(runs >= 3, "feed runs in 7s: {runs}");
}

#[tokio::test]
async fn s5_dashboard_counts_match_sql() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let fid = new_feed(&st.db, "https://dash.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[
            entry("https://dash.example.com/1", None, None),
            entry("https://dash.example.com/2", None, None),
            entry("https://dash.example.com/3", None, None),
        ],
        3,
    )
    .await
    .unwrap();
    sqlx::query(
        "UPDATE articles SET extracted_content = '<p>a</p>', extracted_content_type = 'html' WHERE canonical_url LIKE '%/1'",
    )
    .execute(&st.db)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE article_extraction_jobs SET status = 'completed', completed_at = strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE article_id = (SELECT id FROM articles WHERE canonical_url LIKE '%/1')",
    )
    .execute(&st.db)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE articles SET extracted_content = 'sum', extracted_content_type = 'summary_fallback' WHERE canonical_url LIKE '%/2'",
    )
    .execute(&st.db)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE article_extraction_jobs SET status = 'failed' WHERE article_id = (SELECT id FROM articles WHERE canonical_url LIKE '%/2')",
    )
    .execute(&st.db)
    .await
    .unwrap();
    sqlx::query("UPDATE feeds SET fetch_in_progress = 1 WHERE id = ?")
        .bind(fid)
        .execute(&st.db)
        .await
        .unwrap();
    let counts = db::get_dashboard_counts(&st.db).await.unwrap();
    let (queued,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM article_extraction_jobs WHERE status = 'queued'")
            .fetch_one(&st.db)
            .await
            .unwrap();
    let (processing,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM article_extraction_jobs WHERE status = 'processing'")
            .fetch_one(&st.db)
            .await
            .unwrap();
    let (running,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM feeds WHERE fetch_in_progress = 1")
            .fetch_one(&st.db)
            .await
            .unwrap();
    let (html_done,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM articles a LEFT JOIN article_extraction_jobs j ON j.article_id = a.id WHERE a.extracted_content_type = 'html' AND COALESCE(j.status, 'completed') = 'completed'",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    let (fallback,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM articles a LEFT JOIN article_extraction_jobs j ON j.article_id = a.id WHERE a.extracted_content_type = 'summary_fallback' AND COALESCE(j.status, 'completed') IN ('completed', 'failed')",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert_eq!(counts.pending_extractions, queued);
    assert_eq!(counts.processing_extractions, processing);
    assert_eq!(counts.running_fetches, running);
    assert_eq!(counts.extracted_articles, html_done);
    assert_eq!(counts.fallback_articles, fallback);
    assert_eq!(counts.total_feeds, 1);
    assert_eq!(counts.total_articles, 3);
}

async fn make_job_due(pool: &SqlitePool) {
    sqlx::query(
        "UPDATE article_extraction_jobs SET next_eligible_at = '2000-01-01T00:00:00Z', status = CASE WHEN status = 'failed' THEN status ELSE 'queued' END WHERE status IN ('queued', 'failed')",
    )
    .execute(pool)
    .await
    .unwrap();
}

#[tokio::test]
async fn s6_display_state_table() {
    use quire::articles::{derive_display_state, derive_legacy_extraction_status};
    let cases = [
        ("none", Some("queued"), "Pending", "pending"),
        ("none", Some("processing"), "Extracting", "pending"),
        (
            "summary_fallback",
            Some("queued"),
            "Summary (full text pending)",
            "pending",
        ),
        (
            "summary_fallback",
            Some("processing"),
            "Summary (full text pending)",
            "pending",
        ),
        ("html", Some("queued"), "Full text (refreshing)", "pending"),
        (
            "html",
            Some("processing"),
            "Full text (refreshing)",
            "pending",
        ),
        ("html", Some("completed"), "Full text", "extracted"),
        (
            "summary_fallback",
            Some("completed"),
            "Summary only",
            "skipped",
        ),
        (
            "summary_fallback",
            Some("failed"),
            "Summary only",
            "skipped",
        ),
        ("none", Some("failed"), "Failed", "failed"),
    ];
    for (content, job, display, legacy) in cases {
        assert_eq!(derive_display_state(content, job), display);
        assert_eq!(derive_legacy_extraction_status(content, job), legacy);
    }
}

#[tokio::test]
async fn s6_503_503_200() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_route = hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/art",
        get(move || {
            let hits_route = hits_route.clone();
            async move {
                let n = hits_route.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    (StatusCode::SERVICE_UNAVAILABLE, "down".to_string())
                } else {
                    (
                        StatusCode::OK,
                        "<html><body><article><p>Recovered full text of the story.</p></article></body></html>".to_string(),
                    )
                }
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(Arc::new(FixtureHost(addr)), &[]).await;
    let fid = new_feed(&st.db, "https://seq.example.com/rss").await;
    let url = format!("http://seq.example.com:{}/art", addr.port());
    persist_discovered_entries(&st.db, fid, &[entry(&url, None, Some("A summary"))], 3)
        .await
        .unwrap();
    for expected in ["Summary (full text pending)", "Summary (full text pending)"] {
        st.coordinator.drain_eligible_extractions().await.unwrap();
        let (content, status): (String, String) = sqlx::query_as(
            "SELECT a.extracted_content_type, j.status FROM articles a JOIN article_extraction_jobs j ON j.article_id = a.id",
        )
        .fetch_one(&st.db)
        .await
        .unwrap();
        assert_eq!(
            quire::articles::derive_display_state(&content, Some(&status)),
            expected
        );
        make_job_due(&st.db).await;
    }
    st.coordinator.drain_eligible_extractions().await.unwrap();
    let (content, status, attempts): (String, String, i64) = sqlx::query_as(
        "SELECT a.extracted_content_type, j.status, j.attempts FROM articles a JOIN article_extraction_jobs j ON j.article_id = a.id",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert_eq!(content, "html");
    assert_eq!(status, "completed");
    assert_eq!(attempts, 3);
    assert!(hits.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn s6_503_x3() {
    let fx = spawn_fixture().await;
    let (_t, st) = state_with(Arc::new(FixtureHost(fx.addr)), &[]).await;
    let fid = new_feed(&st.db, "https://down.example.com/rss").await;
    let url = format!("http://down.example.com:{}/a503", fx.addr.port());
    persist_discovered_entries(&st.db, fid, &[entry(&url, None, Some("Only summary"))], 3)
        .await
        .unwrap();
    for _ in 0..3 {
        st.coordinator.drain_eligible_extractions().await.unwrap();
        make_job_due(&st.db).await;
    }
    let (content, status): (String, String) = sqlx::query_as(
        "SELECT a.extracted_content_type, j.status FROM articles a JOIN article_extraction_jobs j ON j.article_id = a.id",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert_eq!(content, "summary_fallback");
    assert_eq!(status, "failed");
    assert_eq!(
        quire::articles::derive_display_state(&content, Some(&status)),
        "Summary only"
    );
    assert!(fx.hits.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn s6_retry_after_120() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_accept = hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let hits_accept = hits_accept.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                hits_accept.fetch_add(1, Ordering::SeqCst);
                let _ = sock
                    .write_all(
                        b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: 120\r\nContent-Length: 4\r\nConnection: close\r\n\r\nwait",
                    )
                    .await;
            });
        }
    });
    let (_t, st) = state_with(Arc::new(FixtureHost(addr)), &[]).await;
    let fid = new_feed(&st.db, "https://rate.example.com/rss").await;
    let url = format!("http://rate.example.com:{}/art", addr.port());
    persist_discovered_entries(&st.db, fid, &[entry(&url, None, Some("sum"))], 3)
        .await
        .unwrap();
    let before = chrono::Utc::now();
    st.coordinator.drain_eligible_extractions().await.unwrap();
    let (next_at, status, last_error): (String, String, Option<String>) =
        sqlx::query_as("SELECT next_eligible_at, status, last_error FROM article_extraction_jobs")
            .fetch_one(&st.db)
            .await
            .unwrap();
    let parsed = chrono::DateTime::parse_from_rfc3339(&next_at).unwrap();
    let delta = parsed.timestamp() - before.timestamp();
    assert!(
        delta >= 119,
        "next_eligible_at={next_at} delta={delta}s status={status} last_error={last_error:?}"
    );
}

#[tokio::test]
async fn s6_request_budget() {
    let counter = Arc::new(AtomicUsize::new(0));
    let c2 = counter.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/art",
        get(move || {
            let c2 = c2.clone();
            async move {
                c2.fetch_add(1, Ordering::SeqCst);
                (StatusCode::SERVICE_UNAVAILABLE, "down")
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[("QUIRE_FETCH_MAX_ATTEMPTS", "3")],
    )
    .await;
    let fid = new_feed(&st.db, "https://budget.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        fid,
        &[entry(
            &format!("http://budget.example.com:{}/art", addr.port()),
            None,
            Some("sum"),
        )],
        3,
    )
    .await
    .unwrap();
    for _ in 0..6 {
        st.coordinator.drain_eligible_extractions().await.unwrap();
        make_job_due(&st.db).await;
    }
    let n = counter.load(Ordering::SeqCst);
    assert!(n > 0 && n <= 6, "requests {n}");
}

#[tokio::test]
async fn s7_delete_feed_orphans_and_repoints() {
    let (_t, st) = state_with(Arc::new(SystemDnsResolver), &[]).await;
    let a = new_feed(&st.db, "https://orphan-a.example.com/rss").await;
    let b = new_feed(&st.db, "https://orphan-b.example.com/rss").await;
    persist_discovered_entries(
        &st.db,
        a,
        &[
            entry("https://shared.example.com/one", Some("s"), None),
            entry("https://only-a.example.com/orphan", Some("o"), None),
        ],
        3,
    )
    .await
    .unwrap();
    persist_discovered_entries(
        &st.db,
        b,
        &[entry("https://shared.example.com/one", Some("s2"), None)],
        3,
    )
    .await
    .unwrap();
    feeds::delete_feed(&st.db, a).await.unwrap();
    let (orphan,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM articles WHERE canonical_url LIKE '%orphan%'")
            .fetch_one(&st.db)
            .await
            .unwrap();
    assert_eq!(orphan, 0);
    let (feed_id,): (Option<i64>,) = sqlx::query_as(
        "SELECT feed_id FROM articles WHERE canonical_url = 'https://shared.example.com/one'",
    )
    .fetch_one(&st.db)
    .await
    .unwrap();
    assert_eq!(feed_id, Some(b));
}

struct MixedResolver;
impl HostResolver for MixedResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        Box::pin(async move {
            Ok(vec![
                SocketAddr::from(([93, 184, 216, 34], port)),
                SocketAddr::from(([10, 0, 0, 1], port)),
            ])
        })
    }
}

struct HangResolver;
impl HostResolver for HangResolver {
    fn resolve<'a>(
        &'a self,
        _host: &'a str,
        _port: u16,
    ) -> Pin<Box<dyn Future<Output = AppResult<Vec<SocketAddr>>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn s8_resolver_mixed_answer_blocked() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(MixedResolver);
    let url = format!("http://mixed.example.com:{}/secret", fx.addr.port());
    let err = fetch_url_with_retries(&url, &opts(), &resolver)
        .await
        .expect_err("mixed answer must be blocked");
    assert_eq!(err.category, FetchErrorCategory::SsrfBlocked);
    assert_eq!(fx.secret_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn s8_resolver_hang_hits_deadline() {
    let resolver: Arc<dyn HostResolver> = Arc::new(HangResolver);
    let mut o = opts();
    o.request_timeout = Duration::from_secs(2);
    let started = Instant::now();
    let err = fetch_url_with_retries("http://hang.example.com/x", &o, &resolver)
        .await
        .expect_err("hanging resolver must time out");
    assert_eq!(err.category, FetchErrorCategory::Timeout);
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn s8_https_proxy_ignored() {
    let fx = spawn_fixture().await;
    let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    let proxy_hits = Arc::new(AtomicUsize::new(0));
    let proxy_hits2 = proxy_hits.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = proxy.accept().await else {
                break;
            };
            proxy_hits2.fetch_add(1, Ordering::SeqCst);
            let _ = sock
                .write_all(b"HTTP/1.1 500 proxy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        }
    });
    let proxy_url = format!("http://{proxy_addr}");
    unsafe {
        std::env::set_var("HTTPS_PROXY", &proxy_url);
        std::env::set_var("HTTP_PROXY", &proxy_url);
    }
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(fx.addr));
    let url = format!("http://direct.example.com:{}/slowfeed", fx.addr.port());
    let res = fetch_url_with_retries(&url, &opts(), &resolver).await;
    unsafe {
        std::env::remove_var("HTTPS_PROXY");
        std::env::remove_var("HTTP_PROXY");
    }
    assert!(res.is_ok(), "{res:?}");
    assert!(fx.hits.load(Ordering::SeqCst) > 0);
    assert_eq!(proxy_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn s8_production_path_xml() {
    let evil_hits = Arc::new(AtomicUsize::new(0));
    let evil_hits2 = evil_hits.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let app = Router::new()
        .route(
            "/lol",
            get(move || async move {
                let xml = format!(
                    "<?xml version=\"1.0\"?><!DOCTYPE lolz [<!ENTITY lol \"lol\"><!ENTITY lol2 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">]><rss version=\"2.0\"><channel><title>&lol2;</title><item><title>&lol2;</title><link>http://lol.example.com:{port}/a</link></item></channel></rss>"
                );
                ([(header::CONTENT_TYPE, "application/rss+xml")], xml)
            }),
        )
        .route(
            "/file",
            get(move || async move {
                let xml = format!(
                    "<?xml version=\"1.0\"?><!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]><rss version=\"2.0\"><channel><title>&xxe;</title><item><title>&xxe;</title><link>http://file.example.com:{port}/a</link><description>&xxe;</description></item></channel></rss>"
                );
                ([(header::CONTENT_TYPE, "application/rss+xml")], xml)
            }),
        )
        .route(
            "/dtd",
            get(move || async move {
                let xml = format!(
                    "<?xml version=\"1.0\"?><!DOCTYPE foo SYSTEM \"http://evil.example.com:{port}/evil.dtd\"><rss version=\"2.0\"><channel><title>t</title></channel></rss>"
                );
                ([(header::CONTENT_TYPE, "application/rss+xml")], xml)
            }),
        )
        .route(
            "/evil.dtd",
            get(move || {
                let evil_hits2 = evil_hits2.clone();
                async move {
                    evil_hits2.fetch_add(1, Ordering::SeqCst);
                    "boom"
                }
            }),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[("QUIRE_FEED_REQUEST_TIMEOUT_SECONDS", "5")],
    )
    .await;
    for path in ["/lol", "/file", "/dtd"] {
        let fid = new_feed(&st.db, &format!("http://xml.example.com:{port}{path}")).await;
        let (feed, run_id) = feeds::try_claim_feed_fetch(&st.db, fid, "scheduled")
            .await
            .unwrap()
            .unwrap();
        let cfg = st.coordinator.load_runtime_config().await.unwrap();
        let started = Instant::now();
        st.coordinator
            .execute_claimed_feed_fetch(feed, run_id, &cfg)
            .await;
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{path} exceeded timeout"
        );
    }
    let blobs: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT title, description FROM articles")
            .fetch_all(&st.db)
            .await
            .unwrap();
    let titles: Vec<(String,)> = sqlx::query_as("SELECT name FROM feeds")
        .fetch_all(&st.db)
        .await
        .unwrap();
    for (title, description) in &blobs {
        assert!(!title.contains("root:"), "{title}");
        assert!(!title.contains("lollol"), "entity expanded: {title}");
        assert!(!description.as_deref().unwrap_or("").contains("root:"));
        assert!(title.len() < 500, "expanded entity: {title}");
    }
    for (name,) in &titles {
        assert!(!name.contains("root:"));
        assert!(
            name.len() < 500,
            "expanded feed title stored in name? {name}"
        );
    }
    let runs: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT status, error_message FROM feed_fetch_runs")
            .fetch_all(&st.db)
            .await
            .unwrap();
    for (status, message) in &runs {
        let message = message.as_deref().unwrap_or("");
        assert!(!message.contains("root:"));
        assert!(message.len() < 2000, "{status} {message}");
    }
    assert_eq!(evil_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn s8_large_extraction_keeps_health_fast() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let body = Arc::new(vec![b'x'; 10 * 1024 * 1024]);
    let app = Router::new().route(
        "/big",
        get({
            let body = body.clone();
            move || {
                let body = body.clone();
                async move { ([(header::CONTENT_TYPE, "text/html")], (*body).clone()) }
            }
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let (_t, st) = state_with(
        Arc::new(FixtureHost(addr)),
        &[("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS", "60")],
    )
    .await;
    let health = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_addr = health.local_addr().unwrap();
    let router = quire::web::create_router(st.clone());
    tokio::spawn(async move {
        let _ = axum::serve(health, router).await;
    });
    let fid = new_feed(&st.db, "https://big.example.com/rss").await;
    let url = format!("http://big.example.com:{}/big", addr.port());
    persist_discovered_entries(&st.db, fid, &[entry(&url, None, Some("sum"))], 3)
        .await
        .unwrap();
    let st2 = st.clone();
    let extract = tokio::spawn(async move {
        st2.coordinator.drain_eligible_extractions().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let client = reqwest::Client::builder().build().unwrap();
    let health_url = format!("http://{health_addr}/health");
    let mut samples = Vec::new();
    for _ in 0..20 {
        let started = Instant::now();
        let resp = client.get(&health_url).send().await.unwrap();
        assert!(resp.status().is_success(), "{}", resp.status());
        samples.push(started.elapsed());
    }
    samples.sort();
    let p95 = samples[18];
    assert!(
        p95 <= Duration::from_millis(100),
        "health p95 {p95:?} samples={samples:?}"
    );
    extract.await.unwrap();
}

async fn load_fixture(fail_articles: bool) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let app = Router::new()
        .route(
            "/feed/{n}",
            get(move |axum::extract::Path(n): axum::extract::Path<u32>| async move {
                let mut items = String::new();
                for i in 0..10 {
                    items.push_str(&format!(
                        "<item><guid>n{n}-{i}</guid><title>t{i}</title><link>http://site{n}.example.com:{port}/a/{n}/{i}</link><description>summary {i}</description></item>"
                    ));
                }
                (
                    [
                        (header::CONTENT_TYPE, "application/rss+xml"),
                        (header::ETAG, "\"v1\""),
                    ],
                    format!(
                        "<rss version=\"2.0\"><channel><title>f{n}</title>{items}</channel></rss>"
                    ),
                )
            }),
        )
        .route(
            "/a/{n}/{i}",
            get(
                move |axum::extract::Path((n, i)): axum::extract::Path<(u32, u32)>| async move {
                    if fail_articles {
                        return (StatusCode::INTERNAL_SERVER_ERROR, String::new()).into_response();
                    }
                    let body: String = (0..6)
                        .map(|k| {
                            format!(
                                "<p>Paragraph {k} of article {n}/{i}, with prose, commas, and sentences that readability will keep.</p>"
                            )
                        })
                        .collect();
                    (
                        [(header::CONTENT_TYPE, "text/html")],
                        format!(
                            "<html><body><article><h1>A {n}/{i}</h1>{body}</article></body></html>"
                        ),
                    )
                        .into_response()
                },
            ),
        );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: Stage 11"]
async fn r27_load_100_feeds_1000_articles() {
    let addr = load_fixture(false).await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(addr));
    let (tmp, st) = state_with(resolver, &[]).await;
    for n in 0..100 {
        new_feed(
            &st.db,
            &format!("http://feeds.example.com:{}/feed/{n}", addr.port()),
        )
        .await;
    }
    let t = Instant::now();
    let mut ticks = 0;
    loop {
        ticks += 1;
        st.coordinator.run_scheduler_tick().await.unwrap();
        let (due,): (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM feeds WHERE next_scheduled_fetch_at <= strftime('%Y-%m-%dT%H:%M:%SZ','now')",
        )
        .fetch_one(&st.db)
        .await
        .unwrap();
        if due == 0 || ticks > 200 {
            break;
        }
    }
    let el = t.elapsed();
    let runs: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM feed_fetch_runs GROUP BY status")
            .fetch_all(&st.db)
            .await
            .unwrap();
    let (arts, dup): (i64, i64) =
        sqlx::query_as("SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT canonical_url) FROM articles")
            .fetch_one(&st.db)
            .await
            .unwrap();
    let ext: Vec<(String, i64)> = sqlx::query_as(
        "SELECT extraction_status, COUNT(*) FROM articles GROUP BY extraction_status",
    )
    .fetch_all(&st.db)
    .await
    .unwrap();
    let jobs: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM article_extraction_jobs GROUP BY status")
            .fetch_all(&st.db)
            .await
            .unwrap();
    let size = std::fs::metadata(tmp.path().join("r.db"))
        .map(|m| m.len())
        .unwrap_or(0)
        + std::fs::metadata(tmp.path().join("r.db-wal"))
            .map(|m| m.len())
            .unwrap_or(0);
    println!(
        "LOAD first pass: ticks={ticks} elapsed={el:?} runs={runs:?} articles={arts} dup_urls={dup} article_status={ext:?} jobs={jobs:?} db+wal_bytes={size}"
    );
    assert_eq!(arts, 1000, "first pass did not store 1000 articles");
    assert_eq!(dup, 0);

    sqlx::query(
        "UPDATE feeds SET next_scheduled_fetch_at = '2000-01-01T00:00:00Z', consecutive_failures = 0",
    )
    .execute(&st.db)
    .await
    .unwrap();
    let t2 = Instant::now();
    for _ in 0..40 {
        st.coordinator.run_scheduler_tick().await.unwrap();
    }
    let runs2: Vec<(String, i64)> =
        sqlx::query_as("SELECT status, COUNT(*) FROM feed_fetch_runs GROUP BY status")
            .fetch_all(&st.db)
            .await
            .unwrap();
    let (arts2,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM articles")
        .fetch_one(&st.db)
        .await
        .unwrap();
    let (reext,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM articles WHERE extraction_attempts > 1")
            .fetch_one(&st.db)
            .await
            .unwrap();
    println!(
        "LOAD repeat pass: elapsed={:?} runs={runs2:?} articles={arts2} re-extracted={reext}",
        t2.elapsed()
    );
    assert_eq!(arts2, 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: Stage 11"]
async fn r28_load_concurrent_extraction_failures() {
    let addr = load_fixture(true).await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(addr));
    let (_tmp, st) = state_with(
        resolver,
        &[
            ("QUIRE_FETCH_CONCURRENCY", "1"),
            ("QUIRE_EXTRACTION_CONCURRENCY", "8"),
        ],
    )
    .await;
    for n in 0..10 {
        new_feed(
            &st.db,
            &format!("http://feeds.example.com:{}/feed/{n}", addr.port()),
        )
        .await;
    }
    let t = Instant::now();
    for _ in 0..10 {
        st.coordinator.run_scheduler_tick().await.unwrap();
    }
    let rows: Vec<(String, String, i64, i64)> = sqlx::query_as(
        "SELECT a.extracted_content_type, j.status, j.attempts, COUNT(*)
         FROM articles a
         JOIN article_extraction_jobs j ON j.article_id = a.id
         GROUP BY 1, 2, 3",
    )
    .fetch_all(&st.db)
    .await
    .unwrap();
    println!("FAILLOAD elapsed={:?} rows={rows:?}", t.elapsed());
    assert!(!rows.is_empty(), "no extraction jobs were created");
    for (content, status, attempts, count) in &rows {
        assert_ne!(status, "failed", "500 must stay retryable, count={count}");
        assert!(*attempts <= 3, "attempts={attempts}");
        if *attempts > 0 {
            assert_eq!(
                content, "summary_fallback",
                "status={status} attempts={attempts}"
            );
            assert_eq!(status, "queued");
        }
    }
}
