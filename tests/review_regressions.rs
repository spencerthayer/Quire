//! Reviewer regression suite ported from `docs/reviews/phase-2-artifacts/review_tests.rs`.
//! In Stage 1, baseline defects are marked `#[ignore = "QUA-xxx: fixed in stage N"]`
//! so `cargo test --test review_regressions -- --ignored` reproduces the baseline
//! failures before remediation, and each stage un-ignores its tests as fixes land.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{StatusCode, header};
use axum::routing::get;
use flate2::Compression;
use flate2::write::GzEncoder;
use quire::articles::extractor::{ExtractionResult, fetch_and_extract_article};
use quire::articles::normalizer::canonicalize_article_url;
use quire::articles::sanitizer::sanitize_article_html;
use quire::articles::{
    claim_next_extraction_job, complete_extraction_job, enqueue_manual_article_extraction,
    persist_discovered_entries, recover_interrupted_extractions,
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
    max_inflight: Arc<AtomicUsize>,
}

async fn spawn_fixture() -> Fixture {
    let secret_hits = Arc::new(AtomicUsize::new(0));
    let inflight = Arc::new(AtomicUsize::new(0));
    let max_inflight = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let port = addr.port();
    let bomb = Arc::new(build_gzip_bomb_bytes(8 * 1024 * 1024));

    let sh = secret_hits.clone();
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
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "down") }),
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
                async move {
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
#[ignore = "QUA-012: AnyHost 127.0.0.1 peer/resolver check added in Stage 8"]
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
#[ignore = "QUA-012: fixed in Stage 8"]
fn r02_ipv6_embedded_ipv4_transition_prefixes_blocked() {
    let mut leaks = vec![];
    for s in [
        "64:ff9b::7f00:1",
        "64:ff9b::a9fe:a9fe",
        "2002:7f00:1::",
        "2002:c0a8:101::",
        "fec0::1",
    ] {
        let ip: std::net::IpAddr = s.parse().unwrap();
        if !is_disallowed_ip(&ip) {
            leaks.push(s);
        }
    }
    assert!(leaks.is_empty(), "not blocked: {leaks:?}");
}

#[tokio::test]
#[ignore = "QUA-013: fixed in Stage 8"]
async fn r03_system_resolver_handles_bracketed_ipv6_literal() {
    let r = SystemDnsResolver;
    let res = r.resolve("[2606:4700:4700::1111]", 80).await;
    assert!(
        res.is_ok(),
        "public IPv6 literal feed URLs cannot be resolved: {res:?}"
    );
}

#[tokio::test]
#[ignore = "QUA-012: fixed in Stage 8"]
async fn r04_fetcher_trusts_resolver_output() {
    let fx = spawn_fixture().await;
    let resolver: Arc<dyn HostResolver> = Arc::new(AnyHost(fx.addr));
    let url = format!("http://rebind.example.com:{}/secret", fx.addr.port());
    let res = fetch_url_with_retries(&url, &opts(), &resolver).await;
    assert!(
        res.is_err(),
        "fetcher connected to 127.0.0.1 because the injected resolver did not filter"
    );
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
#[ignore = "QUA-005: fixed in Stage 8"]
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
#[ignore = "QUA-005: fixed in Stage 8"]
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
#[ignore = "QUA-015: fixed in Stage 8"]
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
#[ignore = "QUA-016: fixed in Stage 8"]
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
#[ignore = "QUA-002: fixed in Stage 6"]
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
#[ignore = "QUA-002: fixed in Stage 6"]
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
#[ignore = "QUA-014: fixed in Stage 6"]
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
#[ignore = "QUA-001: fixed in Stage 2"]
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
#[ignore = "QUA-004: fixed in Stage 4"]
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
    drop(j);
    let requeued = enqueue_manual_article_extraction(&st.db, 1, 3)
        .await
        .unwrap();
    let drained = st.coordinator.drain_eligible_extractions().await.unwrap();
    assert!(
        requeued || drained > 0,
        "job permanently stuck in 'processing' until process restart"
    );
}

#[tokio::test]
#[ignore = "QUA-010: fixed in Stage 4"]
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
#[ignore = "QUA-011: fixed in Stage 4"]
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
}

// -------------------------------------------------------- feed deletion ----

#[tokio::test]
#[ignore = "QUA-007: fixed in Stage 7"]
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
    assert_eq!(
        n, 1,
        "deleting a feed silently removed articles from an existing edition"
    );
}

#[tokio::test]
#[ignore = "QUA-006: fixed in Stage 7"]
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
#[ignore = "QUA-003: fixed in Stage 3"]
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
    let _started = st
        .coordinator
        .trigger_manual_fetch_all_enabled()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let mx = fx.max_inflight.load(Ordering::SeqCst);
    assert!(mx <= 2, "fetch_concurrency=2 exceeded: {mx}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "QUA-001: fixed in Stage 2"]
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
#[ignore = "QUA-001: fixed in Stage 2"]
async fn r25_scheduler_tick_e2e_concurrent_feeds() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/feed/{n}",
        get(|axum::extract::Path(n): axum::extract::Path<u32>| async move {
            let mut items = String::new();
            for i in 0..40 {
                items.push_str(&format!("<item><guid>n{n}-{i}</guid><title>t{i}</title><link>https://site{n}.example.com/a/{i}</link><description>summary {i}</description></item>"));
            }
            (
                [(header::CONTENT_TYPE, "application/rss+xml")],
                format!("<rss version=\"2.0\"><channel><title>f{n}</title>{items}</channel></rss>"),
            )
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
        pool.close().await;
    }
}

async fn big_feed_fixture(entries: u32) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new().route(
        "/feed/{n}",
        get(move |axum::extract::Path(n): axum::extract::Path<u32>| async move {
            let mut items = String::new();
            for i in 0..entries {
                items.push_str(&format!("<item><guid>n{n}-{i}</guid><title>t{i}</title><link>http://site{n}.example.com/a/{i}</link><description>summary {i}</description></item>"));
            }
            (
                [(header::CONTENT_TYPE, "application/rss+xml")],
                format!("<rss version=\"2.0\"><channel><title>f{n}</title>{items}</channel></rss>"),
            )
        }),
    );
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "QUA-001: fixed in Stage 2"]
async fn r29_production_tick_and_fetch_all_contention() {
    let addr = big_feed_fixture(30).await;
    let resolver: Arc<dyn HostResolver> = Arc::new(FixtureHost(addr));
    let (_tmp, st) = state_with(resolver, &[("QUIRE_EXTRACTION_CONCURRENCY", "1")]).await;
    for n in 0..12 {
        new_feed(
            &st.db,
            &format!("http://feeds.example.com:{}/feed/{n}", addr.port()),
        )
        .await;
    }
    st.coordinator
        .trigger_manual_fetch_all_enabled()
        .await
        .unwrap();
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (r,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM feeds WHERE fetch_in_progress=1")
            .fetch_one(&st.db)
            .await
            .unwrap();
        if r == 0 {
            break;
        }
    }
    let failed: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM feed_fetch_runs WHERE status != 'success'")
            .fetch_one(&st.db)
            .await
            .unwrap();
    assert_eq!(failed.0, 0, "fetch-all runs failed under contention");
}
