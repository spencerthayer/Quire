use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use rand::Rng;
use reqwest::StatusCode;
use reqwest::header::{self, HeaderMap, HeaderValue};
use reqwest::redirect::Policy;
use thiserror::Error;
use tokio::sync::watch;
use url::{Host, Url};

use crate::feeds::validation::{HostResolver, is_public_unicast, parse_and_validate_http_url};

pub const USER_AGENT: &str = "Quire/0.2 (+https://github.com/spencerthayer/Quire)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchErrorCategory {
    Validation,
    SsrfBlocked,
    Dns,
    Connect,
    Tls,
    Timeout,
    Redirect,
    HttpClientError,
    HttpServerError,
    RateLimited,
    ResponseTooLarge,
    DecodeError,
    Cancelled,
}

impl FetchErrorCategory {
    pub fn as_str(self) -> &'static str {
        match self {
            FetchErrorCategory::Validation => "validation",
            FetchErrorCategory::SsrfBlocked => "ssrf_blocked",
            FetchErrorCategory::Dns => "dns",
            FetchErrorCategory::Connect => "connect",
            FetchErrorCategory::Tls => "tls",
            FetchErrorCategory::Timeout => "timeout",
            FetchErrorCategory::Redirect => "redirect",
            FetchErrorCategory::HttpClientError => "http_client",
            FetchErrorCategory::HttpServerError => "http_server",
            FetchErrorCategory::RateLimited => "rate_limited",
            FetchErrorCategory::ResponseTooLarge => "response_too_large",
            FetchErrorCategory::DecodeError => "decode",
            FetchErrorCategory::Cancelled => "cancelled",
        }
    }
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct FetchError {
    pub category: FetchErrorCategory,
    pub http_status: Option<u16>,
    pub message: String,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
}

impl FetchError {
    pub fn new(
        category: FetchErrorCategory,
        http_status: Option<u16>,
        message: impl Into<String>,
        retryable: bool,
    ) -> Self {
        Self {
            category,
            http_status,
            message: message.into(),
            retryable,
            retry_after: None,
        }
    }

    pub fn with_retry_after(mut self, retry_after: Option<Duration>) -> Self {
        self.retry_after = retry_after;
        self
    }
}

#[derive(Debug, Clone)]
pub struct FetchRequestOptions {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_response_bytes: usize,
    pub max_redirects: usize,
    pub max_attempts: u32,
    pub retry_initial_backoff: Duration,
    pub retry_max_backoff: Duration,
    /// When `true` (article extraction layer per Revision 2 §4.2), the HTTP layer
    /// retries ONLY connection-reset / connect failures (at most 2 attempts, fixed
    /// 1s delay) and returns timeouts, 5xx, 429, and DNS errors immediately to the
    /// job layer (`retryable: true`) so retries are not multiplied across layers.
    pub retry_connect_only: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub accept_header: &'static str,
}

impl Default for FetchRequestOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            max_response_bytes: 10 * 1024 * 1024,
            max_redirects: 5,
            max_attempts: 3,
            retry_initial_backoff: Duration::from_secs(2),
            retry_max_backoff: Duration::from_secs(30),
            retry_connect_only: false,
            etag: None,
            last_modified: None,
            accept_header: "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
        }
    }
}

#[derive(Debug, Clone)]
pub enum FetchOutcome {
    NotModified {
        http_status: u16,
        final_url: Url,
        etag: Option<String>,
        last_modified: Option<String>,
        duration_ms: u64,
    },
    Fetched {
        http_status: u16,
        final_url: Url,
        bytes: Vec<u8>,
        content_type: Option<String>,
        etag: Option<String>,
        last_modified: Option<String>,
        duration_ms: u64,
    },
}

/// Execute an SSRF-protected, DNS-pinned, redirect-validated, streaming-bounded
/// HTTP GET request with bounded retries.
pub async fn fetch_url_with_retries(
    raw_url: &str,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
) -> Result<FetchOutcome, FetchError> {
    fetch_url_with_retries_cancellable(raw_url, options, resolver, None).await
}

pub async fn fetch_url_with_retries_cancellable(
    raw_url: &str,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
    mut cancel_rx: Option<watch::Receiver<bool>>,
) -> Result<FetchOutcome, FetchError> {
    let max_attempts = if options.retry_connect_only {
        options.max_attempts.clamp(1, 2)
    } else {
        options.max_attempts.max(1)
    };
    let mut last_err: Option<FetchError> = None;

    for attempt in 1..=max_attempts {
        if cancel_rx.as_ref().is_some_and(|rx| *rx.borrow()) {
            return Err(FetchError::new(
                FetchErrorCategory::Cancelled,
                None,
                "Request cancelled due to shutdown.",
                true,
            ));
        }

        let single_fut = execute_single_fetch(raw_url, options, resolver);
        let single_res = if let Some(rx) = cancel_rx.as_mut() {
            tokio::select! {
                res = single_fut => res,
                _ = rx.changed() => {
                    return Err(FetchError::new(
                        FetchErrorCategory::Cancelled,
                        None,
                        "Request cancelled due to shutdown.",
                        true,
                    ));
                }
            }
        } else {
            single_fut.await
        };

        match single_res {
            Ok((outcome, _retry_after)) => return Ok(outcome),
            Err((err, retry_after)) => {
                let err_with_ra = err.with_retry_after(retry_after);
                let should_retry_at_http_layer = if options.retry_connect_only {
                    err_with_ra.category == FetchErrorCategory::Connect && err_with_ra.retryable
                } else {
                    err_with_ra.retryable
                };

                if !should_retry_at_http_layer || attempt == max_attempts {
                    return Err(err_with_ra);
                }

                let backoff = if options.retry_connect_only {
                    Duration::from_secs(1)
                } else {
                    compute_backoff(
                        attempt,
                        options.retry_initial_backoff,
                        options.retry_max_backoff,
                        retry_after,
                    )
                };
                last_err = Some(err_with_ra);

                if let Some(rx) = cancel_rx.as_mut() {
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        _ = rx.changed() => {
                            return Err(FetchError::new(
                                FetchErrorCategory::Cancelled,
                                None,
                                "Request cancelled due to shutdown.",
                                true,
                            ));
                        }
                    }
                } else {
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        FetchError::new(
            FetchErrorCategory::Connect,
            None,
            "Request failed after exhausting retry attempts.",
            false,
        )
    }))
}

fn compute_backoff(
    attempt: u32,
    initial: Duration,
    max_backoff: Duration,
    retry_after: Option<Duration>,
) -> Duration {
    if let Some(ra) = retry_after {
        return ra.min(max_backoff);
    }
    let factor = 2u64.saturating_pow(attempt.saturating_sub(1));
    let base_ms = (initial.as_millis() as u64).saturating_mul(factor);
    let capped_ms = base_ms.min(max_backoff.as_millis() as u64);
    if capped_ms == 0 {
        return Duration::from_millis(0);
    }
    let jitter_max = (capped_ms / 4).max(1);
    let jitter = rand::rng().random_range(0..=jitter_max);
    Duration::from_millis((capped_ms + jitter).min(max_backoff.as_millis() as u64))
}

async fn execute_single_fetch(
    raw_url: &str,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
) -> Result<(FetchOutcome, Option<Duration>), (FetchError, Option<Duration>)> {
    let started = Instant::now();
    let mut current_url = parse_and_validate_http_url(raw_url, "Target URL").map_err(|e| {
        let msg = e.to_string();
        let cat = if msg.contains("cannot target") || msg.contains("restricted") {
            FetchErrorCategory::SsrfBlocked
        } else {
            FetchErrorCategory::Validation
        };
        (FetchError::new(cat, None, msg, false), None)
    })?;

    for redirect_count in 0..=options.max_redirects {
        let elapsed = started.elapsed();
        if elapsed >= options.request_timeout {
            return Err((
                FetchError::new(
                    FetchErrorCategory::Timeout,
                    None,
                    format!(
                        "Request timed out after {}s.",
                        options.request_timeout.as_secs()
                    ),
                    true,
                ),
                None,
            ));
        }
        let remaining_timeout = options.request_timeout.saturating_sub(elapsed);

        let host_enum = current_url.host().ok_or_else(|| {
            (
                FetchError::new(
                    FetchErrorCategory::Validation,
                    None,
                    "URL is missing a host.",
                    false,
                ),
                None,
            )
        })?;
        let port = current_url.port_or_known_default().unwrap_or(80);

        let (resolved_addrs, domain_to_pin): (Vec<SocketAddr>, Option<String>) = match host_enum {
            Host::Ipv4(v4) => {
                let ip = std::net::IpAddr::V4(v4);
                if !is_public_unicast(&ip) {
                    return Err((
                        FetchError::new(
                            FetchErrorCategory::SsrfBlocked,
                            None,
                            format!("Target IP '{ip}' is not a public unicast address."),
                            false,
                        ),
                        None,
                    ));
                }
                (vec![SocketAddr::new(ip, port)], None)
            }
            Host::Ipv6(v6) => {
                let ip = std::net::IpAddr::V6(v6);
                if !is_public_unicast(&ip) {
                    return Err((
                        FetchError::new(
                            FetchErrorCategory::SsrfBlocked,
                            None,
                            format!("Target IPv6 '{ip}' is not a public unicast address."),
                            false,
                        ),
                        None,
                    ));
                }
                (vec![SocketAddr::new(ip, port)], None)
            }
            Host::Domain(domain) => {
                let resolve_res =
                    tokio::time::timeout(remaining_timeout, resolver.resolve(domain, port))
                        .await
                        .map_err(|_| {
                            (
                                FetchError::new(
                                    FetchErrorCategory::Timeout,
                                    None,
                                    format!(
                                        "DNS resolution timed out for host '{domain}' after {}s.",
                                        remaining_timeout.as_secs()
                                    ),
                                    true,
                                ),
                                None,
                            )
                        })?;

                let addrs = resolve_res.map_err(|e| {
                    let msg = e.to_string();
                    let (cat, retryable) = if msg.contains("restricted network address") {
                        (FetchErrorCategory::SsrfBlocked, false)
                    } else {
                        (FetchErrorCategory::Dns, true)
                    };
                    (FetchError::new(cat, None, msg, retryable), None)
                })?;

                if addrs.is_empty() {
                    return Err((
                        FetchError::new(
                            FetchErrorCategory::Dns,
                            None,
                            format!("DNS resolution returned no addresses for '{domain}'."),
                            true,
                        ),
                        None,
                    ));
                }

                // Enforcement Point 2 (Revision 2 §5.4 / QUA-012):
                // Validate every resolved IP inside the fetcher regardless of which
                // HostResolver implementation returned it.
                for addr in &addrs {
                    if !is_public_unicast(&addr.ip()) && !resolver.allows_test_fixture_addr(addr) {
                        return Err((
                            FetchError::new(
                                FetchErrorCategory::SsrfBlocked,
                                None,
                                format!(
                                    "Host '{domain}' resolved to restricted non-public address '{}'.",
                                    addr.ip()
                                ),
                                false,
                            ),
                            None,
                        ));
                    }
                }

                (addrs, Some(domain.to_string()))
            }
        };

        let remaining_after_dns = options
            .request_timeout
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));

        let mut client_builder = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(options.connect_timeout.min(remaining_after_dns))
            .timeout(remaining_after_dns);

        if let Some(domain) = &domain_to_pin {
            client_builder = client_builder.resolve_to_addrs(domain, &resolved_addrs);
        }

        let client = client_builder.build().map_err(|e| {
            (
                FetchError::new(
                    FetchErrorCategory::Connect,
                    None,
                    format!("Failed to build HTTP client: {e}"),
                    false,
                ),
                None,
            )
        })?;

        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static(options.accept_header),
        );

        if redirect_count == 0 {
            if let Some(etag) = options.etag.as_deref().filter(|s| !s.trim().is_empty())
                && let Ok(val) = HeaderValue::from_str(etag.trim())
            {
                headers.insert(header::IF_NONE_MATCH, val);
            }
            if let Some(lm) = options
                .last_modified
                .as_deref()
                .filter(|s| !s.trim().is_empty())
                && let Ok(val) = HeaderValue::from_str(lm.trim())
            {
                headers.insert(header::IF_MODIFIED_SINCE, val);
            }
        }

        let response = client
            .get(current_url.clone())
            .headers(headers)
            .send()
            .await
            .map_err(|e| (classify_reqwest_error(e), None))?;

        // Enforcement Point 4 (Revision 2 §5.4):
        // Verify that the connected peer socket address matches one of the
        // validated addresses for this hop and is a public unicast IP.
        match response.remote_addr() {
            Some(peer_addr) => {
                let matches_validated = resolved_addrs
                    .iter()
                    .any(|a| a.ip() == peer_addr.ip() && a.port() == peer_addr.port());
                let ip_allowed = is_public_unicast(&peer_addr.ip())
                    || resolver.allows_test_fixture_addr(&peer_addr);
                if !matches_validated || !ip_allowed {
                    return Err((
                        FetchError::new(
                            FetchErrorCategory::SsrfBlocked,
                            None,
                            format!(
                                "Connected peer address '{peer_addr}' did not match validated public destination."
                            ),
                            false,
                        ),
                        None,
                    ));
                }
            }
            None => {
                return Err((
                    FetchError::new(
                        FetchErrorCategory::SsrfBlocked,
                        None,
                        "Connected response did not expose a verifiable peer address.",
                        false,
                    ),
                    None,
                ));
            }
        }

        let status = response.status();
        let status_code = status.as_u16();
        let resp_headers = response.headers().clone();

        if matches!(
            status,
            StatusCode::MOVED_PERMANENTLY
                | StatusCode::FOUND
                | StatusCode::SEE_OTHER
                | StatusCode::TEMPORARY_REDIRECT
                | StatusCode::PERMANENT_REDIRECT
        ) {
            if redirect_count >= options.max_redirects {
                return Err((
                    FetchError::new(
                        FetchErrorCategory::Redirect,
                        Some(status_code),
                        format!(
                            "Exceeded maximum redirect limit of {}.",
                            options.max_redirects
                        ),
                        false,
                    ),
                    None,
                ));
            }

            let location = resp_headers
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    (
                        FetchError::new(
                            FetchErrorCategory::Redirect,
                            Some(status_code),
                            format!(
                                "HTTP {status_code} redirect response missing Location header."
                            ),
                            false,
                        ),
                        None,
                    )
                })?;

            let joined = current_url.join(location.trim()).map_err(|e| {
                (
                    FetchError::new(
                        FetchErrorCategory::Redirect,
                        Some(status_code),
                        format!("Invalid redirect Location '{location}': {e}"),
                        false,
                    ),
                    None,
                )
            })?;

            // Enforcement Point 3: Validate redirect target against full URL + SSRF policy
            let validated_next = parse_and_validate_http_url(joined.as_str(), "Redirect URL")
                .map_err(|e| {
                    let msg = e.to_string();
                    let cat = if msg.contains("cannot target") || msg.contains("restricted") {
                        FetchErrorCategory::SsrfBlocked
                    } else {
                        FetchErrorCategory::Redirect
                    };
                    (FetchError::new(cat, Some(status_code), msg, false), None)
                })?;

            current_url = validated_next;
            continue;
        }

        let etag = resp_headers
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let last_modified = resp_headers
            .get(header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        if status == StatusCode::NOT_MODIFIED {
            return Ok((
                FetchOutcome::NotModified {
                    http_status: status_code,
                    final_url: current_url,
                    etag: etag.or_else(|| options.etag.clone()),
                    last_modified: last_modified.or_else(|| options.last_modified.clone()),
                    duration_ms: started.elapsed().as_millis() as u64,
                },
                None,
            ));
        }

        let retry_after = parse_retry_after(&resp_headers);

        if status == StatusCode::TOO_MANY_REQUESTS || status == StatusCode::REQUEST_TIMEOUT {
            return Err((
                FetchError::new(
                    FetchErrorCategory::RateLimited,
                    Some(status_code),
                    format!("HTTP {status_code} response received."),
                    true,
                ),
                retry_after,
            ));
        }

        if status.is_server_error() {
            return Err((
                FetchError::new(
                    FetchErrorCategory::HttpServerError,
                    Some(status_code),
                    format!("Upstream server returned HTTP {status_code}."),
                    true,
                ),
                retry_after,
            ));
        }

        if !status.is_success() {
            return Err((
                FetchError::new(
                    FetchErrorCategory::HttpClientError,
                    Some(status_code),
                    format!("HTTP request failed with status {status_code}."),
                    false,
                ),
                None,
            ));
        }

        if let Some(cl) = response.content_length()
            && cl > options.max_response_bytes as u64
        {
            return Err((
                FetchError::new(
                    FetchErrorCategory::ResponseTooLarge,
                    Some(status_code),
                    format!(
                        "Response Content-Length ({cl} bytes) exceeds limit of {} bytes.",
                        options.max_response_bytes
                    ),
                    false,
                ),
                None,
            ));
        }

        let content_type = resp_headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string());

        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk_res) = stream.next().await {
            if started.elapsed() > options.request_timeout {
                return Err((
                    FetchError::new(
                        FetchErrorCategory::Timeout,
                        Some(status_code),
                        format!(
                            "Response streaming timed out after {}s.",
                            options.request_timeout.as_secs()
                        ),
                        true,
                    ),
                    None,
                ));
            }
            let chunk = chunk_res.map_err(|e| (classify_reqwest_error(e), None))?;
            if body.len().saturating_add(chunk.len()) > options.max_response_bytes {
                return Err((
                    FetchError::new(
                        FetchErrorCategory::ResponseTooLarge,
                        Some(status_code),
                        format!(
                            "Response body exceeded maximum allowed size of {} bytes.",
                            options.max_response_bytes
                        ),
                        false,
                    ),
                    None,
                ));
            }
            body.extend_from_slice(&chunk);
        }

        return Ok((
            FetchOutcome::Fetched {
                http_status: status_code,
                final_url: current_url,
                bytes: body,
                content_type,
                etag,
                last_modified,
                duration_ms: started.elapsed().as_millis() as u64,
            },
            None,
        ));
    }

    Err((
        FetchError::new(
            FetchErrorCategory::Redirect,
            None,
            format!(
                "Exceeded maximum redirect limit of {}.",
                options.max_redirects
            ),
            false,
        ),
        None,
    ))
}

fn classify_reqwest_error(err: reqwest::Error) -> FetchError {
    if err.is_timeout() {
        return FetchError::new(
            FetchErrorCategory::Timeout,
            err.status().map(|s| s.as_u16()),
            "HTTP request timed out.",
            true,
        );
    }
    if err.is_connect() {
        let lower = err.to_string().to_ascii_lowercase();
        if lower.contains("tls") || lower.contains("certificate") || lower.contains("ssl") {
            return FetchError::new(
                FetchErrorCategory::Tls,
                None,
                format!("TLS handshake failed: {err}"),
                false,
            );
        }
        return FetchError::new(
            FetchErrorCategory::Connect,
            None,
            format!("Connection failed: {err}"),
            true,
        );
    }
    if err.is_redirect() {
        return FetchError::new(
            FetchErrorCategory::Redirect,
            err.status().map(|s| s.as_u16()),
            format!("Redirect error: {err}"),
            false,
        );
    }
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("certificate") || lower.contains("tls") || lower.contains("handshake") {
        return FetchError::new(
            FetchErrorCategory::Tls,
            None,
            format!("TLS error: {err}"),
            false,
        );
    }
    FetchError::new(
        FetchErrorCategory::Connect,
        err.status().map(|s| s.as_u16()),
        format!("Network error: {err}"),
        true,
    )
}

pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let val = headers.get(header::RETRY_AFTER)?.to_str().ok()?.trim();
    if let Ok(secs) = val.parse::<u64>() {
        return Some(Duration::from_secs(secs));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(val) {
        let now = chrono::Utc::now();
        let diff = dt.with_timezone(&chrono::Utc) - now;
        if diff.num_seconds() > 0 {
            return Some(Duration::from_secs(diff.num_seconds() as u64));
        }
    }
    None
}
