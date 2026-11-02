use std::sync::Arc;
use std::time::Duration;

use dom_smoothie::{CandidateSelectMode, Config, Readability, TextMode};
use encoding_rs::Encoding;
use tokio::sync::watch;
use url::Url;

use crate::articles::normalizer::{compute_content_hash, html_to_normalized_text};
use crate::articles::sanitizer::{sanitize_article_html, sanitize_summary_fallback};
use crate::feeds::fetcher::{
    FetchErrorCategory, FetchOutcome, FetchRequestOptions, fetch_url_with_retries_cancellable,
};
use crate::feeds::validation::HostResolver;

#[derive(Debug, Clone)]
pub enum ExtractionResult {
    Completed {
        sanitized_html: String,
        content_hash: String,
        final_url: String,
    },
    /// Used when a permanent (non-retryable) failure occurs on an article that
    /// has a usable feed summary, or when all retry attempts are exhausted.
    Fallback {
        sanitized_html: String,
        content_hash: String,
        reason: String,
    },
    Failed {
        error_message: String,
        retryable: bool,
        retry_after: Option<Duration>,
        interim_fallback: Option<(String, String)>,
        cancelled: bool,
    },
}

/// Check whether an HTTP `Content-Type` header (or sniffed byte prefix when the
/// header is absent or generic `text/plain` / `application/octet-stream`)
/// represents HTML or XHTML suitable for readability extraction (fixes QUA-014).
pub fn is_supported_html_response(content_type: Option<&str>, bytes: &[u8]) -> Result<(), String> {
    if bytes.starts_with(b"%PDF-") {
        return Err("Unsupported article Content-Type: PDF document cannot be extracted as HTML.".to_string());
    }

    if let Some(raw_ct) = content_type {
        let mime = raw_ct
            .split(';')
            .next()
            .unwrap_or(raw_ct)
            .trim()
            .to_ascii_lowercase();

        if mime == "text/html" || mime == "application/xhtml+xml" {
            return Ok(());
        }

        // If no Content-Type or generic octet-stream/text/plain is sent, allow only
        // if the leading non-whitespace bytes look like an HTML document.
        if mime.is_empty() || mime == "text/plain" || mime == "application/octet-stream" {
            if sniff_looks_like_html(bytes) {
                return Ok(());
            }
            return Err(format!(
                "Unsupported article Content-Type '{mime}' (response does not appear to be HTML)."
            ));
        }

        return Err(format!(
            "Unsupported article Content-Type '{mime}': only text/html and application/xhtml+xml are supported."
        ));
    }

    // No Content-Type header at all: accept unless it looks like binary
    if bytes.iter().take(512).any(|&b| b == 0) {
        return Err("Binary response body cannot be extracted as HTML.".to_string());
    }

    Ok(())
}

fn sniff_looks_like_html(bytes: &[u8]) -> bool {
    let prefix = String::from_utf8_lossy(&bytes[..bytes.len().min(1024)])
        .trim_start()
        .to_ascii_lowercase();
    prefix.starts_with("<!doctype html")
        || prefix.starts_with("<html")
        || prefix.starts_with("<head")
        || prefix.starts_with("<body")
        || prefix.starts_with("<article")
        || prefix.contains("<html")
        || prefix.contains("<article")
}

/// Extract sanitized, readable HTML from a raw HTML document string using
/// `dom_smoothie::Readability`.
pub fn extract_readable_html_from_str(
    raw_html: &str,
    document_url: &Url,
) -> Result<(String, String), String> {
    if raw_html.trim().is_empty() {
        return Err("Article HTML document is empty.".to_string());
    }

    let cfg = Config {
        text_mode: TextMode::Raw,
        candidate_select_mode: CandidateSelectMode::DomSmoothie,
        ..Default::default()
    };

    let readability_res = Readability::new(raw_html, Some(document_url.as_str()), Some(cfg))
        .and_then(|mut r| r.parse());

    let candidate_html = match readability_res {
        Ok(article) => article.content.to_string(),
        Err(e) => {
            return Err(format!("Readability extraction failed: {e:?}"));
        }
    };

    let sanitized = sanitize_article_html(&candidate_html, Some(document_url));
    let plain_text = html_to_normalized_text(&sanitized);

    if plain_text.is_empty() {
        return Err("Extracted article content was empty after sanitization.".to_string());
    }

    let hash = compute_content_hash(&sanitized);
    Ok((sanitized, hash))
}

/// Download an article URL using the SSRF-hardened HTTP fetcher, verify its
/// content type, decode the character set, and offload `dom_smoothie` readability
/// extraction and `ammonia` sanitization to `tokio::task::spawn_blocking`.
///
/// Crucially (fixes QUA-002 / Revision 2 §4.2):
/// - When a transient/retryable fetch failure occurs (such as HTTP 503, 429,
///   timeout, DNS, or connect error), this function returns
///   `ExtractionResult::Failed { retryable: true, interim_fallback, .. }` so the
///   job layer schedules a retry while still storing the sanitized summary as
///   interim content if available.
/// - Only permanent (non-retryable) failures immediately return `Fallback`.
pub async fn fetch_and_extract_article(
    article_url: &str,
    fallback_summary: Option<&str>,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
) -> ExtractionResult {
    fetch_and_extract_article_cancellable(article_url, fallback_summary, options, resolver, None)
        .await
}

pub async fn fetch_and_extract_article_cancellable(
    article_url: &str,
    fallback_summary: Option<&str>,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
    cancel_rx: Option<watch::Receiver<bool>>,
) -> ExtractionResult {
    let base_url = Url::parse(article_url).ok();

    let mut req_opts = options.clone();
    req_opts.accept_header = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    req_opts.etag = None;
    req_opts.last_modified = None;
    // Per Revision 2 §4.2: the HTTP layer for articles retries only connection resets
    // (at most 2 tries, 1s delay) so total requests per article never exceed 6.
    req_opts.retry_connect_only = true;
    req_opts.max_attempts = req_opts.max_attempts.clamp(1, 2);

    let summary_owned = fallback_summary.map(str::to_string);

    match fetch_url_with_retries_cancellable(article_url, &req_opts, resolver, cancel_rx).await {
        Ok(FetchOutcome::Fetched {
            bytes,
            content_type,
            final_url,
            ..
        }) => {
            let ct_clone = content_type.clone();
            let final_url_clone = final_url.clone();
            let summary_for_block = summary_owned.clone();

            let blocking_res = tokio::task::spawn_blocking(move || {
                if let Err(ct_err) = is_supported_html_response(ct_clone.as_deref(), &bytes) {
                    let fb = summary_for_block
                        .as_deref()
                        .and_then(|s| sanitize_summary_fallback(s, Some(&final_url_clone)))
                        .map(|html| {
                            let hash = compute_content_hash(&html);
                            (html, hash)
                        });
                    return match fb {
                        Some((sanitized_html, content_hash)) => ExtractionResult::Fallback {
                            sanitized_html,
                            content_hash,
                            reason: ct_err,
                        },
                        None => ExtractionResult::Failed {
                            error_message: ct_err,
                            retryable: false,
                            retry_after: None,
                            interim_fallback: None,
                            cancelled: false,
                        },
                    };
                }

                let decoded_html = decode_http_bytes(&bytes, ct_clone.as_deref());
                match extract_readable_html_from_str(&decoded_html, &final_url_clone) {
                    Ok((sanitized_html, content_hash)) => ExtractionResult::Completed {
                        sanitized_html,
                        content_hash,
                        final_url: final_url_clone.to_string(),
                    },
                    Err(extract_err) => {
                        if let Some(summary) = summary_for_block.as_deref()
                            && let Some(fallback_html) =
                                sanitize_summary_fallback(summary, Some(&final_url_clone))
                        {
                            let content_hash = compute_content_hash(&fallback_html);
                            ExtractionResult::Fallback {
                                sanitized_html: fallback_html,
                                content_hash,
                                reason: extract_err,
                            }
                        } else {
                            ExtractionResult::Failed {
                                error_message: extract_err,
                                retryable: false,
                                retry_after: None,
                                interim_fallback: None,
                                cancelled: false,
                            }
                        }
                    }
                }
            })
            .await;

            match blocking_res {
                Ok(res) => res,
                Err(join_err) => ExtractionResult::Failed {
                    error_message: format!("Article extraction worker panicked: {join_err}"),
                    retryable: true,
                    retry_after: None,
                    interim_fallback: None,
                    cancelled: false,
                },
            }
        }
        Ok(FetchOutcome::NotModified { final_url, .. }) => {
            let interim = summary_owned
                .as_deref()
                .and_then(|s| sanitize_summary_fallback(s, Some(&final_url)))
                .map(|html| {
                    let hash = compute_content_hash(&html);
                    (html, hash)
                });
            ExtractionResult::Failed {
                error_message: "Unexpected HTTP 304 on article extraction.".to_string(),
                retryable: true,
                retry_after: None,
                interim_fallback: interim,
                cancelled: false,
            }
        }
        Err(fetch_err) => {
            if fetch_err.category == FetchErrorCategory::Cancelled {
                return ExtractionResult::Failed {
                    error_message: fetch_err.message,
                    retryable: true,
                    retry_after: None,
                    interim_fallback: None,
                    cancelled: true,
                };
            }

            let sanitized_fb = summary_owned
                .as_deref()
                .and_then(|s| sanitize_summary_fallback(s, base_url.as_ref()))
                .map(|html| {
                    let hash = compute_content_hash(&html);
                    (html, hash)
                });

            if fetch_err.retryable {
                // QUA-002 fix: preserve retry eligibility on transient errors even when
                // a summary exists, passing the sanitized summary as `interim_fallback`
                // so the job layer can make it available immediately while keeping the
                // job queued for full-text retry.
                ExtractionResult::Failed {
                    error_message: fetch_err.message,
                    retryable: true,
                    retry_after: fetch_err.retry_after,
                    interim_fallback: sanitized_fb,
                    cancelled: false,
                }
            } else if let Some((sanitized_html, content_hash)) = sanitized_fb {
                ExtractionResult::Fallback {
                    sanitized_html,
                    content_hash,
                    reason: fetch_err.message,
                }
            } else {
                ExtractionResult::Failed {
                    error_message: fetch_err.message,
                    retryable: false,
                    retry_after: None,
                    interim_fallback: None,
                    cancelled: false,
                }
            }
        }
    }
}

pub fn decode_http_bytes(bytes: &[u8], content_type: Option<&str>) -> String {
    if let Some(ct) = content_type {
        for part in ct.split(';').skip(1) {
            if let Some((k, v)) = part.trim().split_once('=')
                && k.trim().eq_ignore_ascii_case("charset")
            {
                let label = v.trim().trim_matches('"').trim_matches('\'');
                if let Some(enc) = Encoding::for_label(label.as_bytes()) {
                    let (cow, _, _) = enc.decode(bytes);
                    return cow.into_owned();
                }
            }
        }
    }
    let (cow, _, _) = encoding_rs::UTF_8.decode(bytes);
    cow.into_owned()
}

pub fn default_article_fetch_options(
    connect_timeout_secs: u64,
    request_timeout_secs: u64,
    max_bytes: usize,
    max_attempts: u32,
    retry_initial_secs: u64,
    retry_max_secs: u64,
) -> FetchRequestOptions {
    FetchRequestOptions {
        connect_timeout: Duration::from_secs(connect_timeout_secs),
        request_timeout: Duration::from_secs(request_timeout_secs),
        max_response_bytes: max_bytes,
        max_redirects: 5,
        max_attempts: max_attempts.clamp(1, 2),
        retry_initial_backoff: Duration::from_secs(retry_initial_secs),
        retry_max_backoff: Duration::from_secs(retry_max_secs),
        retry_connect_only: true,
        etag: None,
        last_modified: None,
        accept_header: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
}
