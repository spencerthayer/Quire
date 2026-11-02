use std::sync::Arc;
use std::time::Duration;

use dom_smoothie::{CandidateSelectMode, Config, Readability, TextMode};
use encoding_rs::Encoding;
use url::Url;

use crate::articles::normalizer::{compute_content_hash, html_to_normalized_text};
use crate::articles::sanitizer::{sanitize_article_html, sanitize_summary_fallback};
use crate::feeds::fetcher::{FetchOutcome, FetchRequestOptions, fetch_url_with_retries};
use crate::feeds::validation::HostResolver;

#[derive(Debug, Clone)]
pub enum ExtractionResult {
    Completed {
        sanitized_html: String,
        content_hash: String,
        final_url: String,
    },
    Fallback {
        sanitized_html: String,
        content_hash: String,
        reason: String,
    },
    Failed {
        error_message: String,
        retryable: bool,
    },
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

/// Download an article URL using the SSRF-hardened HTTP fetcher, decode the
/// character set, run `dom_smoothie` readability extraction, sanitize the
/// resulting HTML, and fall back to the feed-provided summary when extraction
/// fails and a usable summary exists.
pub async fn fetch_and_extract_article(
    article_url: &str,
    fallback_summary: Option<&str>,
    options: &FetchRequestOptions,
    resolver: &Arc<dyn HostResolver>,
) -> ExtractionResult {
    let base_url = Url::parse(article_url).ok();

    let mut req_opts = options.clone();
    req_opts.accept_header = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    req_opts.etag = None;
    req_opts.last_modified = None;

    match fetch_url_with_retries(article_url, &req_opts, resolver).await {
        Ok(FetchOutcome::Fetched {
            bytes,
            content_type,
            final_url,
            ..
        }) => {
            let decoded_html = decode_http_bytes(&bytes, content_type.as_deref());
            match extract_readable_html_from_str(&decoded_html, &final_url) {
                Ok((sanitized_html, content_hash)) => ExtractionResult::Completed {
                    sanitized_html,
                    content_hash,
                    final_url: final_url.to_string(),
                },
                Err(extract_err) => {
                    if let Some(summary) = fallback_summary
                        && let Some(fallback_html) =
                            sanitize_summary_fallback(summary, Some(&final_url))
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
                        }
                    }
                }
            }
        }
        Ok(FetchOutcome::NotModified { final_url, .. }) => {
            if let Some(summary) = fallback_summary
                && let Some(fallback_html) = sanitize_summary_fallback(summary, Some(&final_url))
            {
                let content_hash = compute_content_hash(&fallback_html);
                ExtractionResult::Fallback {
                    sanitized_html: fallback_html,
                    content_hash,
                    reason: "Unexpected HTTP 304 on article extraction.".to_string(),
                }
            } else {
                ExtractionResult::Failed {
                    error_message: "Unexpected HTTP 304 on article extraction.".to_string(),
                    retryable: true,
                }
            }
        }
        Err(fetch_err) => {
            if let Some(summary) = fallback_summary
                && let Some(fallback_html) = sanitize_summary_fallback(summary, base_url.as_ref())
            {
                let content_hash = compute_content_hash(&fallback_html);
                ExtractionResult::Fallback {
                    sanitized_html: fallback_html,
                    content_hash,
                    reason: fetch_err.message,
                }
            } else {
                ExtractionResult::Failed {
                    error_message: fetch_err.message,
                    retryable: fetch_err.retryable,
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
        max_attempts,
        retry_initial_backoff: Duration::from_secs(retry_initial_secs),
        retry_max_backoff: Duration::from_secs(retry_max_secs),
        etag: None,
        last_modified: None,
        accept_header: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
}
