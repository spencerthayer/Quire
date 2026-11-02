use sha2::{Digest, Sha256};
use url::Url;

use crate::error::AppResult;
use crate::feeds::validation::parse_and_validate_http_url;

/// Known tracking query parameters that are safe to remove without altering
/// article identity on publisher websites.
const TRACKING_QUERY_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "utm_name",
    "utm_reader",
    "utm_viz_id",
    "utm_pubreferrer",
    "utm_swu",
    "fbclid",
    "gclid",
    "dclid",
    "msclkid",
    "twclid",
    "mc_cid",
    "mc_eid",
    "oly_enc_id",
    "oly_anon_id",
    "_hsenc",
    "_hsmi",
    "hsctatracking",
    "rb_clickid",
    "vero_id",
    "wickedid",
    "yclid",
    "igshid",
    "si",
];

/// Resolve a candidate article link (which may be relative or protocol-relative)
/// against a base URL, validate that it is a safe HTTP/HTTPS URL, strip
/// fragments, and remove known tracking query parameters while preserving raw
/// query key/value text (including bare keys such as `?123`) when non-tracking
/// parameters remain (fixes QUA-016).
pub fn canonicalize_article_url(raw_link: &str, base_url: Option<&Url>) -> AppResult<String> {
    let trimmed = raw_link.trim();
    if trimmed.is_empty() {
        return Err(crate::error::AppError::Validation(
            "Article URL is empty.".to_string(),
        ));
    }

    let resolved = match Url::parse(trimmed) {
        Ok(u) => u,
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let Some(base) = base_url else {
                return Err(crate::error::AppError::Validation(format!(
                    "Relative article URL '{trimmed}' cannot be resolved without a base URL."
                )));
            };
            base.join(trimmed).map_err(|e| {
                crate::error::AppError::Validation(format!(
                    "Failed to resolve relative article URL '{trimmed}': {e}"
                ))
            })?
        }
        Err(e) => {
            return Err(crate::error::AppError::Validation(format!(
                "Invalid article URL '{trimmed}': {e}"
            )));
        }
    };

    let mut validated = parse_and_validate_http_url(resolved.as_str(), "Article URL")?;

    // Strip URL fragment (#section) so anchor links to the same article deduplicate
    validated.set_fragment(None);

    // Strip trailing dot on host if present
    if let Some(host) = validated.host_str().map(str::to_string)
        && host.ends_with('.')
    {
        let stripped = host.trim_end_matches('.');
        let _ = validated.set_host(Some(stripped));
    }

    // Conservatively remove known tracking parameters while preserving the exact
    // raw representation of kept query segments (QUA-016).
    if let Some(raw_query) = validated.query().map(str::to_string) {
        let mut kept_segments: Vec<&str> = Vec::new();
        let mut removed_any = false;

        for segment in raw_query.split('&') {
            if segment.is_empty() {
                continue;
            }
            let raw_key = segment.split('=').next().unwrap_or(segment);
            let decoded_key = url::form_urlencoded::parse(raw_key.as_bytes())
                .next()
                .map(|(k, _)| k.into_owned())
                .unwrap_or_else(|| raw_key.to_string())
                .to_ascii_lowercase();

            if TRACKING_QUERY_PARAMS.contains(&decoded_key.as_str()) {
                removed_any = true;
            } else {
                kept_segments.push(segment);
            }
        }

        if removed_any {
            if kept_segments.is_empty() {
                validated.set_query(None);
            } else {
                let rebuilt = kept_segments.join("&");
                validated.set_query(Some(&rebuilt));
            }
        }
    }

    Ok(validated.to_string())
}

/// Normalize whitespace in titles or author strings.
pub fn normalize_whitespace(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for word in input.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

/// Strip HTML tags to plain text and collapse whitespace for stable SHA-256
/// content hashing and plain-text previews.
pub fn html_to_normalized_text(html: &str) -> String {
    let doc = dom_query::Document::from(html);
    let raw_text = doc.text();
    normalize_whitespace(&raw_text)
}

/// Compute a hex-encoded SHA-256 content hash over normalized text content so
/// insignificant HTML whitespace variations do not alter the hash.
pub fn compute_content_hash(html_or_text: &str) -> String {
    let normalized = html_to_normalized_text(html_or_text);
    let source = if normalized.is_empty() {
        normalize_whitespace(html_or_text)
    } else {
        normalized
    };
    let mut hasher = Sha256::new();
    hasher.update(source.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}
