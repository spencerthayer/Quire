use chrono::{DateTime, SecondsFormat, Utc};
use feed_rs::model::Entry;
use feed_rs::parser;
use url::Url;

use crate::articles::extractor::decode_http_bytes;
use crate::articles::normalizer::{canonicalize_article_url, normalize_whitespace};
use crate::error::{AppError, AppResult};
use crate::feeds::validation::validate_http_url;

#[derive(Debug, Clone)]
pub struct ParsedFeedMetadata {
    pub title: Option<String>,
    pub description: Option<String>,
    pub website_url: Option<String>,
    pub language: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NormalizedFeedEntry {
    pub external_guid: Option<String>,
    pub canonical_url: String,
    pub original_source_url: String,
    pub title: String,
    pub author: Option<String>,
    pub summary: Option<String>,
    pub published_at: Option<String>,
    pub entry_updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ParsedFeedDocument {
    pub metadata: ParsedFeedMetadata,
    pub entries: Vec<NormalizedFeedEntry>,
    pub entries_seen: usize,
    pub entries_skipped: usize,
    pub entries_truncated: usize,
}

/// Reject XML payloads containing `<!ENTITY` declarations to defend against
/// XML External Entity (XXE) or Billion Laughs expansion before invoking `feed-rs`.
fn reject_dangerous_xml_declarations(bytes: &[u8]) -> AppResult<()> {
    let prefix_len = bytes.len().min(65_536);
    let head = String::from_utf8_lossy(&bytes[..prefix_len]).to_ascii_uppercase();
    if head.contains("<!ENTITY") || head.contains("SYSTEM \"FILE:") || head.contains("PUBLIC \"") {
        return Err(AppError::Validation(
            "Feed XML rejected: external entity or DTD declarations are not permitted.".to_string(),
        ));
    }
    Ok(())
}

/// Parse an RSS 2.0, RSS 1.0/RDF, Atom 1.0, or JSON Feed document into normalized
/// feed metadata and entries, applying `max_entries` truncation and skipping
/// entries that lack a usable HTTP/HTTPS article URL.
pub fn parse_feed_bytes(
    raw_bytes: &[u8],
    content_type: Option<&str>,
    feed_url: &Url,
    max_entries: usize,
) -> AppResult<ParsedFeedDocument> {
    if raw_bytes.is_empty() {
        return Err(AppError::Validation(
            "Feed response body is empty.".to_string(),
        ));
    }

    reject_dangerous_xml_declarations(raw_bytes)?;

    // Decode non-UTF-8 feeds if Content-Type declares a specific charset.
    // Replace any XML header `encoding="..."` attribute with `encoding="UTF-8"`
    // before passing the decoded UTF-8 string to `feed-rs` (`quick-xml`) so it
    // does not re-decode the UTF-8 bytes a second time as ISO-8859-1.
    let decoded_str = normalize_xml_prolog_encoding(&decode_http_bytes(raw_bytes, content_type));
    let parse_builder = parser::Builder::new().build();

    let feed = parse_builder
        .parse(decoded_str.as_bytes())
        .or_else(|_| parser::Builder::new().build().parse(raw_bytes))
        .map_err(|e| AppError::Validation(format!("Failed to parse RSS/Atom feed: {e}")))?;

    let site_url = feed
        .links
        .iter()
        .find(|l| l.rel.as_deref() == Some("alternate") || l.rel.is_none())
        .and_then(|l| {
            feed_url
                .join(l.href.trim())
                .ok()
                .and_then(|u| validate_http_url(u.as_str(), "Feed Website URL").ok())
        });

    let base_for_entries = site_url
        .as_deref()
        .and_then(|u| Url::parse(u).ok())
        .unwrap_or_else(|| feed_url.clone());

    let metadata = ParsedFeedMetadata {
        title: feed
            .title
            .map(|t| normalize_whitespace(&t.content))
            .filter(|s| !s.is_empty()),
        description: feed
            .description
            .map(|d| normalize_whitespace(&d.content))
            .filter(|s| !s.is_empty()),
        website_url: site_url,
        language: feed
            .language
            .map(|l| l.trim().to_string())
            .filter(|s| !s.is_empty()),
        updated_at: feed.updated.map(format_utc_rfc3339),
    };

    let entries_seen = feed.entries.len();
    let entries_truncated = entries_seen.saturating_sub(max_entries);

    let mut entries = Vec::new();
    let mut entries_skipped = 0usize;

    for entry in feed.entries.into_iter().take(max_entries) {
        match normalize_entry(entry, &base_for_entries) {
            Some(normalized) => entries.push(normalized),
            None => entries_skipped += 1,
        }
    }

    Ok(ParsedFeedDocument {
        metadata,
        entries,
        entries_seen,
        entries_skipped,
        entries_truncated,
    })
}

fn normalize_entry(entry: Entry, base_url: &Url) -> Option<NormalizedFeedEntry> {
    let raw_link = select_entry_link(&entry)?;
    let canonical_url = canonicalize_article_url(&raw_link, Some(base_url)).ok()?;
    let original_source_url = base_url
        .join(raw_link.trim())
        .map(|u| u.to_string())
        .unwrap_or_else(|_| canonical_url.clone());

    let external_guid = {
        let id_trimmed = entry.id.trim();
        if id_trimmed.is_empty() {
            None
        } else {
            Some(id_trimmed.chars().take(512).collect::<String>())
        }
    };

    let raw_title = entry
        .title
        .map(|t| normalize_whitespace(&t.content))
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "Untitled Article".to_string());
    let title: String = raw_title.chars().take(500).collect();

    let author = if entry.authors.is_empty() {
        None
    } else {
        let joined = entry
            .authors
            .into_iter()
            .filter_map(|p| p.name)
            .map(|n| normalize_whitespace(&n))
            .filter(|n| !n.is_empty())
            .collect::<Vec<_>>()
            .join(", ");
        if joined.is_empty() {
            None
        } else {
            Some(joined.chars().take(300).collect())
        }
    };

    let summary = entry
        .content
        .and_then(|c| c.body)
        .or_else(|| entry.summary.map(|s| s.content))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let published_at = entry.published.map(format_utc_rfc3339);
    let entry_updated_at = entry.updated.map(format_utc_rfc3339);

    Some(NormalizedFeedEntry {
        external_guid,
        canonical_url,
        original_source_url,
        title,
        author,
        summary,
        published_at,
        entry_updated_at,
    })
}

fn select_entry_link(entry: &Entry) -> Option<String> {
    if let Some(alt) = entry
        .links
        .iter()
        .find(|l| l.rel.as_deref() == Some("alternate") && !l.href.trim().is_empty())
    {
        return Some(alt.href.trim().to_string());
    }
    if let Some(primary) = entry
        .links
        .iter()
        .find(|l| l.rel.is_none() && !l.href.trim().is_empty())
    {
        return Some(primary.href.trim().to_string());
    }
    if let Some(any_link) = entry.links.iter().find(|l| !l.href.trim().is_empty()) {
        return Some(any_link.href.trim().to_string());
    }
    // Some RSS feeds put a full URL inside <guid isPermaLink="true"> with no <link>
    let id_trimmed = entry.id.trim();
    if id_trimmed.starts_with("http://") || id_trimmed.starts_with("https://") {
        return Some(id_trimmed.to_string());
    }
    None
}

fn format_utc_rfc3339(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn normalize_xml_prolog_encoding(xml: &str) -> String {
    if let Some(end_idx) = xml.find("?>")
        && xml[..end_idx].starts_with("<?xml")
    {
        return format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>{}",
            &xml[end_idx + 2..]
        );
    }
    xml.to_string()
}
