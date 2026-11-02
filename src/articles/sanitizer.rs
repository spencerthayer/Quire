use std::collections::HashSet;

use ammonia::{Builder, UrlRelative};
use dom_query::Document;
use url::Url;

/// Sanitize untrusted article or summary HTML, stripping scripts, event handlers,
/// styles, forms, and non-content widgets while preserving structural article
/// markup (`<p>`, `<h1>`-`<h6>`, `<ul>`, `<ol>`, `<li>`, `<blockquote>`, `<pre>`,
/// `<code>`, `<a>`, `<figure>`, `<figcaption>`, `<table>`, etc.).
///
/// Images with usable `alt` text are converted into `<figure><figcaption>` blocks
/// so image descriptions remain readable without downloading remote image binaries.
pub fn sanitize_article_html(raw_html: &str, base_url: Option<&Url>) -> String {
    let preprocessed = preprocess_dom(raw_html);

    let allowed_tags: HashSet<&str> = [
        "p",
        "br",
        "hr",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "ul",
        "ol",
        "li",
        "dl",
        "dt",
        "dd",
        "blockquote",
        "pre",
        "code",
        "em",
        "i",
        "strong",
        "b",
        "u",
        "s",
        "del",
        "ins",
        "sub",
        "sup",
        "mark",
        "small",
        "abbr",
        "cite",
        "q",
        "figure",
        "figcaption",
        "table",
        "thead",
        "tbody",
        "tfoot",
        "tr",
        "th",
        "td",
        "caption",
        "a",
        "div",
        "span",
        "section",
        "article",
    ]
    .into_iter()
    .collect();

    let clean_content_tags: HashSet<&str> = [
        "script", "style", "noscript", "iframe", "object", "embed", "applet", "form", "svg",
        "math", "canvas", "audio", "video", "nav", "header", "footer", "aside",
    ]
    .into_iter()
    .collect();

    let mut builder = Builder::default();
    builder
        .tags(allowed_tags)
        .clean_content_tags(clean_content_tags)
        .url_schemes(["http", "https", "mailto"].into_iter().collect())
        .link_rel(Some("noopener noreferrer"));

    if let Some(base) = base_url.cloned() {
        builder.url_relative(UrlRelative::RewriteWithBase(base));
    } else {
        builder.url_relative(UrlRelative::Deny);
    }

    builder.clean(&preprocessed).to_string()
}

fn preprocess_dom(raw_html: &str) -> String {
    let doc = Document::from(raw_html);

    // Remove common non-article noise containers before sanitization
    let noise_selector = [
        "script",
        "style",
        "noscript",
        "iframe",
        "form",
        "nav",
        "footer",
        "aside",
        "[role='navigation']",
        "[role='banner']",
        "[role='contentinfo']",
        "[role='complementary']",
        ".cookie-banner",
        ".cookie-consent",
        ".newsletter-signup",
        ".advertisement",
        ".ad-container",
        ".social-share",
        ".share-buttons",
        ".related-articles",
        ".related-posts",
    ]
    .join(", ");

    let sel = doc.select(&noise_selector);
    if sel.exists() {
        sel.remove();
    }

    // Convert <img> tags with non-empty alt attributes into readable caption notes
    // while removing raw remote image tags.
    for img_node in doc.select("img").nodes() {
        let sel_node = dom_query::Selection::from(*img_node);
        let alt = sel_node
            .attr("alt")
            .map(|a| a.trim().to_string())
            .filter(|a| !a.is_empty());
        if let Some(alt_text) = alt {
            let escaped = escape_html_text(&alt_text);
            sel_node.replace_with_html(format!(
                "<figure><figcaption>[Image: {escaped}]</figcaption></figure>"
            ));
        } else {
            sel_node.remove();
        }
    }

    doc.select("body").inner_html().to_string()
}

fn escape_html_text(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Sanitize a feed-provided plain-text or HTML summary into safe HTML suitable
/// for fallback article display.
pub fn sanitize_summary_fallback(raw_summary: &str, base_url: Option<&Url>) -> Option<String> {
    let trimmed = raw_summary.trim();
    if trimmed.is_empty() {
        return None;
    }

    let candidate_html = if trimmed.contains('<') && trimmed.contains('>') {
        sanitize_article_html(trimmed, base_url)
    } else {
        let escaped = escape_html_text(trimmed);
        format!("<p>{escaped}</p>")
    };

    let text_check = crate::articles::normalizer::html_to_normalized_text(&candidate_html);
    if text_check.is_empty() {
        None
    } else {
        Some(candidate_html)
    }
}
