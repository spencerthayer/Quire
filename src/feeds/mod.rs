use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use url::Url;

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Feed {
    pub id: i64,
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: String,
    pub updated_at: String,
    pub last_successful_fetch_at: Option<String>,
    pub last_attempted_fetch_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewFeedInput {
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct UpdateFeedInput {
    pub name: String,
    pub feed_url: String,
    pub website_url: Option<String>,
    pub description: Option<String>,
    pub enabled: bool,
}

/// Validate and normalize an HTTP/HTTPS URL, blocking loopback, link-local,
/// unspecified, and RFC1918 private IP literals as an SSRF safeguard.
pub fn validate_http_url(raw: &str, field_label: &str) -> AppResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(format!(
            "{field_label} is required and cannot be empty."
        )));
    }
    if trimmed.len() > 2048 {
        return Err(AppError::Validation(format!(
            "{field_label} must not exceed 2048 characters."
        )));
    }

    let parsed = Url::parse(trimmed).map_err(|_| {
        AppError::Validation(format!(
            "{field_label} must be a valid absolute URL (https://... or http://...)."
        ))
    })?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => {
            return Err(AppError::Validation(format!(
                "{field_label} must use http or https scheme (got '{other}')."
            )));
        }
    }

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AppError::Validation(format!(
            "{field_label} must not embed credentials in the URL."
        )));
    }

    let Some(host_str) = parsed.host_str() else {
        return Err(AppError::Validation(format!(
            "{field_label} must include a valid host."
        )));
    };

    let lower_host = host_str.to_ascii_lowercase();
    if lower_host == "localhost"
        || lower_host.ends_with(".localhost")
        || lower_host.ends_with(".local")
        || lower_host.ends_with(".internal")
    {
        return Err(AppError::Validation(format!(
            "{field_label} cannot target localhost or internal hostnames."
        )));
    }

    let bare_host = lower_host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(&lower_host);

    if let Ok(ip) = bare_host.parse::<IpAddr>()
        && is_disallowed_ip(&ip)
    {
        return Err(AppError::Validation(format!(
            "{field_label} cannot target private, loopback, or link-local IP addresses."
        )));
    }

    Ok(parsed.to_string())
}

fn is_disallowed_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_documentation()
                || v4.octets()[0] == 100 && (v4.octets()[1] & 0b1100_0000) == 0b0100_0000
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| is_disallowed_ip(&IpAddr::V4(v4)))
        }
    }
}

fn normalize_name(name: &str) -> AppResult<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "Feed name is required and cannot be empty.".to_string(),
        ));
    }
    if trimmed.chars().count() > 200 {
        return Err(AppError::Validation(
            "Feed name must not exceed 200 characters.".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

fn normalize_optional_description(desc: Option<&str>) -> AppResult<Option<String>> {
    let Some(raw) = desc else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > 1000 {
        return Err(AppError::Validation(
            "Description must not exceed 1000 characters.".to_string(),
        ));
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_optional_website_url(url: Option<&str>) -> AppResult<Option<String>> {
    let Some(raw) = url else {
        return Ok(None);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_http_url(trimmed, "Website URL").map(Some)
}

pub async fn list_feeds(pool: &SqlitePool) -> AppResult<Vec<Feed>> {
    let feeds = sqlx::query_as::<_, Feed>(
        "SELECT
            id,
            name,
            feed_url,
            website_url,
            description,
            enabled,
            created_at,
            updated_at,
            last_successful_fetch_at,
            last_attempted_fetch_at,
            last_error
         FROM feeds
         ORDER BY name COLLATE NOCASE ASC, id ASC",
    )
    .fetch_all(pool)
    .await?;
    Ok(feeds)
}

pub async fn get_feed_by_id(pool: &SqlitePool, id: i64) -> AppResult<Feed> {
    let feed = sqlx::query_as::<_, Feed>(
        "SELECT
            id,
            name,
            feed_url,
            website_url,
            description,
            enabled,
            created_at,
            updated_at,
            last_successful_fetch_at,
            last_attempted_fetch_at,
            last_error
         FROM feeds
         WHERE id = ?",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("Feed with ID {id} was not found.")))?;
    Ok(feed)
}

pub async fn create_feed(pool: &SqlitePool, input: NewFeedInput) -> AppResult<Feed> {
    let name = normalize_name(&input.name)?;
    let feed_url = validate_http_url(&input.feed_url, "Feed URL")?;
    let website_url = normalize_optional_website_url(input.website_url.as_deref())?;
    let description = normalize_optional_description(input.description.as_deref())?;
    let enabled_int: i64 = if input.enabled { 1 } else { 0 };

    let mut tx = pool.begin().await?;
    let insert_res = sqlx::query(
        "INSERT INTO feeds (name, feed_url, website_url, description, enabled)
         VALUES (?, ?, ?, ?, ?)",
    )
    .bind(&name)
    .bind(&feed_url)
    .bind(&website_url)
    .bind(&description)
    .bind(enabled_int)
    .execute(&mut *tx)
    .await;

    let result = match insert_res {
        Ok(res) => res,
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            return Err(AppError::Conflict(format!(
                "A feed with URL '{feed_url}' is already configured."
            )));
        }
        Err(other) => return Err(AppError::Database(other)),
    };

    let id = result.last_insert_rowid();
    tx.commit().await?;

    get_feed_by_id(pool, id).await
}

pub async fn update_feed(pool: &SqlitePool, id: i64, input: UpdateFeedInput) -> AppResult<Feed> {
    let name = normalize_name(&input.name)?;
    let feed_url = validate_http_url(&input.feed_url, "Feed URL")?;
    let website_url = normalize_optional_website_url(input.website_url.as_deref())?;
    let description = normalize_optional_description(input.description.as_deref())?;
    let enabled_int: i64 = if input.enabled { 1 } else { 0 };

    let mut tx = pool.begin().await?;
    let update_res = sqlx::query(
        "UPDATE feeds
         SET name = ?,
             feed_url = ?,
             website_url = ?,
             description = ?,
             enabled = ?,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(&name)
    .bind(&feed_url)
    .bind(&website_url)
    .bind(&description)
    .bind(enabled_int)
    .bind(id)
    .execute(&mut *tx)
    .await;

    let result = match update_res {
        Ok(res) => res,
        Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
            return Err(AppError::Conflict(format!(
                "Another feed with URL '{feed_url}' already exists."
            )));
        }
        Err(other) => return Err(AppError::Database(other)),
    };

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    get_feed_by_id(pool, id).await
}

pub async fn set_feed_enabled(pool: &SqlitePool, id: i64, enabled: bool) -> AppResult<Feed> {
    let enabled_int: i64 = if enabled { 1 } else { 0 };
    let mut tx = pool.begin().await?;
    let result = sqlx::query(
        "UPDATE feeds
         SET enabled = ?,
             updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
         WHERE id = ?",
    )
    .bind(enabled_int)
    .bind(id)
    .execute(&mut *tx)
    .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    get_feed_by_id(pool, id).await
}

pub async fn delete_feed(pool: &SqlitePool, id: i64) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let result = sqlx::query("DELETE FROM feeds WHERE id = ?")
        .bind(id)
        .execute(&mut *tx)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!(
            "Feed with ID {id} was not found."
        )));
    }

    tx.commit().await?;
    Ok(())
}
