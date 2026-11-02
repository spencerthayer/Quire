use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};

use crate::error::AppResult;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ArticleSummaryRow {
    pub id: i64,
    pub feed_id: i64,
    pub feed_name: String,
    pub external_guid: Option<String>,
    pub canonical_url: String,
    pub title: String,
    pub author: Option<String>,
    pub description: Option<String>,
    pub published_at: Option<String>,
    pub discovered_at: String,
    pub extraction_status: String,
    pub created_at: String,
}

pub async fn list_recent_articles(
    pool: &SqlitePool,
    limit: i64,
) -> AppResult<Vec<ArticleSummaryRow>> {
    let articles = sqlx::query_as::<_, ArticleSummaryRow>(
        "SELECT
            a.id,
            a.feed_id,
            f.name AS feed_name,
            a.external_guid,
            a.canonical_url,
            a.title,
            a.author,
            a.description,
            a.published_at,
            a.discovered_at,
            a.extraction_status,
            a.created_at
         FROM articles a
         INNER JOIN feeds f ON f.id = a.feed_id
         ORDER BY COALESCE(a.published_at, a.discovered_at) DESC, a.id DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(articles)
}
