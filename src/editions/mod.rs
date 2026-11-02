use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};

use crate::error::AppResult;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct EditionRow {
    pub id: i64,
    pub title: String,
    pub publication_date: String,
    pub status: String,
    pub article_count: i64,
    pub output_file_path: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

pub async fn list_editions(pool: &SqlitePool, limit: i64) -> AppResult<Vec<EditionRow>> {
    let editions = sqlx::query_as::<_, EditionRow>(
        "SELECT
            e.id,
            e.title,
            e.publication_date,
            e.status,
            COUNT(ea.article_id) AS article_count,
            e.output_file_path,
            e.created_at,
            e.updated_at
         FROM editions e
         LEFT JOIN edition_articles ea ON ea.edition_id = e.id
         GROUP BY e.id
         ORDER BY e.publication_date DESC, e.id DESC
         LIMIT ?",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(editions)
}
