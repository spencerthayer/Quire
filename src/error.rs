use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error")]
    Database(#[from] sqlx::Error),

    #[error("Migration error")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("Template rendering error")]
    Template(#[from] askama::Error),

    #[error("Internal error")]
    Internal(#[from] anyhow::Error),
}

pub type AppResult<T> = Result<T, AppError>;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match &self {
            AppError::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone()).into_response(),
            AppError::NotFound(msg) => (StatusCode::NOT_FOUND, msg.clone()).into_response(),
            AppError::Conflict(msg) => (StatusCode::CONFLICT, msg.clone()).into_response(),
            AppError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg.clone()).into_response(),
            AppError::Config(msg) => {
                error!(error = %msg, "Configuration error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "A configuration error occurred.",
                )
                    .into_response()
            }
            AppError::Database(err) => {
                error!(error = ?err, "Database operation failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "An internal database error occurred.",
                )
                    .into_response()
            }
            AppError::Migration(err) => {
                error!(error = ?err, "Database migration failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Database initialization failed.",
                )
                    .into_response()
            }
            AppError::Template(err) => {
                error!(error = ?err, "Template rendering failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to render page template.",
                )
                    .into_response()
            }
            AppError::Internal(err) => {
                error!(error = ?err, "Unhandled internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "An unexpected internal error occurred.",
                )
                    .into_response()
            }
        }
    }
}
