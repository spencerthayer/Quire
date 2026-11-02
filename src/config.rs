use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use chrono_tz::Tz;
use serde::Serialize;

use crate::error::{AppError, AppResult};

pub const DEFAULT_HOST: &str = "0.0.0.0";
pub const DEFAULT_PORT: u16 = 8080;
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_DATA_DIR: &str = "./data";
pub const DEFAULT_TIMEZONE: &str = "America/Los_Angeles";

// Phase 2 Ingestion & Extraction Defaults
pub const DEFAULT_FETCH_ENABLED: bool = true;
pub const DEFAULT_FETCH_INTERVAL_MINUTES: u32 = 60;
pub const DEFAULT_FETCH_MIN_INTERVAL_MINUTES: u32 = 5;
pub const DEFAULT_FETCH_MAX_INTERVAL_MINUTES: u32 = 10_080; // 7 days
pub const DEFAULT_FETCH_CONCURRENCY: usize = 4;
pub const DEFAULT_EXTRACTION_CONCURRENCY: usize = 4;
pub const DEFAULT_CONNECT_TIMEOUT_SECONDS: u64 = 10;
pub const DEFAULT_FEED_REQUEST_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_ARTICLE_REQUEST_TIMEOUT_SECONDS: u64 = 45;
pub const DEFAULT_MAX_FEED_RESPONSE_BYTES: usize = 10 * 1024 * 1024; // 10 MiB
pub const DEFAULT_MAX_ARTICLE_RESPONSE_BYTES: usize = 15 * 1024 * 1024; // 15 MiB
pub const DEFAULT_MAX_FEED_ENTRIES: usize = 200;
pub const DEFAULT_FETCH_MAX_ATTEMPTS: u32 = 3;
pub const DEFAULT_FETCH_RETRY_INITIAL_SECONDS: u64 = 2;
pub const DEFAULT_FETCH_RETRY_MAX_SECONDS: u64 = 30;
pub const DEFAULT_MAX_REDIRECTS: usize = 5;
pub const DEFAULT_FETCH_HISTORY_RETENTION: i64 = 100;

/// Redacted wrapper for secret configuration strings so they can never be
/// accidentally printed in Debug or Display output.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigSource {
    Environment,
    Database,
    Default,
}

impl ConfigSource {
    pub fn label(self) -> &'static str {
        match self {
            ConfigSource::Environment => "Environment Variable",
            ConfigSource::Database => "Database (Web UI)",
            ConfigSource::Default => "Default",
        }
    }

    pub fn is_env_locked(self) -> bool {
        match self {
            ConfigSource::Environment => true,
            ConfigSource::Database | ConfigSource::Default => false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedSetting {
    pub key: &'static str,
    pub env_var: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub value: String,
    pub source: ConfigSource,
    pub editable_in_ui: bool,
    pub phase_status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeIngestionConfig {
    pub timezone: String,
    pub fetch_enabled: bool,
    pub fetch_interval_minutes: u32,
    pub fetch_min_interval_minutes: u32,
    pub fetch_concurrency: usize,
    pub extraction_concurrency: usize,
    pub connect_timeout_seconds: u64,
    pub feed_request_timeout_seconds: u64,
    pub article_request_timeout_seconds: u64,
    pub max_feed_response_bytes: usize,
    pub max_article_response_bytes: usize,
    pub max_feed_entries: usize,
    pub fetch_max_attempts: u32,
    pub fetch_retry_initial_seconds: u64,
    pub fetch_retry_max_seconds: u64,
    pub max_redirects: usize,
    pub fetch_history_retention: i64,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: String,
    pub host_source: ConfigSource,
    pub port: u16,
    pub port_source: ConfigSource,
    pub log_level: String,
    pub log_level_source: ConfigSource,
    pub data_dir: PathBuf,
    pub data_dir_source: ConfigSource,
    pub database_url: String,
    pub database_url_source: ConfigSource,
    pub timezone: String,
    pub timezone_source: ConfigSource,

    // Phase 2 Ingestion Configuration
    pub fetch_enabled: bool,
    pub fetch_enabled_source: ConfigSource,
    pub fetch_interval_minutes: u32,
    pub fetch_interval_minutes_source: ConfigSource,
    pub fetch_min_interval_minutes: u32,
    pub fetch_min_interval_minutes_source: ConfigSource,
    pub fetch_concurrency: usize,
    pub fetch_concurrency_source: ConfigSource,
    pub extraction_concurrency: usize,
    pub extraction_concurrency_source: ConfigSource,
    pub feed_request_timeout_seconds: u64,
    pub feed_request_timeout_seconds_source: ConfigSource,
    pub article_request_timeout_seconds: u64,
    pub article_request_timeout_seconds_source: ConfigSource,
    pub max_feed_response_bytes: usize,
    pub max_feed_response_bytes_source: ConfigSource,
    pub max_article_response_bytes: usize,
    pub max_article_response_bytes_source: ConfigSource,
    pub max_feed_entries: usize,
    pub max_feed_entries_source: ConfigSource,
    pub fetch_max_attempts: u32,
    pub fetch_max_attempts_source: ConfigSource,
    pub fetch_retry_initial_seconds: u64,
    pub fetch_retry_initial_seconds_source: ConfigSource,
    pub fetch_retry_max_seconds: u64,
    pub fetch_retry_max_seconds_source: ConfigSource,

    // Reserved Phase 4 & 6 Configuration
    pub ai_provider: Option<String>,
    pub ai_provider_source: ConfigSource,
    pub ai_base_url: Option<String>,
    pub ai_base_url_source: ConfigSource,
    pub ai_model: Option<String>,
    pub ai_model_source: ConfigSource,
    pub ai_api_key: Option<SecretString>,
    pub elevenreader_refresh_token: Option<SecretString>,
}

impl AppConfig {
    /// Load configuration from process environment variables.
    pub fn from_env() -> AppResult<Self> {
        let vars: HashMap<String, String> = std::env::vars().collect();
        Self::from_map(&vars)
    }

    /// Load configuration from an explicit map of environment variables.
    pub fn from_map(vars: &HashMap<String, String>) -> AppResult<Self> {
        let get_non_empty = |key: &str| -> Option<String> {
            vars.get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let (host, host_source) = match get_non_empty("QUIRE_HOST") {
            Some(v) => (v, ConfigSource::Environment),
            None => (DEFAULT_HOST.to_string(), ConfigSource::Default),
        };

        let (port, port_source) = match get_non_empty("QUIRE_PORT") {
            Some(raw) => {
                let parsed = raw.parse::<u16>().map_err(|_| {
                    AppError::Config(format!(
                        "Invalid QUIRE_PORT value '{raw}': expected integer 1..=65535"
                    ))
                })?;
                if parsed == 0 {
                    return Err(AppError::Config(
                        "Invalid QUIRE_PORT value '0': port must be between 1 and 65535"
                            .to_string(),
                    ));
                }
                (parsed, ConfigSource::Environment)
            }
            None => (DEFAULT_PORT, ConfigSource::Default),
        };

        let (log_level, log_level_source) = match get_non_empty("QUIRE_LOG_LEVEL") {
            Some(v) => (v, ConfigSource::Environment),
            None => (DEFAULT_LOG_LEVEL.to_string(), ConfigSource::Default),
        };

        let (data_dir_str, data_dir_source) = match get_non_empty("QUIRE_DATA_DIR") {
            Some(v) => (v, ConfigSource::Environment),
            None => (DEFAULT_DATA_DIR.to_string(), ConfigSource::Default),
        };
        let data_dir = PathBuf::from(&data_dir_str);

        let (database_url, database_url_source) = match get_non_empty("QUIRE_DATABASE_URL") {
            Some(v) => {
                if !v.starts_with("sqlite:") {
                    return Err(AppError::Config(
                        "QUIRE_DATABASE_URL must start with 'sqlite:'".to_string(),
                    ));
                }
                (v, ConfigSource::Environment)
            }
            None => {
                let db_path = data_dir.join("quire.db");
                (
                    format!("sqlite://{}", db_path.display()),
                    ConfigSource::Default,
                )
            }
        };

        let (timezone, timezone_source) = match get_non_empty("QUIRE_TIMEZONE") {
            Some(v) => {
                validate_timezone(&v).map_err(|e| AppError::Config(e.to_string()))?;
                (v, ConfigSource::Environment)
            }
            None => (DEFAULT_TIMEZONE.to_string(), ConfigSource::Default),
        };

        let (fetch_enabled, fetch_enabled_source) = match get_non_empty("QUIRE_FETCH_ENABLED") {
            Some(v) => (
                parse_bool_setting(&v, "QUIRE_FETCH_ENABLED")
                    .map_err(|e| AppError::Config(e.to_string()))?,
                ConfigSource::Environment,
            ),
            None => (DEFAULT_FETCH_ENABLED, ConfigSource::Default),
        };

        let (fetch_min_interval_minutes, fetch_min_interval_minutes_source) =
            match get_non_empty("QUIRE_FETCH_MIN_INTERVAL_MINUTES") {
                Some(v) => (
                    validate_min_interval_minutes(&v, "QUIRE_FETCH_MIN_INTERVAL_MINUTES")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_FETCH_MIN_INTERVAL_MINUTES, ConfigSource::Default),
            };

        let (fetch_interval_minutes, fetch_interval_minutes_source) =
            match get_non_empty("QUIRE_FETCH_INTERVAL_MINUTES") {
                Some(v) => (
                    validate_fetch_interval_minutes(
                        &v,
                        fetch_min_interval_minutes,
                        "QUIRE_FETCH_INTERVAL_MINUTES",
                    )
                    .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (
                    DEFAULT_FETCH_INTERVAL_MINUTES.max(fetch_min_interval_minutes),
                    ConfigSource::Default,
                ),
            };

        let (fetch_concurrency, fetch_concurrency_source) =
            match get_non_empty("QUIRE_FETCH_CONCURRENCY") {
                Some(v) => (
                    validate_concurrency(&v, "QUIRE_FETCH_CONCURRENCY")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_FETCH_CONCURRENCY, ConfigSource::Default),
            };

        let (extraction_concurrency, extraction_concurrency_source) =
            match get_non_empty("QUIRE_EXTRACTION_CONCURRENCY") {
                Some(v) => (
                    validate_concurrency(&v, "QUIRE_EXTRACTION_CONCURRENCY")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_EXTRACTION_CONCURRENCY, ConfigSource::Default),
            };

        let (feed_request_timeout_seconds, feed_request_timeout_seconds_source) =
            match get_non_empty("QUIRE_FEED_REQUEST_TIMEOUT_SECONDS") {
                Some(v) => (
                    validate_timeout_seconds(&v, "QUIRE_FEED_REQUEST_TIMEOUT_SECONDS")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_FEED_REQUEST_TIMEOUT_SECONDS, ConfigSource::Default),
            };

        let (article_request_timeout_seconds, article_request_timeout_seconds_source) =
            match get_non_empty("QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS") {
                Some(v) => (
                    validate_timeout_seconds(&v, "QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (
                    DEFAULT_ARTICLE_REQUEST_TIMEOUT_SECONDS,
                    ConfigSource::Default,
                ),
            };

        let (max_feed_response_bytes, max_feed_response_bytes_source) =
            match get_non_empty("QUIRE_MAX_FEED_RESPONSE_BYTES") {
                Some(v) => (
                    validate_max_response_bytes(&v, "QUIRE_MAX_FEED_RESPONSE_BYTES")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_MAX_FEED_RESPONSE_BYTES, ConfigSource::Default),
            };

        let (max_article_response_bytes, max_article_response_bytes_source) =
            match get_non_empty("QUIRE_MAX_ARTICLE_RESPONSE_BYTES") {
                Some(v) => (
                    validate_max_response_bytes(&v, "QUIRE_MAX_ARTICLE_RESPONSE_BYTES")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_MAX_ARTICLE_RESPONSE_BYTES, ConfigSource::Default),
            };

        let (max_feed_entries, max_feed_entries_source) =
            match get_non_empty("QUIRE_MAX_FEED_ENTRIES") {
                Some(v) => (
                    validate_max_entries(&v, "QUIRE_MAX_FEED_ENTRIES")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_MAX_FEED_ENTRIES, ConfigSource::Default),
            };

        let (fetch_max_attempts, fetch_max_attempts_source) =
            match get_non_empty("QUIRE_FETCH_MAX_ATTEMPTS") {
                Some(v) => (
                    validate_max_attempts(&v, "QUIRE_FETCH_MAX_ATTEMPTS")
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_FETCH_MAX_ATTEMPTS, ConfigSource::Default),
            };

        let (fetch_retry_initial_seconds, fetch_retry_initial_seconds_source) =
            match get_non_empty("QUIRE_FETCH_RETRY_INITIAL_SECONDS") {
                Some(v) => (
                    validate_retry_seconds(&v, "QUIRE_FETCH_RETRY_INITIAL_SECONDS", 1, 120)
                        .map_err(|e| AppError::Config(e.to_string()))?,
                    ConfigSource::Environment,
                ),
                None => (DEFAULT_FETCH_RETRY_INITIAL_SECONDS, ConfigSource::Default),
            };

        let (fetch_retry_max_seconds, fetch_retry_max_seconds_source) = match get_non_empty(
            "QUIRE_FETCH_RETRY_MAX_SECONDS",
        ) {
            Some(v) => {
                let parsed = validate_retry_seconds(&v, "QUIRE_FETCH_RETRY_MAX_SECONDS", 1, 600)
                    .map_err(|e| AppError::Config(e.to_string()))?;
                if parsed < fetch_retry_initial_seconds {
                    return Err(AppError::Config(
                            "QUIRE_FETCH_RETRY_MAX_SECONDS must be >= QUIRE_FETCH_RETRY_INITIAL_SECONDS".to_string(),
                        ));
                }
                (parsed, ConfigSource::Environment)
            }
            None => (
                DEFAULT_FETCH_RETRY_MAX_SECONDS.max(fetch_retry_initial_seconds),
                ConfigSource::Default,
            ),
        };

        let (ai_provider, ai_provider_source) = match get_non_empty("QUIRE_AI_PROVIDER") {
            Some(v) => (Some(v), ConfigSource::Environment),
            None => (None, ConfigSource::Default),
        };

        let (ai_base_url, ai_base_url_source) = match get_non_empty("QUIRE_AI_BASE_URL") {
            Some(v) => (Some(v), ConfigSource::Environment),
            None => (None, ConfigSource::Default),
        };

        let (ai_model, ai_model_source) = match get_non_empty("QUIRE_AI_MODEL") {
            Some(v) => (Some(v), ConfigSource::Environment),
            None => (None, ConfigSource::Default),
        };

        let ai_api_key = get_non_empty("QUIRE_AI_API_KEY").map(SecretString::new);
        let elevenreader_refresh_token =
            get_non_empty("QUIRE_ELEVENREADER_REFRESH_TOKEN").map(SecretString::new);

        Ok(Self {
            host,
            host_source,
            port,
            port_source,
            log_level,
            log_level_source,
            data_dir,
            data_dir_source,
            database_url,
            database_url_source,
            timezone,
            timezone_source,
            fetch_enabled,
            fetch_enabled_source,
            fetch_interval_minutes,
            fetch_interval_minutes_source,
            fetch_min_interval_minutes,
            fetch_min_interval_minutes_source,
            fetch_concurrency,
            fetch_concurrency_source,
            extraction_concurrency,
            extraction_concurrency_source,
            feed_request_timeout_seconds,
            feed_request_timeout_seconds_source,
            article_request_timeout_seconds,
            article_request_timeout_seconds_source,
            max_feed_response_bytes,
            max_feed_response_bytes_source,
            max_article_response_bytes,
            max_article_response_bytes_source,
            max_feed_entries,
            max_feed_entries_source,
            fetch_max_attempts,
            fetch_max_attempts_source,
            fetch_retry_initial_seconds,
            fetch_retry_initial_seconds_source,
            fetch_retry_max_seconds,
            fetch_retry_max_seconds_source,
            ai_provider,
            ai_provider_source,
            ai_base_url,
            ai_base_url_source,
            ai_model,
            ai_model_source,
            ai_api_key,
            elevenreader_refresh_token,
        })
    }

    pub fn bind_socket_addr(&self) -> AppResult<SocketAddr> {
        let addr_str = format!("{}:{}", self.host, self.port);
        addr_str
            .parse::<SocketAddr>()
            .map_err(|e| AppError::Config(format!("Invalid bind address '{addr_str}': {e}")))
    }

    /// Resolve effective runtime ingestion parameters by merging environment variables,
    /// SQLite `settings` rows, and compiled defaults.
    pub fn resolve_runtime_ingestion(
        &self,
        db_settings: &HashMap<String, String>,
    ) -> RuntimeIngestionConfig {
        let timezone = if self.timezone_source == ConfigSource::Environment {
            self.timezone.clone()
        } else {
            db_settings
                .get("timezone")
                .filter(|s| validate_timezone(s).is_ok())
                .cloned()
                .unwrap_or_else(|| self.timezone.clone())
        };

        let fetch_enabled = if self.fetch_enabled_source == ConfigSource::Environment {
            self.fetch_enabled
        } else {
            db_settings
                .get("fetch_enabled")
                .and_then(|s| parse_bool_setting(s, "fetch_enabled").ok())
                .unwrap_or(self.fetch_enabled)
        };

        let fetch_min_interval_minutes = self.fetch_min_interval_minutes;

        let fetch_interval_minutes =
            if self.fetch_interval_minutes_source == ConfigSource::Environment {
                self.fetch_interval_minutes
            } else {
                db_settings
                    .get("fetch_interval_minutes")
                    .and_then(|s| {
                        validate_fetch_interval_minutes(
                            s,
                            fetch_min_interval_minutes,
                            "fetch_interval_minutes",
                        )
                        .ok()
                    })
                    .unwrap_or(self.fetch_interval_minutes)
            };

        let fetch_concurrency = if self.fetch_concurrency_source == ConfigSource::Environment {
            self.fetch_concurrency
        } else {
            db_settings
                .get("fetch_concurrency")
                .and_then(|s| validate_concurrency(s, "fetch_concurrency").ok())
                .unwrap_or(self.fetch_concurrency)
        };

        let extraction_concurrency =
            if self.extraction_concurrency_source == ConfigSource::Environment {
                self.extraction_concurrency
            } else {
                db_settings
                    .get("extraction_concurrency")
                    .and_then(|s| validate_concurrency(s, "extraction_concurrency").ok())
                    .unwrap_or(self.extraction_concurrency)
            };

        let feed_request_timeout_seconds =
            if self.feed_request_timeout_seconds_source == ConfigSource::Environment {
                self.feed_request_timeout_seconds
            } else {
                db_settings
                    .get("feed_request_timeout_seconds")
                    .and_then(|s| validate_timeout_seconds(s, "feed_request_timeout_seconds").ok())
                    .unwrap_or(self.feed_request_timeout_seconds)
            };

        let article_request_timeout_seconds = if self.article_request_timeout_seconds_source
            == ConfigSource::Environment
        {
            self.article_request_timeout_seconds
        } else {
            db_settings
                .get("article_request_timeout_seconds")
                .and_then(|s| validate_timeout_seconds(s, "article_request_timeout_seconds").ok())
                .unwrap_or(self.article_request_timeout_seconds)
        };

        let max_feed_response_bytes =
            if self.max_feed_response_bytes_source == ConfigSource::Environment {
                self.max_feed_response_bytes
            } else {
                db_settings
                    .get("max_feed_response_bytes")
                    .and_then(|s| validate_max_response_bytes(s, "max_feed_response_bytes").ok())
                    .unwrap_or(self.max_feed_response_bytes)
            };

        let max_article_response_bytes =
            if self.max_article_response_bytes_source == ConfigSource::Environment {
                self.max_article_response_bytes
            } else {
                db_settings
                    .get("max_article_response_bytes")
                    .and_then(|s| validate_max_response_bytes(s, "max_article_response_bytes").ok())
                    .unwrap_or(self.max_article_response_bytes)
            };

        let max_feed_entries = if self.max_feed_entries_source == ConfigSource::Environment {
            self.max_feed_entries
        } else {
            db_settings
                .get("max_feed_entries")
                .and_then(|s| validate_max_entries(s, "max_feed_entries").ok())
                .unwrap_or(self.max_feed_entries)
        };

        let fetch_max_attempts = if self.fetch_max_attempts_source == ConfigSource::Environment {
            self.fetch_max_attempts
        } else {
            db_settings
                .get("fetch_max_attempts")
                .and_then(|s| validate_max_attempts(s, "fetch_max_attempts").ok())
                .unwrap_or(self.fetch_max_attempts)
        };

        RuntimeIngestionConfig {
            timezone,
            fetch_enabled,
            fetch_interval_minutes,
            fetch_min_interval_minutes,
            fetch_concurrency,
            extraction_concurrency,
            connect_timeout_seconds: DEFAULT_CONNECT_TIMEOUT_SECONDS,
            feed_request_timeout_seconds,
            article_request_timeout_seconds,
            max_feed_response_bytes,
            max_article_response_bytes,
            max_feed_entries,
            fetch_max_attempts,
            fetch_retry_initial_seconds: self.fetch_retry_initial_seconds,
            fetch_retry_max_seconds: self.fetch_retry_max_seconds,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            fetch_history_retention: DEFAULT_FETCH_HISTORY_RETENTION,
        }
    }

    /// Merge nonsecret settings from the SQLite `settings` table while respecting
    /// environment variable precedence (environment variables always win).
    pub fn resolve_settings(&self, db_settings: &HashMap<String, String>) -> Vec<ResolvedSetting> {
        let rt = self.resolve_runtime_ingestion(db_settings);

        let resolve_src = |env_source: ConfigSource, db_key: &str| -> (ConfigSource, bool) {
            if env_source == ConfigSource::Environment {
                (ConfigSource::Environment, false)
            } else if db_settings
                .get(db_key)
                .is_some_and(|v| !v.trim().is_empty())
            {
                (ConfigSource::Database, true)
            } else {
                (ConfigSource::Default, true)
            }
        };

        let resolve_opt = |env_val: &Option<String>,
                           env_source: ConfigSource,
                           db_key: &str,
                           default_display: &str,
                           editable_when_unlocked: bool|
         -> (String, ConfigSource, bool) {
            if env_source == ConfigSource::Environment {
                (
                    env_val.clone().unwrap_or_default(),
                    ConfigSource::Environment,
                    false,
                )
            } else if let Some(db_val) = db_settings.get(db_key).filter(|v| !v.trim().is_empty()) {
                (
                    db_val.clone(),
                    ConfigSource::Database,
                    editable_when_unlocked,
                )
            } else {
                (
                    default_display.to_string(),
                    ConfigSource::Default,
                    editable_when_unlocked,
                )
            }
        };

        let (tz_src, tz_edit) = resolve_src(self.timezone_source, "timezone");
        let (fe_src, fe_edit) = resolve_src(self.fetch_enabled_source, "fetch_enabled");
        let (fi_src, fi_edit) =
            resolve_src(self.fetch_interval_minutes_source, "fetch_interval_minutes");
        let (fc_src, fc_edit) = resolve_src(self.fetch_concurrency_source, "fetch_concurrency");
        let (ec_src, ec_edit) =
            resolve_src(self.extraction_concurrency_source, "extraction_concurrency");
        let (ft_src, ft_edit) = resolve_src(
            self.feed_request_timeout_seconds_source,
            "feed_request_timeout_seconds",
        );
        let (at_src, at_edit) = resolve_src(
            self.article_request_timeout_seconds_source,
            "article_request_timeout_seconds",
        );
        let (fb_src, fb_edit) = resolve_src(
            self.max_feed_response_bytes_source,
            "max_feed_response_bytes",
        );
        let (ab_src, ab_edit) = resolve_src(
            self.max_article_response_bytes_source,
            "max_article_response_bytes",
        );
        let (me_src, me_edit) = resolve_src(self.max_feed_entries_source, "max_feed_entries");
        let (ma_src, ma_edit) = resolve_src(self.fetch_max_attempts_source, "fetch_max_attempts");

        let (ai_prov_val, ai_prov_src, ai_prov_edit) = resolve_opt(
            &self.ai_provider,
            self.ai_provider_source,
            "ai_provider",
            "",
            true,
        );
        let (ai_url_val, ai_url_src, ai_url_edit) = resolve_opt(
            &self.ai_base_url,
            self.ai_base_url_source,
            "ai_base_url",
            "",
            true,
        );
        let (ai_mod_val, ai_mod_src, ai_mod_edit) =
            resolve_opt(&self.ai_model, self.ai_model_source, "ai_model", "", true);

        vec![
            ResolvedSetting {
                key: "host",
                env_var: "QUIRE_HOST",
                label: "Bind Host",
                description: "Network interface address the HTTP server listens on.",
                value: self.host.clone(),
                source: self.host_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "port",
                env_var: "QUIRE_PORT",
                label: "HTTP Port",
                description: "TCP port for the web interface and health endpoint.",
                value: self.port.to_string(),
                source: self.port_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "log_level",
                env_var: "QUIRE_LOG_LEVEL",
                label: "Log Level",
                description: "Structured logging filter (error, warn, info, debug, trace).",
                value: self.log_level.clone(),
                source: self.log_level_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "data_dir",
                env_var: "QUIRE_DATA_DIR",
                label: "Data Directory",
                description: "Persistent directory for SQLite and generated publications.",
                value: self.data_dir.display().to_string(),
                source: self.data_dir_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "database_url",
                env_var: "QUIRE_DATABASE_URL",
                label: "Database URL",
                description: "SQLite connection string.",
                value: self.database_url.clone(),
                source: self.database_url_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "timezone",
                env_var: "QUIRE_TIMEZONE",
                label: "Application Timezone",
                description: "IANA timezone for edition scheduling and display timestamps.",
                value: rt.timezone,
                source: tz_src,
                editable_in_ui: tz_edit,
                phase_status: "Active (Phase 1)",
            },
            ResolvedSetting {
                key: "fetch_enabled",
                env_var: "QUIRE_FETCH_ENABLED",
                label: "Automatic Fetching Enabled",
                description: "Enable or pause scheduled background RSS/Atom feed polling.",
                value: rt.fetch_enabled.to_string(),
                source: fe_src,
                editable_in_ui: fe_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "fetch_interval_minutes",
                env_var: "QUIRE_FETCH_INTERVAL_MINUTES",
                label: "Default Fetch Interval (Minutes)",
                description: "Default interval in minutes between automatic feed checks (5..=10080).",
                value: rt.fetch_interval_minutes.to_string(),
                source: fi_src,
                editable_in_ui: fi_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "fetch_min_interval_minutes",
                env_var: "QUIRE_FETCH_MIN_INTERVAL_MINUTES",
                label: "Minimum Fetch Interval (Minutes)",
                description: "Minimum allowed polling interval in minutes.",
                value: rt.fetch_min_interval_minutes.to_string(),
                source: self.fetch_min_interval_minutes_source,
                editable_in_ui: false,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "fetch_concurrency",
                env_var: "QUIRE_FETCH_CONCURRENCY",
                label: "Max Concurrent Feed Requests",
                description: "Maximum simultaneous feed HTTP fetches (1..=32).",
                value: rt.fetch_concurrency.to_string(),
                source: fc_src,
                editable_in_ui: fc_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "extraction_concurrency",
                env_var: "QUIRE_EXTRACTION_CONCURRENCY",
                label: "Max Concurrent Article Extractions",
                description: "Maximum simultaneous article full-text extractions (1..=32).",
                value: rt.extraction_concurrency.to_string(),
                source: ec_src,
                editable_in_ui: ec_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "feed_request_timeout_seconds",
                env_var: "QUIRE_FEED_REQUEST_TIMEOUT_SECONDS",
                label: "Feed Request Timeout (Seconds)",
                description: "Maximum duration in seconds for a single feed HTTP request (5..=300).",
                value: rt.feed_request_timeout_seconds.to_string(),
                source: ft_src,
                editable_in_ui: ft_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "article_request_timeout_seconds",
                env_var: "QUIRE_ARTICLE_REQUEST_TIMEOUT_SECONDS",
                label: "Article Request Timeout (Seconds)",
                description: "Maximum duration in seconds for an article HTML download (5..=300).",
                value: rt.article_request_timeout_seconds.to_string(),
                source: at_src,
                editable_in_ui: at_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "max_feed_response_bytes",
                env_var: "QUIRE_MAX_FEED_RESPONSE_BYTES",
                label: "Max Feed Response Size (Bytes)",
                description: "Streaming size limit in bytes for RSS/Atom documents.",
                value: rt.max_feed_response_bytes.to_string(),
                source: fb_src,
                editable_in_ui: fb_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "max_article_response_bytes",
                env_var: "QUIRE_MAX_ARTICLE_RESPONSE_BYTES",
                label: "Max Article Response Size (Bytes)",
                description: "Streaming size limit in bytes for article HTML pages.",
                value: rt.max_article_response_bytes.to_string(),
                source: ab_src,
                editable_in_ui: ab_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "max_feed_entries",
                env_var: "QUIRE_MAX_FEED_ENTRIES",
                label: "Max Entries Per Feed Fetch",
                description: "Maximum entries processed per feed poll (1..=5000).",
                value: rt.max_feed_entries.to_string(),
                source: me_src,
                editable_in_ui: me_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "fetch_max_attempts",
                env_var: "QUIRE_FETCH_MAX_ATTEMPTS",
                label: "Max HTTP Fetch Attempts",
                description: "Bounded retry attempts for transient network or HTTP failures (1..=10).",
                value: rt.fetch_max_attempts.to_string(),
                source: ma_src,
                editable_in_ui: ma_edit,
                phase_status: "Active (Phase 2)",
            },
            ResolvedSetting {
                key: "ai_provider",
                env_var: "QUIRE_AI_PROVIDER",
                label: "AI Provider",
                description: "LLM provider identifier (for example: ollama, openai, openrouter).",
                value: ai_prov_val,
                source: ai_prov_src,
                editable_in_ui: ai_prov_edit,
                phase_status: "Reserved (Phase 4)",
            },
            ResolvedSetting {
                key: "ai_base_url",
                env_var: "QUIRE_AI_BASE_URL",
                label: "AI Base URL",
                description: "Base URL for OpenAI-compatible or local Ollama inference endpoints.",
                value: ai_url_val,
                source: ai_url_src,
                editable_in_ui: ai_url_edit,
                phase_status: "Reserved (Phase 4)",
            },
            ResolvedSetting {
                key: "ai_model",
                env_var: "QUIRE_AI_MODEL",
                label: "AI Model",
                description: "Model name used for article scoring and editorial summaries.",
                value: ai_mod_val,
                source: ai_mod_src,
                editable_in_ui: ai_mod_edit,
                phase_status: "Reserved (Phase 4)",
            },
            ResolvedSetting {
                key: "ai_api_key",
                env_var: "QUIRE_AI_API_KEY",
                label: "AI API Key",
                description: "Secret API credential for hosted LLM providers (environment only; never displayed).",
                value: if self.ai_api_key.is_some() {
                    "Configured (redacted)".to_string()
                } else {
                    "Not configured".to_string()
                },
                source: if self.ai_api_key.is_some() {
                    ConfigSource::Environment
                } else {
                    ConfigSource::Default
                },
                editable_in_ui: false,
                phase_status: "Reserved (Phase 4)",
            },
            ResolvedSetting {
                key: "elevenreader_refresh_token",
                env_var: "QUIRE_ELEVENREADER_REFRESH_TOKEN",
                label: "ElevenReader Refresh Token",
                description: "Secret Firebase refresh token for experimental ElevenReader delivery (environment only; never displayed).",
                value: if self.elevenreader_refresh_token.is_some() {
                    "Configured (redacted)".to_string()
                } else {
                    "Not configured".to_string()
                },
                source: if self.elevenreader_refresh_token.is_some() {
                    ConfigSource::Environment
                } else {
                    ConfigSource::Default
                },
                editable_in_ui: false,
                phase_status: "Reserved (Phase 6)",
            },
        ]
    }
}

pub fn validate_timezone(tz: &str) -> AppResult<()> {
    let trimmed = tz.trim();
    if trimmed.is_empty() {
        return Err(AppError::Validation(
            "Timezone cannot be empty.".to_string(),
        ));
    }
    trimmed.parse::<Tz>().map_err(|_| {
        AppError::Validation(format!(
            "Invalid IANA timezone '{trimmed}' (example: America/Los_Angeles or UTC)."
        ))
    })?;
    Ok(())
}

pub fn parse_bool_setting(raw: &str, label: &str) -> AppResult<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(AppError::Validation(format!(
            "Invalid boolean value '{other}' for {label}: expected true or false."
        ))),
    }
}

pub fn validate_min_interval_minutes(raw: &str, label: &str) -> AppResult<u32> {
    let val = raw.trim().parse::<u32>().map_err(|_| {
        AppError::Validation(format!("{label} must be a positive integer in minutes."))
    })?;
    if !(1..=DEFAULT_FETCH_MAX_INTERVAL_MINUTES).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 1 and {DEFAULT_FETCH_MAX_INTERVAL_MINUTES} minutes."
        )));
    }
    Ok(val)
}

pub fn validate_fetch_interval_minutes(
    raw: &str,
    min_interval: u32,
    label: &str,
) -> AppResult<u32> {
    let val = raw.trim().parse::<u32>().map_err(|_| {
        AppError::Validation(format!("{label} must be a positive integer in minutes."))
    })?;
    if val < min_interval || val > DEFAULT_FETCH_MAX_INTERVAL_MINUTES {
        return Err(AppError::Validation(format!(
            "{label} must be between {min_interval} and {DEFAULT_FETCH_MAX_INTERVAL_MINUTES} minutes."
        )));
    }
    Ok(val)
}

pub fn validate_concurrency(raw: &str, label: &str) -> AppResult<usize> {
    let val = raw.trim().parse::<usize>().map_err(|_| {
        AppError::Validation(format!("{label} must be an integer between 1 and 32."))
    })?;
    if !(1..=32).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 1 and 32."
        )));
    }
    Ok(val)
}

pub fn validate_timeout_seconds(raw: &str, label: &str) -> AppResult<u64> {
    let val = raw.trim().parse::<u64>().map_err(|_| {
        AppError::Validation(format!(
            "{label} must be an integer between 5 and 300 seconds."
        ))
    })?;
    if !(5..=300).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 5 and 300 seconds."
        )));
    }
    Ok(val)
}

pub fn validate_max_response_bytes(raw: &str, label: &str) -> AppResult<usize> {
    let val = raw.trim().parse::<usize>().map_err(|_| {
        AppError::Validation(format!(
            "{label} must be an integer between 65536 (64 KiB) and 104857600 (100 MiB)."
        ))
    })?;
    if !(65_536..=104_857_600).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 65536 (64 KiB) and 104857600 (100 MiB)."
        )));
    }
    Ok(val)
}

pub fn validate_max_entries(raw: &str, label: &str) -> AppResult<usize> {
    let val = raw.trim().parse::<usize>().map_err(|_| {
        AppError::Validation(format!("{label} must be an integer between 1 and 5000."))
    })?;
    if !(1..=5000).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 1 and 5000."
        )));
    }
    Ok(val)
}

pub fn validate_max_attempts(raw: &str, label: &str) -> AppResult<u32> {
    let val = raw.trim().parse::<u32>().map_err(|_| {
        AppError::Validation(format!("{label} must be an integer between 1 and 10."))
    })?;
    if !(1..=10).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between 1 and 10."
        )));
    }
    Ok(val)
}

pub fn validate_retry_seconds(raw: &str, label: &str, min: u64, max: u64) -> AppResult<u64> {
    let val = raw.trim().parse::<u64>().map_err(|_| {
        AppError::Validation(format!(
            "{label} must be an integer between {min} and {max} seconds."
        ))
    })?;
    if !(min..=max).contains(&val) {
        return Err(AppError::Validation(format!(
            "{label} must be between {min} and {max} seconds."
        )));
    }
    Ok(val)
}

/// Format an ISO-8601 UTC timestamp string into the configured IANA timezone
/// for readable display in the browser UI.
pub fn format_timestamp_for_tz(utc_iso: &str, tz_name: &str) -> String {
    let Ok(tz) = tz_name.parse::<Tz>() else {
        return utc_iso.to_string();
    };
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(utc_iso) {
        let converted = dt.with_timezone(&tz);
        return converted.format("%Y-%m-%d %H:%M:%S %Z").to_string();
    }
    utc_iso.to_string()
}
