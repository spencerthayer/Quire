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
    /// Useful for deterministic unit testing without mutating global process state.
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
                validate_timezone(&v)?;
                (v, ConfigSource::Environment)
            }
            None => (DEFAULT_TIMEZONE.to_string(), ConfigSource::Default),
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

    /// Merge nonsecret settings from the SQLite `settings` table while respecting
    /// environment variable precedence (environment variables always win).
    pub fn resolve_settings(&self, db_settings: &HashMap<String, String>) -> Vec<ResolvedSetting> {
        let resolve_str = |env_val: &str,
                           env_source: ConfigSource,
                           db_key: &str,
                           editable_when_unlocked: bool|
         -> (String, ConfigSource, bool) {
            if env_source == ConfigSource::Environment {
                (env_val.to_string(), ConfigSource::Environment, false)
            } else if let Some(db_val) = db_settings.get(db_key).filter(|v| !v.trim().is_empty()) {
                (
                    db_val.clone(),
                    ConfigSource::Database,
                    editable_when_unlocked,
                )
            } else {
                (
                    env_val.to_string(),
                    ConfigSource::Default,
                    editable_when_unlocked,
                )
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

        let (tz_val, tz_src, tz_edit) =
            resolve_str(&self.timezone, self.timezone_source, "timezone", true);
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
                value: tz_val,
                source: tz_src,
                editable_in_ui: tz_edit,
                phase_status: "Active (Phase 1)",
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
