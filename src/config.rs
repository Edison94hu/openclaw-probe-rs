use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Config models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub name: String,
    pub port: u16,
    pub profile: Option<i64>,
    #[serde(default = "default_openclaw_home")]
    pub openclaw_home: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_openclaw_home() -> String {
    "~/.openclaw".into()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeConfig {
    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_retry_delay")]
    pub retry_delay_seconds: u64,
}
impl Default for ProbeConfig {
    fn default() -> Self {
        Self {
            interval_seconds: 30,
            timeout_seconds: 5,
            failure_threshold: 2,
            retry_delay_seconds: 10,
        }
    }
}
fn default_interval() -> u64 {
    30
}
fn default_timeout() -> u64 {
    5
}
fn default_failure_threshold() -> u32 {
    2
}
fn default_retry_delay() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_max_restarts")]
    pub max_restarts_per_window: u32,
    #[serde(default = "default_window_minutes")]
    pub window_minutes: u32,
    #[serde(default = "default_wait_after_restart")]
    pub wait_after_restart_seconds: u64,
}
impl Default for RestartConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_restarts_per_window: 3,
            window_minutes: 10,
            wait_after_restart_seconds: 20,
        }
    }
}
fn default_max_restarts() -> u32 {
    3
}
fn default_window_minutes() -> u32 {
    10
}
fn default_wait_after_restart() -> u64 {
    20
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    #[serde(default)]
    pub feishu_webhook: String,
    #[serde(default)]
    pub enabled: bool,
}
impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            feishu_webhook: String::new(),
            enabled: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenScanConfig {
    #[serde(default = "default_token_interval")]
    pub interval_minutes: u64,
}
impl Default for TokenScanConfig {
    fn default() -> Self {
        Self {
            interval_minutes: 5,
        }
    }
}
fn default_token_interval() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8000,
        }
    }
}
fn default_host() -> String {
    "0.0.0.0".into()
}
fn default_port() -> u16 {
    8000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_agent_name")]
    pub name: String,
    #[serde(default)]
    pub site: String,
    #[serde(default)]
    pub api_token: String,
}
impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            name: hostname::get()
                .map(|h| h.to_string_lossy().into_owned())
                .unwrap_or_default(),
            site: String::new(),
            api_token: String::new(),
        }
    }
}
fn default_agent_name() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().into_owned())
        .unwrap_or_default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationsConfig {
    #[serde(default = "default_backup_dir")]
    pub backup_dir: String,
    #[serde(default = "default_max_backups")]
    pub max_backups: usize,
}
impl Default for OperationsConfig {
    fn default() -> Self {
        Self {
            backup_dir: "~/openclaw-agent-backups".into(),
            max_backups: 10,
        }
    }
}
fn default_backup_dir() -> String {
    "~/openclaw-agent-backups".into()
}
fn default_max_backups() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub instances: Vec<InstanceConfig>,
    #[serde(default)]
    pub probe: ProbeConfig,
    #[serde(default)]
    pub restart: RestartConfig,
    #[serde(default)]
    pub alert: AlertConfig,
    #[serde(default)]
    pub token_scan: TokenScanConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub agent: AgentConfig,
    #[serde(default)]
    pub operations: OperationsConfig,
}

// ---------------------------------------------------------------------------
// Config manager with hot-reload
// ---------------------------------------------------------------------------

pub struct ConfigManager {
    inner: RwLock<ConfigInner>,
    path: PathBuf,
}

struct ConfigInner {
    config: AppConfig,
    mtime: Option<std::time::SystemTime>,
}

impl ConfigManager {
    pub fn new(path: PathBuf) -> Self {
        let (config, mtime) = load_from_disk(&path);
        Self {
            inner: RwLock::new(ConfigInner { config, mtime }),
            path,
        }
    }

    /// Get current config, reloading from disk when the file has changed.
    pub async fn get(&self) -> AppConfig {
        let current_mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();

        {
            let inner = self.inner.read().await;
            if inner.mtime == current_mtime {
                return inner.config.clone();
            }
        }

        let mut inner = self.inner.write().await;
        // Double-check after acquiring write lock.
        let current_mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if inner.mtime != current_mtime {
            let (config, mtime) = load_from_disk(&self.path);
            inner.config = config;
            inner.mtime = mtime;
        }
        inner.config.clone()
    }

    /// Overwrite config on disk and update in-memory copy.
    pub async fn update(&self, new_config: AppConfig) -> anyhow::Result<AppConfig> {
        let json = serde_json::to_string_pretty(&new_config)?;
        tokio::fs::write(&self.path, &json).await?;
        let mtime = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        let mut inner = self.inner.write().await;
        inner.config = new_config.clone();
        inner.mtime = mtime;
        Ok(new_config)
    }
}

fn load_from_disk(path: &Path) -> (AppConfig, Option<std::time::SystemTime>) {
    match std::fs::read_to_string(path) {
        Ok(data) => {
            let config: AppConfig = serde_json::from_str(&data).unwrap_or_default();
            let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
            (config, mtime)
        }
        Err(_) => (AppConfig::default(), None),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn expand_home(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs_home() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var("HOME").ok().map(PathBuf::from)
}

/// Shared application state threaded through Axum handlers.
pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub config: ConfigManager,
    pub db: sqlx::SqlitePool,
    pub instance_states: RwLock<std::collections::HashMap<String, InstanceState>>,
    pub started_at: std::time::Instant,
    pub started_at_epoch: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceState {
    pub online: Option<bool>,
    pub last_check: Option<f64>,
    pub last_response_time_ms: Option<f64>,
    pub last_status_code: Option<u16>,
    pub consecutive_failures: u32,
    pub uptime_since: Option<f64>,
}

impl Default for InstanceState {
    fn default() -> Self {
        Self {
            online: None,
            last_check: None,
            last_response_time_ms: None,
            last_status_code: None,
            consecutive_failures: 0,
            uptime_since: None,
        }
    }
}

pub fn now_epoch() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

pub fn now_iso() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}
