use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub snapshot_grpc_addr: SocketAddr,
    pub bootstrap_rpc: BootstrapRpc,
    pub grpc_sources: Vec<GrpcSource>,
    #[serde(default = "yellowstone_idle_timeout_seconds")]
    pub yellowstone_idle_timeout_secs: u64,
    #[serde(default = "full_refresh_interval_seconds")]
    pub full_refresh_interval_secs: u64,
    #[serde(default = "capacity")]
    pub stream_capacity: usize,
    #[serde(default = "max_snapshot_page_size")]
    pub max_snapshot_page_size: usize,
    #[serde(default)]
    pub logging: Logging,
    #[serde(default)]
    pub alerts: Alerts,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapRpc {
    pub url: Option<String>,
    pub url_env: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcSource {
    pub url: Option<String>,
    pub url_env: Option<String>,
    pub token: Option<String>,
    pub token_env: Option<String>,
}

impl BootstrapRpc {
    pub fn resolve_url(&self) -> Result<String> {
        resolve_required(
            "bootstrap RPC URL",
            self.url.as_deref(),
            self.url_env.as_deref(),
        )
    }
}

impl GrpcSource {
    pub fn resolve_url(&self) -> Result<String> {
        resolve_required(
            "gRPC source URL",
            self.url.as_deref(),
            self.url_env.as_deref(),
        )
    }

    pub fn resolve_token(&self) -> Result<Option<String>> {
        resolve_optional(
            "gRPC source token",
            self.token.as_deref(),
            self.token_env.as_deref(),
        )
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alerts {
    pub slack: Option<WebhookConfig>,
    pub discord: Option<WebhookConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    pub url: Option<String>,
    pub url_env: Option<String>,
}

impl WebhookConfig {
    pub fn resolve_url(&self, name: &str) -> Result<String> {
        resolve_required(name, self.url.as_deref(), self.url_env.as_deref())
    }
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Logging {
    #[serde(default)]
    pub stdout: Stdout,
    #[serde(default)]
    pub file: File,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Stdout {
    pub enabled: bool,
    pub level: String,
    pub enable_ansi: bool,
}
impl Default for Stdout {
    fn default() -> Self {
        Self {
            enabled: true,
            level: "info".into(),
            enable_ansi: true,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct File {
    pub enabled: bool,
    pub level: String,
    pub max_days: usize,
    pub dir: PathBuf,
}
impl Default for File {
    fn default() -> Self {
        Self {
            enabled: false,
            level: "info".into(),
            max_days: 30,
            dir: "logs".into(),
        }
    }
}
fn yellowstone_idle_timeout_seconds() -> u64 {
    15
}
fn full_refresh_interval_seconds() -> u64 {
    3600
}
fn capacity() -> usize {
    4096
}
fn max_snapshot_page_size() -> usize {
    100_000
}
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.http_addr != self.snapshot_grpc_addr,
            "http_addr and snapshot_grpc_addr must be different"
        );
        ensure!(
            !self.grpc_sources.is_empty(),
            "at least one gRPC source is required"
        );
        ensure!(self.stream_capacity > 0, "stream_capacity must be positive");
        ensure!(
            self.max_snapshot_page_size > 0,
            "max_snapshot_page_size must be positive"
        );
        ensure!(
            self.yellowstone_idle_timeout_secs > 0,
            "yellowstone_idle_timeout_secs must be positive"
        );
        ensure!(
            self.full_refresh_interval_secs > 0,
            "full_refresh_interval_secs must be positive"
        );
        ensure!(self.logging.file.max_days > 0, "max_days must be positive");
        self.bootstrap_rpc.resolve_url()?;
        let mut urls = std::collections::HashSet::new();
        for source in &self.grpc_sources {
            ensure!(
                urls.insert(source.resolve_url()?),
                "gRPC source URLs must be unique"
            );
            source.resolve_token()?;
        }
        if let Some(webhook) = &self.alerts.slack {
            webhook.resolve_url("Slack webhook URL")?;
        }
        if let Some(webhook) = &self.alerts.discord {
            webhook.resolve_url("Discord webhook URL")?;
        }
        Ok(())
    }
}

fn resolve_required(label: &str, direct: Option<&str>, env: Option<&str>) -> Result<String> {
    match (direct, env) {
        (Some(_), Some(_)) => anyhow::bail!(
            "{label} must use either a direct value or an environment variable, not both"
        ),
        (Some(value), None) => {
            ensure!(!value.is_empty(), "{label} must not be empty");
            Ok(value.to_owned())
        }
        (None, Some(name)) => secret(name),
        (None, None) => anyhow::bail!("{label} is required"),
    }
}

fn resolve_optional(
    label: &str,
    direct: Option<&str>,
    env: Option<&str>,
) -> Result<Option<String>> {
    match (direct, env) {
        (Some(_), Some(_)) => anyhow::bail!(
            "{label} must use either a direct value or an environment variable, not both"
        ),
        (Some(value), None) => {
            ensure!(!value.is_empty(), "{label} must not be empty");
            Ok(Some(value.to_owned()))
        }
        (None, Some(name)) => secret(name).map(Some),
        (None, None) => Ok(None),
    }
}
pub fn secret(name: &str) -> Result<String> {
    let value =
        std::env::var(name).map_err(|_| anyhow::anyhow!("missing environment variable {name}"))?;
    ensure!(!value.is_empty(), "empty environment variable {name}");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn example_config_parses() {
        let c: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        assert_eq!(
            c.bootstrap_rpc.url_env.as_deref(),
            Some("ALT_BOOTSTRAP_RPC_URL")
        );
        assert_eq!(c.grpc_sources.len(), 2);
        assert_eq!(c.stream_capacity, 4096);
        assert_eq!(c.max_snapshot_page_size, 100_000);
    }

    #[test]
    fn direct_source_values_are_supported() {
        let source: GrpcSource = toml::from_str(
            r#"
                url = "https://yellowstone.example.com"
                token = "token"
            "#,
        )
        .unwrap();
        assert_eq!(
            source.resolve_url().unwrap(),
            "https://yellowstone.example.com"
        );
        assert_eq!(source.resolve_token().unwrap().as_deref(), Some("token"));
    }

    #[test]
    fn mixed_direct_and_environment_values_are_rejected() {
        let source: GrpcSource = toml::from_str(
            r#"
                url = "https://yellowstone.example.com"
                url_env = "YELLOWSTONE_URL"
            "#,
        )
        .unwrap();
        assert!(source.resolve_url().is_err());
    }
    #[test]
    fn unknown_config_is_rejected() {
        let text = format!("unexpected = 1\n{}", include_str!("../config.example.toml"));
        assert!(toml::from_str::<Config>(&text).is_err());
    }
}
