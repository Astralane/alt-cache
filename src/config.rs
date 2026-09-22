use anyhow::{Result, ensure};
use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub bootstrap_rpc: BootstrapRpc,
    pub grpc_sources: Vec<GrpcSource>,
    #[serde(default = "yellowstone_idle_timeout_seconds")]
    pub yellowstone_idle_timeout_secs: u64,
    #[serde(default = "full_refresh_interval_seconds")]
    pub full_refresh_interval_secs: u64,
    #[serde(default = "capacity")]
    pub stream_capacity: usize,
    #[serde(default)]
    pub logging: Logging,
    pub alert_webhook_env: Option<String>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapRpc {
    pub url_env: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrpcSource {
    pub url_env: String,
    pub token_env: Option<String>,
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
impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.grpc_sources.is_empty(),
            "at least one gRPC source is required"
        );
        ensure!(self.stream_capacity > 0, "stream_capacity must be positive");
        ensure!(
            self.yellowstone_idle_timeout_secs > 0,
            "yellowstone_idle_timeout_secs must be positive"
        );
        ensure!(
            self.full_refresh_interval_secs > 0,
            "full_refresh_interval_secs must be positive"
        );
        ensure!(self.logging.file.max_days > 0, "max_days must be positive");
        secret(&self.bootstrap_rpc.url_env)?;
        let mut urls = std::collections::HashSet::new();
        for source in &self.grpc_sources {
            ensure!(
                urls.insert(secret(&source.url_env)?),
                "gRPC source URLs must be unique"
            );
            if let Some(name) = &source.token_env {
                secret(name)?;
            }
        }
        if let Some(name) = &self.alert_webhook_env {
            secret(name)?;
        }
        Ok(())
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
        assert_eq!(c.bootstrap_rpc.url_env, "ALT_BOOTSTRAP_RPC_URL");
        assert_eq!(c.grpc_sources.len(), 2);
        assert_eq!(c.stream_capacity, 4096);
    }
    #[test]
    fn unknown_config_is_rejected() {
        let text = format!("unexpected = 1\n{}", include_str!("../config.example.toml"));
        assert!(toml::from_str::<Config>(&text).is_err());
    }
}
