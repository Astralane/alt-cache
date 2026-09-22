use crate::config::Logging;
use anyhow::{Result, ensure};
use std::{
    sync::{
        OnceLock,
        mpsc::{SyncSender, sync_channel},
    },
    time::Duration,
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init(config: &Logging) -> Result<Vec<WorkerGuard>> {
    let mut guards = Vec::new();
    let stdout = if config.stdout.enabled {
        let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
        guards.push(guard);
        Some(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_ansi(config.stdout.enable_ansi)
                .with_writer(writer)
                .with_filter(EnvFilter::try_new(&config.stdout.level)?),
        )
    } else {
        None
    };
    let file = if config.file.enabled {
        let writer = tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("alt-cache")
            .max_log_files(config.file.max_days)
            .build(&config.file.dir)?;
        let (writer, guard) = tracing_appender::non_blocking(writer);
        guards.push(guard);
        Some(
            tracing_subscriber::fmt::layer()
                .json()
                .with_ansi(false)
                .with_writer(writer)
                .with_filter(EnvFilter::try_new(&config.file.level)?),
        )
    } else {
        None
    };
    tracing_subscriber::registry()
        .with(stdout)
        .with(file)
        .try_init()?;
    Ok(guards)
}

static ALERTS: OnceLock<Option<SyncSender<String>>> = OnceLock::new();

enum AlertTarget {
    Slack(String),
    Discord(String),
}

impl AlertTarget {
    fn name(&self) -> &'static str {
        match self {
            Self::Slack(_) => "slack",
            Self::Discord(_) => "discord",
        }
    }

    fn url(&self) -> &str {
        match self {
            Self::Slack(url) | Self::Discord(url) => url,
        }
    }

    fn payload(&self, text: &str) -> serde_json::Value {
        match self {
            Self::Slack(_) => serde_json::json!({"text": text}),
            Self::Discord(_) => serde_json::json!({"content": text}),
        }
    }
}

pub fn init_alerts(slack: Option<String>, discord: Option<String>) -> Result<()> {
    let mut targets = Vec::new();
    if let Some(url) = slack {
        targets.push(AlertTarget::Slack(url));
    }
    if let Some(url) = discord {
        targets.push(AlertTarget::Discord(url));
    }
    let sender = if targets.is_empty() {
        None
    } else {
        let (sender, receiver) = sync_channel::<String>(32);
        std::thread::Builder::new()
            .name("alt-alerts".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("alert runtime");
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()
                    .unwrap();
                let mut last = None;
                while let Ok(text) = receiver.recv() {
                    if last.is_some_and(|at: std::time::Instant| {
                        at.elapsed() < Duration::from_secs(60)
                    }) {
                        continue;
                    }
                    last = Some(std::time::Instant::now());
                    for target in &targets {
                        let sent = runtime.block_on(async {
                            client
                                .post(target.url())
                                .json(&target.payload(&text))
                                .send()
                                .await?
                                .error_for_status()
                        });
                        if sent.is_err() {
                            tracing::warn!(target = target.name(), "alert delivery failed");
                        }
                    }
                }
            })?;
        Some(sender)
    };
    ensure!(ALERTS.set(sender).is_ok(), "alerts already initialized");
    Ok(())
}

pub fn alert(text: impl Into<String>) {
    let Some(sender) = ALERTS.get().and_then(Option::as_ref) else {
        return;
    };
    if sender.try_send(text.into()).is_err() {
        tracing::warn!("alert queue full");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_targets_use_native_payloads() {
        assert_eq!(
            AlertTarget::Slack(String::new()).payload("message"),
            serde_json::json!({"text": "message"})
        );
        assert_eq!(
            AlertTarget::Discord(String::new()).payload("message"),
            serde_json::json!({"content": "message"})
        );
    }
}
