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

pub fn init_alerts(webhook: Option<String>) -> Result<()> {
    let sender = if let Some(webhook) = webhook {
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
                    let sent = runtime.block_on(async {
                        client
                            .post(&webhook)
                            .json(&serde_json::json!({"text":text}))
                            .send()
                            .await?
                            .error_for_status()
                    });
                    if sent.is_err() {
                        tracing::warn!("alert delivery failed");
                    }
                }
            })?;
        Some(sender)
    } else {
        None
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
