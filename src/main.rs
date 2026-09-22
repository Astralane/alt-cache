use anyhow::Result;
use astralane_alt_cache::{
    api,
    config::{Config, secret},
    logging,
    store::Store,
    updater, yellowstone,
};
use signal_hook::consts::{SIGINT, SIGTERM};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

const MAX_BLOCKING_THREADS_PER_SERVICE: usize = 1;

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".into());
    let config: Config = toml::from_str(&std::fs::read_to_string(path)?)?;
    config.validate()?;
    let _guards = logging::init(&config.logging)?;
    logging::init_alerts(
        config
            .alert_webhook_env
            .as_deref()
            .map(secret)
            .transpose()?,
    )?;
    install_panic_hook();

    let config = Arc::new(config);
    let store = Arc::new(Store::new());
    let stop = CancellationToken::new();
    let (source_events, events) = tokio::sync::mpsc::channel(config.stream_capacity);

    {
        let config = config.clone();
        let store = store.clone();
        let stop = stop.clone();
        spawn_service(
            "alt-state-updater",
            stop.clone(),
            updater::run(config, store, events, stop),
        )?;
    }
    for (index, source) in config.grpc_sources.iter().cloned().enumerate() {
        let stop = stop.clone();
        let stale_after_secs = config.stale_after_secs;
        let events = source_events.clone();
        spawn_service(
            &format!("alt-yellowstone-{index}"),
            stop.clone(),
            async move {
                yellowstone::run(index, source, stale_after_secs, events, stop).await;
                Ok(())
            },
        )?;
    }
    drop(source_events);
    {
        let store = store.clone();
        let stop = stop.clone();
        let addr = config.http_addr;
        spawn_service(
            "alt-json-rpc",
            stop.clone(),
            api::serve_json(addr, store, stop),
        )?;
    }

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        service_threads = config.grpc_sources.len() + 2,
        "ALT cache started"
    );
    wait_for_shutdown()?;
    stop.cancel();
    Ok(())
}

fn spawn_service<F>(name: &str, stop: CancellationToken, future: F) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let name = name.to_owned();
    let thread_name = name.clone();
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(MAX_BLOCKING_THREADS_PER_SERVICE)
                .enable_all()
                .build()
                .map_err(anyhow::Error::from)
                .and_then(|runtime| runtime.block_on(future));
            if !stop.is_cancelled() {
                match result {
                    Ok(()) => panic!("{name} exited unexpectedly"),
                    Err(error) => panic!("{name} failed: {error:#}"),
                }
            }
        })?;
    Ok(())
}

fn wait_for_shutdown() -> Result<()> {
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGINT, shutdown.clone())?;
    signal_hook::flag::register(SIGTERM, shutdown.clone())?;
    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let location = info
            .location()
            .map(|value| format!("{}:{}", value.file(), value.line()))
            .unwrap_or_else(|| "unknown".into());
        let message = info
            .payload()
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        tracing::error!(%location, %message, "ALT cache panicked");
        previous(info);
        std::process::exit(1);
    }));
}
