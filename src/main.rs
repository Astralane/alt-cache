use anyhow::{Result, anyhow};
use astralane_alt_cache::{
    api,
    config::{Config, secret},
    feed, logging,
    store::Store,
};
use signal_hook::consts::{SIGINT, SIGTERM};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel},
    },
    thread::JoinHandle,
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
    let alerts = logging::Alerts::start(
        config
            .alert_webhook_env
            .as_deref()
            .map(secret)
            .transpose()?,
    )?;
    install_panic_hook(alerts.clone());

    let config = Arc::new(config);
    let store = Arc::new(Store::new(
        config.stream_capacity,
        Duration::from_secs(config.stale_after_secs),
    ));
    let stop = CancellationToken::new();
    let (exits, exit_events) = sync_channel(config.sources.len() + 3);
    let mut services = Vec::with_capacity(config.sources.len() + 3);

    {
        let config = config.clone();
        let store = store.clone();
        let alerts = alerts.clone();
        let stop = stop.clone();
        services.push(spawn_service("alt-feed", exits.clone(), async move {
            feed::run(config, store, alerts, stop).await;
            Ok(())
        })?);
    }
    for (index, source) in config.sources.iter().cloned().enumerate() {
        let store = store.clone();
        let stop = stop.clone();
        services.push(spawn_service(
            &format!("alt-monitor-{index}"),
            exits.clone(),
            async move {
                feed::monitor(source, store, stop).await;
                Ok(())
            },
        )?);
    }
    {
        let store = store.clone();
        let stop = stop.clone();
        let addr = config.http_addr;
        services.push(spawn_service("alt-json-rpc", exits.clone(), async move {
            api::serve_json(addr, store, stop).await
        })?);
    }
    {
        let store = store.clone();
        let stop = stop.clone();
        let addr = config.grpc_addr;
        services.push(spawn_service("alt-grpc", exits.clone(), async move {
            api::serve_grpc(addr, store, stop).await
        })?);
    }
    drop(exits);

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        service_threads = services.len(),
        "ALT cache started"
    );
    let result = wait_for_shutdown(exit_events, &stop)?;
    store.invalidate();
    stop.cancel();
    for service in services {
        if service.join().is_err() && result.is_ok() {
            return Err(anyhow!("service thread panicked"));
        }
    }
    result
}

fn spawn_service<F>(
    name: &str,
    exits: SyncSender<(String, Result<()>)>,
    future: F,
) -> Result<JoinHandle<()>>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    let name = name.to_owned();
    let thread_name = name.clone();
    Ok(std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .max_blocking_threads(MAX_BLOCKING_THREADS_PER_SERVICE)
                .enable_all()
                .build()
                .map_err(anyhow::Error::from)
                .and_then(|runtime| runtime.block_on(future));
            let _ = exits.send((name, result));
        })?)
}

fn wait_for_shutdown(
    exits: Receiver<(String, Result<()>)>,
    stop: &CancellationToken,
) -> Result<Result<()>> {
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(SIGINT, shutdown.clone())?;
    signal_hook::flag::register(SIGTERM, shutdown.clone())?;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(Ok(()));
        }
        match exits.recv_timeout(Duration::from_millis(100)) {
            Ok((name, Ok(()))) if !stop.is_cancelled() => {
                return Ok(Err(anyhow!("{name} exited unexpectedly")));
            }
            Ok((name, Err(error))) => return Ok(Err(error.context(name))),
            Ok(_) => {}
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return Ok(Err(anyhow!("all services exited unexpectedly")));
            }
        }
    }
}

fn install_panic_hook(alerts: logging::Alerts) {
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
        alerts.send(format!("ALT cache panic at {location}: {message}"));
        previous(info);
    }));
}
