use crate::{
    config::{GrpcSource, secret},
    store::program_id,
};
use anyhow::{Context, Result};
use futures::StreamExt;
use std::{collections::HashMap, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SlotStatus, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdateAccount,
    subscribe_update::UpdateOneof,
};

pub enum Event {
    Connected {
        source: usize,
    },
    Disconnected {
        source: usize,
    },
    Account {
        source: usize,
        update: Box<SubscribeUpdateAccount>,
    },
    ConfirmedSlot {
        source: usize,
        slot: u64,
    },
}

pub async fn run(
    source_id: usize,
    source: GrpcSource,
    idle_timeout_secs: u64,
    events: mpsc::Sender<Event>,
    stop: CancellationToken,
) {
    let source_url = secret(&source.url_env).expect("gRPC source URL was validated");
    loop {
        let result = tokio::select! {
            _ = stop.cancelled() => break,
            result = session(source_id, &source, &source_url, idle_timeout_secs, &events) => result,
        };
        if result.is_err() {
            tracing::warn!(source = source_url, "Yellowstone source disconnected");
            if events
                .send(Event::Disconnected { source: source_id })
                .await
                .is_err()
            {
                break;
            }
        }
        tokio::select! {
            _ = stop.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}

pub(crate) async fn connect(url: &str, token: Option<&str>) -> Result<GeyserGrpcClient> {
    let mut builder = GeyserGrpcClient::build_from_shared(url.to_owned())?
        .x_token(token)?
        .connect_timeout(Duration::from_secs(10));
    if url.starts_with("https://") {
        builder =
            builder.tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

async fn session(
    source_id: usize,
    source: &GrpcSource,
    source_url: &str,
    idle_timeout_secs: u64,
    events: &mpsc::Sender<Event>,
) -> Result<()> {
    let token = source.token_env.as_deref().map(secret).transpose()?;
    let mut grpc = connect(source_url, token.as_deref()).await?;
    let (requests, receiver) = mpsc::channel(8);
    requests.send(subscribe_request()).await?;
    let mut stream = grpc
        .geyser
        .subscribe(tokio_stream::wrappers::ReceiverStream::new(receiver))
        .await?
        .into_inner();
    events.send(Event::Connected { source: source_id }).await?;
    loop {
        let update = tokio::time::timeout(Duration::from_secs(idle_timeout_secs), stream.next())
            .await?
            .context("source stream closed")??;
        match update.update_oneof {
            Some(UpdateOneof::Ping(_)) => send_ping(&requests)?,
            Some(UpdateOneof::Account(update)) => {
                events
                    .send(Event::Account {
                        source: source_id,
                        update: Box::new(update),
                    })
                    .await?;
            }
            Some(UpdateOneof::Slot(update))
                if update.status == SlotStatus::SlotConfirmed as i32 =>
            {
                events
                    .send(Event::ConfirmedSlot {
                        source: source_id,
                        slot: update.slot,
                    })
                    .await?;
            }
            _ => {}
        }
    }
}

pub(crate) fn subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        accounts: HashMap::from([(
            "alt-owner".into(),
            SubscribeRequestFilterAccounts {
                owner: vec![program_id()],
                ..Default::default()
            },
        )]),
        slots: HashMap::from([(
            "confirmed".into(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(false),
            },
        )]),
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    }
}

pub(crate) fn send_ping(requests: &mpsc::Sender<SubscribeRequest>) -> Result<()> {
    requests.try_send(SubscribeRequest {
        ping: Some(SubscribeRequestPing { id: 1 }),
        ..Default::default()
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_subscription_filters_by_alt_owner() {
        let request = subscribe_request();
        assert_eq!(request.accounts["alt-owner"].owner, vec![program_id()]);
        assert_eq!(request.accounts.len(), 1);
        assert_eq!(request.slots.len(), 1);
        assert_eq!(request.commitment, Some(CommitmentLevel::Confirmed as i32));
    }
}
