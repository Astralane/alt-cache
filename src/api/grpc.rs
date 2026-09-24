use crate::{
    proto::{
        AltAccount, SnapshotChunk, SnapshotRequest,
        alt_snapshot_server::{AltSnapshot, AltSnapshotServer},
    },
    store::Store,
};
use anyhow::{Result, bail};
use futures::Stream;
use prost::Message;
use std::{net::SocketAddr, pin::Pin, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, transport::Server};

const TARGET_CHUNK_BYTES: usize = 2 * 1024 * 1024;
const CHUNK_CHANNEL_CAPACITY: usize = 2;

struct SnapshotService {
    store: Arc<Store>,
}

#[tonic::async_trait]
impl AltSnapshot for SnapshotService {
    type StreamSnapshotStream =
        Pin<Box<dyn Stream<Item = Result<SnapshotChunk, Status>> + Send + 'static>>;

    async fn stream_snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::StreamSnapshotStream>, Status> {
        let snapshot = self
            .store
            .read_snapshot()
            .map_err(|_| Status::unavailable("cache not ready; retry after recovery"))?;
        let (sender, receiver) = mpsc::channel(CHUNK_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            let account_count = snapshot.accounts.len();
            let mut chunk = SnapshotChunk {
                confirmed_slot: snapshot.slot,
                total_accounts: account_count as u64,
                accounts: Vec::new(),
            };
            for (key, account) in snapshot.accounts.iter() {
                chunk.accounts.push(AltAccount {
                    pubkey: key.to_vec(),
                    data: account.value().data.clone(),
                });
                if chunk.encoded_len() >= TARGET_CHUNK_BYTES {
                    match sender.send(Ok(chunk)).await {
                        Ok(()) => {
                            chunk = SnapshotChunk {
                                confirmed_slot: snapshot.slot,
                                total_accounts: account_count as u64,
                                accounts: Vec::new(),
                            };
                        }
                        Err(_) => return,
                    }
                }
            }
            if !chunk.accounts.is_empty() || account_count == 0 {
                let _ = sender.send(Ok(chunk)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }
}

pub async fn serve(addr: SocketAddr, store: Arc<Store>, stop: CancellationToken) -> Result<()> {
    let service = AltSnapshotServer::new(SnapshotService { store });
    tokio::select! {
        result = Server::builder()
            .http2_adaptive_window(Some(true))
            .add_service(service)
            .serve(addr) => {
            result?;
            bail!("snapshot gRPC server stopped")
        }
        _ = stop.cancelled() => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Account, StateUpdater};
    use futures::StreamExt;
    use std::collections::BTreeMap;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    #[tokio::test]
    async fn stream_keeps_its_captured_snapshot_after_invalidation() {
        let store = Arc::new(Store::new());
        let mut updater = StateUpdater::new(store.clone());
        let key = [7; 32];
        let account = Account {
            value: SubscribeUpdateAccountInfo {
                pubkey: key.to_vec(),
                data: vec![1, 2, 3],
                ..Default::default()
            },
        };
        updater.install(
            "test".into(),
            42,
            BTreeMap::from([(key, Arc::new(account))]),
        );
        updater.finish_recovery();
        let service = SnapshotService {
            store: store.clone(),
        };
        let mut stream = service
            .stream_snapshot(Request::new(SnapshotRequest {}))
            .await
            .unwrap()
            .into_inner();

        updater.invalidate();
        let chunk = stream.next().await.unwrap().unwrap();

        assert_eq!(chunk.confirmed_slot, 42);
        assert_eq!(chunk.total_accounts, 1);
        assert_eq!(chunk.accounts[0].pubkey, key);
        assert_eq!(chunk.accounts[0].data, [1, 2, 3]);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn unready_store_rejects_a_new_stream() {
        let service = SnapshotService {
            store: Arc::new(Store::new()),
        };
        let error = service
            .stream_snapshot(Request::new(SnapshotRequest {}))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::Unavailable);
    }
}
