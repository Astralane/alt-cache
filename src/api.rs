use crate::{
    proto::{
        self,
        alt_cache_server::{AltCache, AltCacheServer},
        update::Kind,
    },
    store::{Event, Health, KeyedUiAccount, Store, parse_key},
};
use anyhow::{Result as AnyResult, bail};
use futures::{Stream, StreamExt};
use jsonrpsee::{
    core::RpcResult,
    proc_macros::rpc,
    server::{ServerBuilder, ServerConfig},
    types::ErrorObjectOwned,
};
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::UiAccount;
use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

fn invalid_params(message: &'static str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32602, message, None::<()>)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotParams {
    #[serde(default = "snapshot_limit")]
    limit: usize,
    after: Option<String>,
    epoch: Option<String>,
    sequence: Option<u64>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupTableResponse {
    epoch: String,
    sequence: u64,
    processed_slot: u64,
    account: Option<UiAccount>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    epoch: String,
    sequence: u64,
    processed_slot: u64,
    accounts: Vec<KeyedUiAccount>,
    next_cursor: Option<String>,
}

fn snapshot_limit() -> usize {
    1_000
}

#[rpc(server)]
trait AltJsonRpc {
    #[method(name = "getHealth")]
    fn health(&self) -> RpcResult<Health>;

    #[method(name = "getLookupTable")]
    fn lookup_table(&self, pubkey: String) -> RpcResult<LookupTableResponse>;

    #[method(name = "getSnapshot")]
    fn snapshot(&self, options: SnapshotParams) -> RpcResult<SnapshotResponse>;
}

struct JsonRpc {
    store: Arc<Store>,
}

impl AltJsonRpcServer for JsonRpc {
    fn health(&self) -> RpcResult<Health> {
        Ok(self.store.health())
    }

    fn lookup_table(&self, pubkey: String) -> RpcResult<LookupTableResponse> {
        let key = parse_key(&pubkey).map_err(|_| invalid_params("expected one base58 pubkey"))?;
        let (epoch, sequence, slot, account) = self.store.get(&key).map_err(|_| {
            ErrorObjectOwned::owned(-32001, "cache not ready; retry after recovery", None::<()>)
        })?;
        let account = account
            .map(|account| account.to_keyed_ui_account().map(|account| account.account))
            .transpose()
            .map_err(|_| ErrorObjectOwned::owned(-32603, "account encoding failed", None::<()>))?;
        Ok(LookupTableResponse {
            epoch,
            sequence,
            processed_slot: slot,
            account,
        })
    }

    fn snapshot(&self, options: SnapshotParams) -> RpcResult<SnapshotResponse> {
        if options.limit == 0 || options.limit > 1_000 {
            return Err(invalid_params("snapshot limit must be between 1 and 1000"));
        }
        let after = options
            .after
            .as_deref()
            .map(parse_key)
            .transpose()
            .map_err(|_| invalid_params("invalid snapshot cursor"))?;
        if after.is_some() && (options.epoch.is_none() || options.sequence.is_none()) {
            return Err(invalid_params(
                "later snapshot pages require epoch and sequence",
            ));
        }
        let page = self
            .store
            .snapshot_page(after, options.limit)
            .map_err(|_| {
                ErrorObjectOwned::owned(-32001, "cache not ready; retry after recovery", None::<()>)
            })?;
        if options
            .epoch
            .as_deref()
            .is_some_and(|value| value != page.epoch)
            || options.sequence.is_some_and(|value| value != page.sequence)
        {
            return Err(ErrorObjectOwned::owned(
                -32002,
                "snapshot changed; restart pagination",
                None::<()>,
            ));
        }
        let accounts = page
            .accounts
            .iter()
            .map(|account| account.to_keyed_ui_account())
            .collect::<AnyResult<Vec<_>>>()
            .map_err(|_| ErrorObjectOwned::owned(-32603, "account encoding failed", None::<()>))?;
        Ok(SnapshotResponse {
            epoch: page.epoch,
            sequence: page.sequence,
            processed_slot: page.slot,
            accounts,
            next_cursor: page.next.map(|key| bs58::encode(key).into_string()),
        })
    }
}

fn json_rpc(store: Arc<Store>) -> jsonrpsee::RpcModule<JsonRpc> {
    JsonRpc { store }.into_rpc()
}

pub async fn serve_json(
    addr: SocketAddr,
    store: Arc<Store>,
    stop: CancellationToken,
) -> AnyResult<()> {
    let config = ServerConfig::builder()
        .http_only()
        .max_connections(16)
        .max_request_body_size(64 * 1024)
        .build();
    let server = ServerBuilder::with_config(config).build(addr).await?;
    let handle = server.start(json_rpc(store));
    tokio::select! {
        _ = stop.cancelled() => {
            let _ = handle.stop();
            handle.stopped().await;
            Ok(())
        }
        _ = handle.clone().stopped() => bail!("JSON-RPC server stopped"),
    }
}

pub struct Grpc {
    store: Arc<Store>,
    subscribers: Arc<tokio::sync::Semaphore>,
}
impl Grpc {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            subscribers: Arc::new(tokio::sync::Semaphore::new(16)),
        }
    }
}
#[tonic::async_trait]
impl AltCache for Grpc {
    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        let key = request
            .into_inner()
            .pubkey
            .try_into()
            .map_err(|_| Status::invalid_argument("pubkey must be 32 bytes"))?;
        let (epoch, sequence, processed_slot, account) = self
            .store
            .get(&key)
            .map_err(|_| Status::unavailable("cache not ready"))?;
        Ok(Response::new(proto::GetResponse {
            epoch,
            sequence,
            processed_slot,
            account_slot: account.as_ref().map(|account| account.slot),
            account: account.map(|account| account.value.clone()),
        }))
    }
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<proto::Update, Status>> + Send>>;
    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let updates = request.into_inner().updates;
        let permit = self
            .subscribers
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many subscribers"))?;
        let snapshot = self
            .store
            .snapshot()
            .map_err(|_| Status::unavailable("cache not ready"))?;
        let epoch = snapshot.epoch;
        let sequence = snapshot.sequence;
        let processed_slot = snapshot.slot;
        let snapshot_epoch = epoch.clone();
        let initial = futures::stream::iter(snapshot.accounts.into_iter().map(move |a| {
            Ok(proto::Update {
                epoch: snapshot_epoch.clone(),
                sequence,
                kind: Kind::SnapshotAccount as i32,
                account: Some(a.value.clone()),
                pubkey: vec![],
                processed_slot: a.slot,
            })
        }));
        let end = futures::stream::iter([Ok(proto::Update {
            epoch: epoch.clone(),
            sequence,
            kind: Kind::SnapshotEnd as i32,
            account: None,
            pubkey: vec![],
            processed_slot,
        })]);
        let receiver = snapshot.receiver;
        let store = self.store.clone();
        let tail = futures::stream::unfold((receiver, false), move |(mut receiver, finished)| {
            let epoch = epoch.clone();
            let store = store.clone();
            async move {
                if finished || !updates {
                    return None;
                }
                let received = tokio::select! {
                    event = receiver.recv() => Some(event),
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => None,
                };
                let item = match received {
                    Some(Ok(Event::Write {
                        sequence,
                        key,
                        account,
                        slot,
                    })) => Ok(proto::Update {
                        epoch,
                        sequence,
                        kind: if account.is_some() {
                            Kind::Upsert
                        } else {
                            Kind::Delete
                        } as i32,
                        account: account.map(|account| account.value.clone()),
                        pubkey: key.to_vec(),
                        processed_slot: slot,
                    }),
                    Some(Ok(Event::Reset)) => Err(Status::aborted(
                        "source epoch reset; replace local snapshot",
                    )),
                    Some(Err(_)) => Err(Status::out_of_range("update gap; replace local snapshot")),
                    None if !store.health().ready => Err(Status::unavailable(
                        "source is stale; replace local snapshot",
                    )),
                    None => return Some((None, (receiver, false))),
                };
                let finished = item.is_err();
                Some((Some(item), (receiver, finished)))
            }
        })
        .filter_map(|item| async move { item });
        Ok(Response::new(Box::pin(initial.chain(end).chain(tail).map(
            move |item| {
                let _ = &permit;
                item
            },
        ))))
    }
}

pub async fn serve_grpc(
    addr: SocketAddr,
    store: Arc<Store>,
    stop: CancellationToken,
) -> AnyResult<()> {
    let (health, health_service) = tonic_health::server::health_reporter();
    let shutdown = stop.clone();
    let server = tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(AltCacheServer::new(Grpc::new(store.clone())))
        .serve_with_shutdown(addr, shutdown.cancelled_owned());
    tokio::pin!(server);
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut server => return Ok(result?),
            _ = interval.tick() => {
                let status = if store.health().ready {
                    tonic_health::ServingStatus::Serving
                } else {
                    tonic_health::ServingStatus::NotServing
                };
                health
                    .set_service_status("astralane.alt.v1.AltCache", status)
                    .await;
                health.set_service_status("", status).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Account;
    use serde_json::Value;
    use std::{collections::BTreeMap, time::Duration};
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;
    fn store() -> Arc<Store> {
        let s = Arc::new(Store::new(2, Duration::from_secs(10)));
        s.install("test".into(), 1, BTreeMap::new());
        s.progress(3, 2);
        s
    }
    #[tokio::test]
    async fn grpc_snapshot_then_update_and_reset() {
        let s = store();
        let api = Grpc::new(s.clone());
        let mut stream = api
            .subscribe(Request::new(proto::SubscribeRequest { updates: true }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            stream.next().await.unwrap().unwrap().kind,
            Kind::SnapshotEnd as i32
        );
        s.apply(
            [1; 32],
            4,
            1,
            Some(Account {
                slot: 4,
                value: SubscribeUpdateAccountInfo {
                    pubkey: vec![1; 32],
                    lamports: 1,
                    owner: vec![2; 32],
                    executable: false,
                    rent_epoch: 0,
                    data: vec![3, 4],
                    write_version: 1,
                    txn_signature: None,
                },
            }),
        );
        let update = stream.next().await.unwrap().unwrap();
        assert_eq!(update.kind, Kind::Upsert as i32);
        assert_eq!(update.account.unwrap().data, vec![3, 4]);
        s.invalidate();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Aborted
        );
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn grpc_slow_client_gets_resync_error() {
        let s = store();
        let api = Grpc::new(s.clone());
        let mut stream = api
            .subscribe(Request::new(proto::SubscribeRequest { updates: true }))
            .await
            .unwrap()
            .into_inner();
        stream.next().await.unwrap().unwrap();
        for i in 0..4 {
            s.apply([1; 32], 4, i, None);
        }
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::OutOfRange
        );
    }
    #[tokio::test]
    async fn json_health_tracks_invalidation() {
        let s = store();
        let rpc = json_rpc(s.clone());
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}"#;
        let (response, _) = rpc.raw_json_request(request, 1).await.unwrap();
        let response: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["result"]["ready"], true);
        s.invalidate();
        let (response, _) = rpc.raw_json_request(request, 1).await.unwrap();
        let response: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["result"]["ready"], false);
    }
    #[tokio::test]
    async fn json_snapshot_requires_identity_after_first_page() {
        let s = store();
        for value in [1, 2] {
            let key = [value; 32];
            s.apply(
                key,
                4,
                1,
                Some(Account {
                    slot: 4,
                    value: SubscribeUpdateAccountInfo {
                        pubkey: key.to_vec(),
                        lamports: 1,
                        owner: vec![0; 32],
                        executable: false,
                        rent_epoch: 0,
                        data: Vec::new(),
                        write_version: 1,
                        txn_signature: None,
                    },
                }),
            );
        }
        let rpc = json_rpc(s);
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"getSnapshot","params":[{"limit":1}]}"#;
        let (response, _) = rpc.raw_json_request(request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(
            body["result"]["accounts"][0]["account"]["data"][1],
            "base64+zstd"
        );
        let cursor = body["result"]["nextCursor"].as_str().unwrap();
        let request = format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"getSnapshot","params":[{{"limit":1,"after":"{cursor}"}}]}}"#
        );
        let (response, _) = rpc.raw_json_request(&request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(body["error"]["code"], -32602);
    }
}
