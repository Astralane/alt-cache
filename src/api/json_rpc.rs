use crate::store::{Account, Health, KeyedUiAccount, Store, parse_key, program_id};
use anyhow::{Context, Result as AnyResult, bail};
use jsonrpsee::{
    core::RpcResult,
    proc_macros::rpc,
    server::{ServerBuilder, ServerConfig},
    types::ErrorObjectOwned,
};
use serde::{Deserialize, Serialize};
use solana_account_decoder_client_types::{UiAccountEncoding, UiDataSliceConfig};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;

fn invalid_params(message: &'static str) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32602, message, None::<()>)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProgramAccountsConfig {
    commitment: Option<String>,
    min_context_slot: Option<u64>,
    with_context: Option<bool>,
    encoding: Option<UiAccountEncoding>,
    data_slice: Option<UiDataSliceConfig>,
    filters: Option<Vec<serde_json::Value>>,
    #[serde(rename = "sortResults")]
    _sort_results: Option<bool>,
    limit: Option<usize>,
    pagination_key: Option<String>,
    changed_since_slot: Option<u64>,
}

#[derive(Clone, Serialize)]
pub struct RpcContext {
    slot: u64,
}

#[derive(Clone, Serialize)]
pub struct ContextResponse<T> {
    context: RpcContext,
    value: T,
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum ProgramAccountsResponse {
    Accounts(Vec<KeyedUiAccount>),
    Context(ContextResponse<Vec<KeyedUiAccount>>),
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgramAccountsPage {
    accounts: Vec<KeyedUiAccount>,
    pagination_key: Option<String>,
}

#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum ProgramAccountsV2Response {
    Page(ProgramAccountsPage),
    Context(ContextResponse<ProgramAccountsPage>),
}

#[rpc(server)]
trait AltJsonRpc {
    #[method(name = "getHealth")]
    fn health(&self) -> RpcResult<Health>;

    #[method(name = "getProgramAccounts")]
    fn program_accounts(
        &self,
        address: String,
        config: Option<ProgramAccountsConfig>,
    ) -> RpcResult<ProgramAccountsResponse>;

    #[method(name = "getProgramAccountsV2")]
    fn program_accounts_v2(
        &self,
        address: String,
        config: Option<ProgramAccountsConfig>,
    ) -> RpcResult<ProgramAccountsV2Response>;
}

struct JsonRpc {
    store: Arc<Store>,
}

impl AltJsonRpcServer for JsonRpc {
    fn health(&self) -> RpcResult<Health> {
        Ok(self.store.health())
    }

    fn program_accounts(
        &self,
        address: String,
        config: Option<ProgramAccountsConfig>,
    ) -> RpcResult<ProgramAccountsResponse> {
        validate_program(&address)?;
        let config = config.unwrap_or_default();
        validate_config(&config, false)?;
        let snapshot = self.store.read_snapshot().map_err(|_| unavailable())?;
        validate_min_context_slot(&config, snapshot.slot)?;
        let accounts = encode_accounts(snapshot.accounts.values(), &config)?;
        if config.with_context.unwrap_or(false) {
            Ok(ProgramAccountsResponse::Context(ContextResponse {
                context: RpcContext {
                    slot: snapshot.slot,
                },
                value: accounts,
            }))
        } else {
            Ok(ProgramAccountsResponse::Accounts(accounts))
        }
    }

    fn program_accounts_v2(
        &self,
        address: String,
        config: Option<ProgramAccountsConfig>,
    ) -> RpcResult<ProgramAccountsV2Response> {
        validate_program(&address)?;
        let config = config.unwrap_or_default();
        validate_config(&config, true)?;
        let limit = config.limit.unwrap_or(1_000);
        let cursor = config
            .pagination_key
            .as_deref()
            .map(parse_cursor)
            .transpose()
            .map_err(|_| invalid_params("invalid paginationKey"))?;
        let (snapshot_slot, after) = cursor
            .map(|(slot, key)| (Some(slot), Some(key)))
            .unwrap_or((None, None));
        let page = self
            .store
            .snapshot_page(snapshot_slot, after, limit)
            .map_err(|_| {
                if snapshot_slot.is_some() {
                    ErrorObjectOwned::owned(
                        -32002,
                        "snapshot expired; restart pagination",
                        None::<()>,
                    )
                } else {
                    unavailable()
                }
            })?;
        validate_min_context_slot(&config, page.slot)?;
        let value = ProgramAccountsPage {
            accounts: encode_accounts(page.accounts.iter(), &config)?,
            pagination_key: page.next.map(|key| format_cursor(page.slot, key)),
        };
        if config.with_context.unwrap_or(false) {
            Ok(ProgramAccountsV2Response::Context(ContextResponse {
                context: RpcContext { slot: page.slot },
                value,
            }))
        } else {
            Ok(ProgramAccountsV2Response::Page(value))
        }
    }
}

fn unavailable() -> ErrorObjectOwned {
    ErrorObjectOwned::owned(-32001, "cache not ready; retry after recovery", None::<()>)
}

fn validate_program(address: &str) -> RpcResult<()> {
    if address != program_id() {
        return Err(invalid_params("only the ALT program is available"));
    }
    Ok(())
}

fn validate_config(config: &ProgramAccountsConfig, paginated: bool) -> RpcResult<()> {
    if config
        .commitment
        .as_deref()
        .is_some_and(|value| value != "confirmed")
    {
        return Err(invalid_params("only confirmed commitment is available"));
    }
    if config
        .filters
        .as_ref()
        .is_some_and(|filters| !filters.is_empty())
    {
        return Err(invalid_params("account filters are unsupported"));
    }
    if config.changed_since_slot.is_some() {
        return Err(invalid_params("changedSinceSlot is unsupported"));
    }
    if !paginated && (config.limit.is_some() || config.pagination_key.is_some()) {
        return Err(invalid_params(
            "limit and paginationKey require getProgramAccountsV2",
        ));
    }
    if paginated && !matches!(config.limit.unwrap_or(1_000), 1..=10_000) {
        return Err(invalid_params("limit must be between 1 and 10000"));
    }
    if matches!(config.encoding, Some(UiAccountEncoding::JsonParsed)) {
        return Err(invalid_params("jsonParsed encoding is unsupported"));
    }
    Ok(())
}

fn validate_min_context_slot(config: &ProgramAccountsConfig, slot: u64) -> RpcResult<()> {
    if config
        .min_context_slot
        .is_some_and(|minimum| slot < minimum)
    {
        return Err(ErrorObjectOwned::owned(
            -32016,
            "minimum context slot has not been reached",
            None::<()>,
        ));
    }
    Ok(())
}

fn encode_accounts<'a>(
    accounts: impl Iterator<Item = &'a Arc<Account>>,
    config: &ProgramAccountsConfig,
) -> RpcResult<Vec<KeyedUiAccount>> {
    let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);
    accounts
        .map(|account| account.to_keyed_ui_account_with_config(encoding, config.data_slice))
        .collect::<AnyResult<Vec<_>>>()
        .map_err(|_| ErrorObjectOwned::owned(-32603, "account encoding failed", None::<()>))
}

fn parse_cursor(cursor: &str) -> AnyResult<(u64, [u8; 32])> {
    let (slot, key) = cursor
        .split_once(':')
        .context("pagination cursor separator missing")?;
    Ok((slot.parse()?, parse_key(key)?))
}

fn format_cursor(slot: u64, key: [u8; 32]) -> String {
    format!("{slot}:{}", bs58::encode(key).into_string())
}

fn json_rpc(store: Arc<Store>) -> jsonrpsee::RpcModule<JsonRpc> {
    JsonRpc { store }.into_rpc()
}

pub async fn serve(addr: SocketAddr, store: Arc<Store>, stop: CancellationToken) -> AnyResult<()> {
    let config = ServerConfig::builder()
        .http_only()
        .max_connections(16)
        .max_request_body_size(64 * 1024)
        .max_response_body_size(u32::MAX)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StateUpdater;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use yellowstone_grpc_proto::geyser::SubscribeUpdateAccountInfo;

    fn store() -> (Arc<Store>, StateUpdater) {
        let store = Arc::new(Store::new());
        let mut updater = StateUpdater::new(store.clone());
        updater.install("test".into(), 1, BTreeMap::new());
        updater.finish_recovery();
        (store, updater)
    }

    #[tokio::test]
    async fn health_tracks_invalidation() {
        let (store, mut updater) = store();
        let rpc = json_rpc(store);
        let request = r#"{"jsonrpc":"2.0","id":1,"method":"getHealth","params":[]}"#;
        let (response, _) = rpc.raw_json_request(request, 1).await.unwrap();
        let response: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["result"]["ready"], true);
        updater.invalidate();
        let (response, _) = rpc.raw_json_request(request, 1).await.unwrap();
        let response: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(response["result"]["ready"], false);
    }

    #[tokio::test]
    async fn program_accounts_and_v2_use_standard_shapes() {
        let (store, mut updater) = store();
        for value in [1, 2] {
            let key = [value; 32];
            updater
                .queue(
                    key,
                    4,
                    1,
                    Some(Account {
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
                )
                .unwrap();
        }
        updater.confirm(4);
        let rpc = json_rpc(store);
        let program = program_id();
        let request = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getProgramAccounts","params":["{program}"]}}"#
        );
        let (response, _) = rpc.raw_json_request(&request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(body["result"].as_array().unwrap().len(), 2);

        let request = format!(
            r#"{{"jsonrpc":"2.0","id":2,"method":"getProgramAccounts","params":["{program}",{{"commitment":"confirmed","encoding":"base64","withContext":true}}]}}"#
        );
        let (response, _) = rpc.raw_json_request(&request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(body["result"]["context"]["slot"], 4);
        assert_eq!(body["result"]["value"].as_array().unwrap().len(), 2);
        assert_eq!(body["result"]["value"][0]["account"]["data"][1], "base64");

        let request = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"getProgramAccountsV2","params":["{program}",{{"limit":1}}]}}"#
        );
        let (response, _) = rpc.raw_json_request(&request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(
            body["result"]["accounts"][0]["account"]["data"][1],
            "base64+zstd"
        );
        let cursor = body["result"]["paginationKey"].as_str().unwrap();
        let request = format!(
            r#"{{"jsonrpc":"2.0","id":4,"method":"getProgramAccountsV2","params":["{program}",{{"limit":1,"paginationKey":"{cursor}"}}]}}"#
        );
        let (response, _) = rpc.raw_json_request(&request, 1).await.unwrap();
        let body: Value = serde_json::from_str(response.get()).unwrap();
        assert_eq!(body["result"]["accounts"].as_array().unwrap().len(), 1);
        assert!(body["result"]["paginationKey"].is_null());
    }
}
