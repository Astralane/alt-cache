use crate::{
    state_updater::FetchedAltState,
    store::{Account, KeyedUiAccount, parse_key, program_id},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

const PAGE_SIZE: usize = 10_000;

#[derive(Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Deserialize)]
struct JsonRpcError {
    code: i64,
    message: String,
}

struct RpcResult<T> {
    value: T,
    response_bytes: usize,
    fetch_duration: Duration,
    deserialize_duration: Duration,
}

async fn rpc<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    method: &str,
    params: impl Serialize,
) -> Result<RpcResult<T>> {
    let fetch_started = Instant::now();
    let response = client
        .post(url)
        .json(&json!({"jsonrpc":"2.0", "id":1, "method":method, "params":params}))
        .send()
        .await
        .map_err(redact_rpc_error)?
        .error_for_status()
        .map_err(redact_rpc_error)?;
    let body = response.bytes().await.map_err(redact_rpc_error)?;
    let fetch_duration = fetch_started.elapsed();
    let deserialize_started = Instant::now();
    let reply: JsonRpcResponse<T> = serde_json::from_slice(&body)?;
    let deserialize_duration = deserialize_started.elapsed();
    if let Some(error) = reply.error {
        anyhow::bail!("RPC error {}: {}", error.code, error.message);
    }
    Ok(RpcResult {
        value: reply.result.context("RPC result missing")?,
        response_bytes: body.len(),
        fetch_duration,
        deserialize_duration,
    })
}

fn redact_rpc_error(error: reqwest::Error) -> anyhow::Error {
    anyhow::Error::new(error.without_url())
}

#[derive(Deserialize)]
struct PagedAccounts {
    accounts: Vec<KeyedUiAccount>,
    #[serde(rename = "paginationKey")]
    pagination_key: Option<String>,
}

#[derive(Deserialize)]
struct ContextResponse<T> {
    context: RpcContext,
    value: T,
}

#[derive(Deserialize)]
struct RpcContext {
    slot: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgramAccountsConfig<'a> {
    commitment: &'static str,
    encoding: &'static str,
    with_context: bool,
    limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_context_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination_key: Option<&'a str>,
}

pub(crate) async fn fetch_full_alt_state(
    client: &reqwest::Client,
    url: &str,
    minimum_context_slot: Option<u64>,
) -> Result<FetchedAltState> {
    let started = Instant::now();
    let mut context_slot = None;
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    let mut accounts = BTreeMap::new();
    let mut page_number = 0_u64;
    let mut fetched_accounts = 0_usize;
    let mut skipped_accounts = 0_usize;
    let mut response_bytes = 0_u64;
    let mut fetch_duration = Duration::ZERO;
    let mut json_deserialize_duration = Duration::ZERO;
    let mut account_decode_duration = Duration::ZERO;
    loop {
        page_number += 1;
        let config = ProgramAccountsConfig {
            commitment: "confirmed",
            encoding: "base64+zstd",
            with_context: true,
            limit: PAGE_SIZE,
            min_context_slot: minimum_context_slot,
            pagination_key: cursor.as_deref(),
        };
        let response: RpcResult<ContextResponse<PagedAccounts>> = rpc(
            client,
            url,
            "getProgramAccountsV2",
            json!([program_id(), config]),
        )
        .await?;
        fetch_duration += response.fetch_duration;
        json_deserialize_duration += response.deserialize_duration;
        response_bytes += response.response_bytes as u64;
        let response = response.value;
        let slot = response.context.slot;
        let first_slot = *context_slot.get_or_insert(slot);
        if let Some(minimum) = minimum_context_slot {
            ensure!(
                slot >= minimum,
                "RPC context slot {slot} is below required slot {minimum}"
            );
        }
        ensure!(
            slot >= first_slot,
            "RPC snapshot context regressed from slot {first_slot} to {slot}"
        );
        let page = response.value;
        fetched_accounts += page.accounts.len();
        let decode_started = Instant::now();
        for entry in page.accounts {
            if entry.account.lamports == 0 {
                skipped_accounts += 1;
                continue;
            }
            let key = parse_key(&entry.pubkey)?;
            let pubkey = entry.pubkey.clone();
            let account = match Account::from_rpc(entry.pubkey, entry.account) {
                Ok(account) => account,
                Err(error) => {
                    tracing::warn!(%pubkey, %error, "skipping invalid ALT account");
                    skipped_accounts += 1;
                    continue;
                }
            };
            accounts.insert(key, Arc::new(account));
        }
        account_decode_duration += decode_started.elapsed();
        let Some(next) = page.pagination_key else {
            break;
        };
        ensure!(seen.insert(next.clone()), "RPC repeated its pagination key");
        cursor = Some(next);
    }

    let context_slot = context_slot.context("RPC snapshot context missing")?;
    tracing::info!(
        context_slot,
        pages = page_number,
        accounts = accounts.len(),
        fetched_accounts,
        skipped_accounts,
        response_bytes,
        fetch_ms = fetch_duration.as_millis() as u64,
        json_deserialize_ms = json_deserialize_duration.as_millis() as u64,
        account_decode_ms = account_decode_duration.as_millis() as u64,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "full ALT state fetched"
    );
    Ok(FetchedAltState {
        context_slot,
        accounts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_requests_the_current_confirmed_slot() {
        let config = ProgramAccountsConfig {
            commitment: "confirmed",
            encoding: "base64+zstd",
            with_context: true,
            limit: PAGE_SIZE,
            min_context_slot: Some(42),
            pagination_key: None,
        };

        let value = serde_json::to_value(config).unwrap();

        assert_eq!(value["minContextSlot"], 42);
    }
}
