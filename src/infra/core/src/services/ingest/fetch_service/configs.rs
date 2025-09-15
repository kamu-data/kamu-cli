// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Target number of records after which we will stop consuming from the
    /// resumable source and commit data, leaving the rest for the next
    /// iteration. This ensures that one data slice doesn't become too big.
    pub target_records_per_slice: u64,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            target_records_per_slice: 10_000,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// Value to use for User-Agent header
    pub user_agent: String,
    /// Timeout for the connect phase of the HTTP client
    pub connect_timeout: std::time::Duration,
    /// Maximum number of redirects to follow
    pub max_redirects: usize,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            user_agent: concat!("kamu/", env!("CARGO_PKG_VERSION")).to_string(),
            connect_timeout: std::time::Duration::from_secs(30),
            max_redirects: 10,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct MqttSourceConfig {
    /// Time in milliseconds to wait for MQTT broker to send us some data after
    /// which we will consider that we have "caught up" and end the polling
    /// loop.
    pub broker_idle_timeout_ms: u64,
}

impl Default for MqttSourceConfig {
    fn default() -> Self {
        Self {
            broker_idle_timeout_ms: 1_000,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct EthereumSourceConfig {
    /// Default RPC endpoints to use if source does not specify one explicitly.
    pub rpc_endpoints: Vec<EthRpcEndpoint>,

    /// Default number of blocks to scan within one query to `eth_getLogs` RPC
    /// endpoint.
    pub get_logs_block_stride: u64,

    // TODO: Consider replacing this with logic that upon encountering an error still commits the
    // progress made before it
    /// Forces iteration to stop after the specified number of blocks were
    /// scanned even if we didn't reach the target record number. This is useful
    /// to not lose a lot of scanning progress in case of an RPC error.
    pub commit_after_blocks_scanned: u64,

    /// Many providers don't yet return `blockTimestamp` from `eth_getLogs` RPC
    /// endpoint and in such cases `block_timestamp` column will be `null`.
    /// If you enable this fallback the library will perform additional call to
    /// `eth_getBlock` to populate the timestam, but this may result in
    /// significant performance penalty when fetching many log records.
    ///
    /// See: https://github.com/ethereum/execution-apis/issues/295
    pub use_block_timestamp_fallback: bool,
}

impl Default for EthereumSourceConfig {
    fn default() -> Self {
        Self {
            rpc_endpoints: Vec::new(),
            get_logs_block_stride: 100_000,
            commit_after_blocks_scanned: 1_000_000,
            use_block_timestamp_fallback: false,
        }
    }
}

impl EthereumSourceConfig {
    pub fn get_endpoint_by_chain_id(&self, chain_id: u64) -> Option<&EthRpcEndpoint> {
        self.rpc_endpoints.iter().find(|e| e.chain_id == chain_id)
    }
}

#[derive(Debug, Clone)]
pub struct EthRpcEndpoint {
    pub chain_id: u64,
    pub chain_name: String,
    pub node_url: Url,
}
