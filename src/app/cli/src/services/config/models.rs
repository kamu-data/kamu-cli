// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::{ContainerRuntimeType, NetworkNamespaceType};
use database_common::DatabaseProvider;
use duration_string::DurationString;
use kamu::utils::docker_images;
use kamu_accounts::*;
use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CLIConfig {
    /// Engine configuration
    #[merge(strategy = merge_recursive)]
    pub engine: Option<EngineConfig>,

    /// Source configuration
    #[merge(strategy = merge_recursive)]
    pub source: Option<SourceConfig>,

    /// Network protocols configuration
    #[merge(strategy = merge_recursive)]
    pub protocol: Option<ProtocolConfig>,

    /// Data access and visualization configuration
    #[merge(strategy = merge_recursive)]
    pub frontend: Option<FrontendConfig>,

    /// Users configuration
    #[merge(strategy = merge_recursive)]
    pub users: Option<PredefinedAccountsConfig>,

    /// Database connection configuration
    pub database: Option<DatabaseConfig>,
    /// Uploads configuration
    #[merge(strategy = merge_recursive)]
    pub uploads: Option<UploadsConfig>,
}

impl CLIConfig {
    pub fn new() -> Self {
        Self {
            engine: None,
            source: None,
            protocol: None,
            frontend: None,
            users: None,
            database: None,
            uploads: None,
        }
    }

    // TODO: Remove this workaround
    // Returns config with all values set to non-None
    // This is used to walk the key tree where values that default to None would
    // otherwise be omitted
    pub fn sample() -> Self {
        Self {
            engine: Some(EngineConfig::sample()),
            source: Some(SourceConfig::sample()),
            protocol: Some(ProtocolConfig::sample()),
            frontend: Some(FrontendConfig::sample()),
            users: Some(PredefinedAccountsConfig::sample()),
            database: Some(DatabaseConfig::sample()),
            uploads: Some(UploadsConfig::sample()),
        }
    }
}

impl Default for CLIConfig {
    fn default() -> Self {
        Self {
            engine: Some(EngineConfig::default()),
            source: Some(SourceConfig::default()),
            protocol: Some(ProtocolConfig::default()),
            frontend: Some(FrontendConfig::default()),
            users: Some(PredefinedAccountsConfig::default()),
            database: None,
            uploads: Some(UploadsConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Engine
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EngineConfig {
    /// Maximum number of engine operations that can be performed concurrently
    pub max_concurrency: Option<u32>,
    /// Type of the runtime to use when running the data processing engines
    pub runtime: Option<ContainerRuntimeType>,
    /// Type of the networking namespace (relevant when running in container
    /// environments)
    pub network_ns: Option<NetworkNamespaceType>,
    /// Timeout for starting an engine container
    pub start_timeout: Option<DurationString>,
    /// Timeout for waiting the engine container to stop gracefully
    pub shutdown_timeout: Option<DurationString>,
    /// UNSTABLE: Default engine images
    #[merge(strategy = merge_recursive)]
    pub images: Option<EngineImagesConfig>,
}

impl EngineConfig {
    pub fn new() -> Self {
        Self {
            max_concurrency: None,
            runtime: None,
            network_ns: None,
            start_timeout: None,
            shutdown_timeout: None,
            images: None,
        }
    }

    fn sample() -> Self {
        Self {
            max_concurrency: Some(0),
            images: Some(EngineImagesConfig::sample()),
            ..Self::default()
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrency: None,
            runtime: Some(ContainerRuntimeType::Docker),
            network_ns: Some(NetworkNamespaceType::Private),
            start_timeout: Some(DurationString::from_string("30s".to_owned()).unwrap()),
            shutdown_timeout: Some(DurationString::from_string("5s".to_owned()).unwrap()),
            images: Some(EngineImagesConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EngineImagesConfig {
    /// UNSTABLE: Spark engine image
    pub spark: Option<String>,
    /// UNSTABLE: Flink engine image
    pub flink: Option<String>,
    /// UNSTABLE: Datafusion engine image
    pub datafusion: Option<String>,
    /// UNSTABLE: RisingWave engine image
    pub risingwave: Option<String>,
}

impl EngineImagesConfig {
    pub fn new() -> Self {
        Self {
            spark: None,
            flink: None,
            datafusion: None,
            risingwave: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for EngineImagesConfig {
    fn default() -> Self {
        Self {
            spark: Some(docker_images::SPARK.to_owned()),
            flink: Some(docker_images::FLINK.to_owned()),
            datafusion: Some(docker_images::DATAFUSION.to_owned()),
            risingwave: Some(docker_images::RISINGWAVE.to_owned()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Source
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceConfig {
    /// Target number of records after which we will stop consuming from the
    /// resumable source and commit data, leaving the rest for the next
    /// iteration. This ensures that one data slice doesn't become too big.
    pub target_records_per_slice: Option<u64>,
    /// MQTT-specific configuration
    #[merge(strategy = merge_recursive)]
    pub mqtt: Option<MqttSourceConfig>,
    /// Ethereum-specific configuration
    #[merge(strategy = merge_recursive)]
    pub ethereum: Option<EthereumSourceConfig>,
}

impl SourceConfig {
    pub fn new() -> Self {
        Self {
            target_records_per_slice: None,
            mqtt: None,
            ethereum: None,
        }
    }

    fn sample() -> Self {
        Self {
            mqtt: Some(MqttSourceConfig::sample()),
            ethereum: Some(EthereumSourceConfig::sample()),
            ..Self::default()
        }
    }

    pub fn to_infra_cfg(&self) -> kamu::ingest::SourceConfig {
        kamu::ingest::SourceConfig {
            target_records_per_slice: self.target_records_per_slice.unwrap(),
        }
    }
}

impl Default for SourceConfig {
    fn default() -> Self {
        let infra_cfg = kamu::ingest::SourceConfig::default();
        Self {
            target_records_per_slice: Some(infra_cfg.target_records_per_slice),
            mqtt: Some(MqttSourceConfig::default()),
            ethereum: Some(EthereumSourceConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct MqttSourceConfig {
    /// Time in milliseconds to wait for MQTT broker to send us some data after
    /// which we will consider that we have "caught up" and end the polling
    /// loop.
    pub broker_idle_timeout_ms: Option<u64>,
}

impl MqttSourceConfig {
    pub fn new() -> Self {
        Self {
            broker_idle_timeout_ms: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }

    pub fn to_infra_cfg(&self) -> kamu::ingest::MqttSourceConfig {
        kamu::ingest::MqttSourceConfig {
            broker_idle_timeout_ms: self.broker_idle_timeout_ms.unwrap(),
        }
    }
}

impl Default for MqttSourceConfig {
    fn default() -> Self {
        let infra_cfg = kamu::ingest::MqttSourceConfig::default();
        Self {
            broker_idle_timeout_ms: Some(infra_cfg.broker_idle_timeout_ms),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EthereumSourceConfig {
    /// Default RPC endpoints to use if source does not specify one explicitly.
    #[merge(strategy = merge::vec::append)]
    pub rpc_endpoints: Vec<EthRpcEndpoint>,
    /// Default number of blocks to scan within one query to `eth_getLogs` RPC
    /// endpoint.
    pub get_logs_block_stride: Option<u64>,
    /// Forces iteration to stop after the specified number of blocks were
    /// scanned even if we didn't reach the target record number. This is useful
    /// to not lose a lot of scanning progress in case of an RPC error.
    pub commit_after_blocks_scanned: Option<u64>,
}

impl EthereumSourceConfig {
    pub fn new() -> Self {
        Self {
            rpc_endpoints: Vec::new(),
            get_logs_block_stride: None,
            commit_after_blocks_scanned: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }

    pub fn to_infra_cfg(&self) -> kamu::ingest::EthereumSourceConfig {
        kamu::ingest::EthereumSourceConfig {
            rpc_endpoints: self
                .rpc_endpoints
                .iter()
                .map(EthRpcEndpoint::to_infra_cfg)
                .collect(),
            get_logs_block_stride: self.get_logs_block_stride.unwrap(),
            commit_after_blocks_scanned: self.commit_after_blocks_scanned.unwrap(),
        }
    }
}

impl Default for EthereumSourceConfig {
    fn default() -> Self {
        let infra_cfg = kamu::ingest::EthereumSourceConfig::default();
        Self {
            rpc_endpoints: Vec::new(),
            get_logs_block_stride: Some(infra_cfg.get_logs_block_stride),
            commit_after_blocks_scanned: Some(infra_cfg.commit_after_blocks_scanned),
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EthRpcEndpoint {
    pub chain_id: u64,
    pub chain_name: String,
    pub node_url: Url,
}

impl EthRpcEndpoint {
    pub fn to_infra_cfg(&self) -> kamu::ingest::EthRpcEndpoint {
        kamu::ingest::EthRpcEndpoint {
            chain_id: self.chain_id,
            chain_name: self.chain_name.clone(),
            node_url: self.node_url.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Protocol
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProtocolConfig {
    /// IPFS configuration
    #[merge(strategy = merge_recursive)]
    pub ipfs: Option<IpfsConfig>,
}

impl ProtocolConfig {
    pub fn new() -> Self {
        Self { ipfs: None }
    }

    fn sample() -> Self {
        Self {
            ipfs: Some(IpfsConfig::sample()),
        }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            ipfs: Some(IpfsConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IpfsConfig {
    /// HTTP Gateway URL to use for downloads.
    /// For safety, it defaults to `http://localhost:8080` - a local IPFS daemon.
    /// If you don't have IPFS installed, you can set this URL to
    /// one of the public gateways like `https://ipfs.io`.
    /// List of public gateways can be found here: https://ipfs.github.io/public-gateway-checker/
    pub http_gateway: Option<Url>,

    /// Whether kamu should pre-resolve IPNS DNSLink names using DNS or leave it
    /// to the Gateway.
    pub pre_resolve_dnslink: Option<bool>,
}

impl IpfsConfig {
    pub fn new() -> Self {
        Self {
            http_gateway: None,
            pre_resolve_dnslink: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for IpfsConfig {
    fn default() -> Self {
        Self {
            http_gateway: Some(Url::parse("http://localhost:8080").unwrap()),
            pre_resolve_dnslink: Some(true),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Frontend
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FrontendConfig {
    /// Integrated Jupyter notebook configuration
    #[merge(strategy = merge_recursive)]
    pub jupyter: Option<JupyterConfig>,
}

impl FrontendConfig {
    pub fn new() -> Self {
        Self { jupyter: None }
    }

    fn sample() -> Self {
        Self {
            jupyter: Some(JupyterConfig::sample()),
        }
    }
}

impl Default for FrontendConfig {
    fn default() -> Self {
        Self {
            jupyter: Some(JupyterConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JupyterConfig {
    /// Jupyter notebook server image
    pub image: Option<String>,
    /// UNSTABLE: Livy + Spark server image
    pub livy_image: Option<String>,
}

impl JupyterConfig {
    pub const IMAGE: &'static str = docker_images::JUPYTER;

    pub fn new() -> Self {
        Self {
            image: None,
            livy_image: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for JupyterConfig {
    fn default() -> Self {
        Self {
            image: Some(Self::IMAGE.to_owned()),
            livy_image: EngineImagesConfig::default().spark,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Database
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "provider")]
pub enum DatabaseConfig {
    InMemory,
    Sqlite(SqliteDatabaseConfig),
    Postgres(RemoteDatabaseConfig),
    MySql(RemoteDatabaseConfig),
    MariaDB(RemoteDatabaseConfig),
}

impl DatabaseConfig {
    pub fn sample() -> Self {
        Self::Postgres(RemoteDatabaseConfig {
            user: String::from("root"),
            password: String::from("p455w0rd"),
            database_name: String::from("kamu"),
            host: String::from("localhost"),
            port: Some(DatabaseProvider::Postgres.default_port()),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct SqliteDatabaseConfig {
    pub database_path: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RemoteDatabaseConfig {
    pub user: String,
    pub password: String,
    pub database_name: String,
    pub host: String,
    pub port: Option<u32>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Misc
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct UploadsConfig {
    pub max_file_size_in_mb: Option<usize>,
}

impl UploadsConfig {
    pub fn sample() -> Self {
        Default::default()
    }
}

impl Default for UploadsConfig {
    fn default() -> Self {
        Self {
            max_file_size_in_mb: Some(50),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigScope {
    User,
    Workspace,
    Flattened,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// For some reason merge crate does not recursively merge values inside `Option`
fn merge_recursive<T>(left: &mut Option<T>, right: Option<T>)
where
    T: Merge,
{
    let Some(r) = right else {
        return;
    };

    if let Some(l) = left {
        l.merge(r);
    } else {
        left.replace(r);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
