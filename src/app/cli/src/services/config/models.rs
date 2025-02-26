// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use container_runtime::{ContainerRuntimeType, NetworkNamespaceType};
use database_common::DatabaseProvider;
use duration_string::DurationString;
use kamu::utils::docker_images;
use kamu_accounts::*;
use kamu_datasets::DatasetEnvVarsConfig;
use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CLIConfig {
    /// Database connection configuration
    pub database: Option<DatabaseConfig>,

    /// Dataset environment variables configuration
    #[merge(strategy = merge_recursive)]
    pub dataset_env_vars: Option<DatasetEnvVarsConfig>,

    /// Engine configuration
    #[merge(strategy = merge_recursive)]
    pub engine: Option<EngineConfig>,

    /// Data access and visualization configuration
    #[merge(strategy = merge_recursive)]
    pub frontend: Option<FrontendConfig>,

    /// UNSTABLE: Identity configuration
    #[merge(strategy = merge_recursive)]
    pub identity: Option<IdentityConfig>,

    /// Messaging outbox configuration
    #[merge(strategy = merge_recursive)]
    pub outbox: Option<OutboxConfig>,

    /// Network protocols configuration
    #[merge(strategy = merge_recursive)]
    pub protocol: Option<ProtocolConfig>,

    /// Configuration for flow system
    #[merge(strategy = merge_recursive)]
    pub flow_system: Option<FlowSystemConfig>,

    /// Source configuration
    #[merge(strategy = merge_recursive)]
    pub source: Option<SourceConfig>,

    /// Users configuration
    #[merge(strategy = merge_recursive)]
    pub users: Option<PredefinedAccountsConfig>,

    /// Uploads configuration
    #[merge(strategy = merge_recursive)]
    pub uploads: Option<UploadsConfig>,
}

impl CLIConfig {
    pub fn new() -> Self {
        Self {
            database: None,
            dataset_env_vars: None,
            engine: None,
            frontend: None,
            identity: None,
            outbox: None,
            protocol: None,
            source: None,
            users: None,
            uploads: None,
            flow_system: None,
        }
    }

    // TODO: Remove this workaround
    // Returns config with all values set to non-None
    // This is used to walk the key tree where values that default to None would
    // otherwise be omitted
    pub fn sample() -> Self {
        Self {
            database: Some(DatabaseConfig::sample()),
            dataset_env_vars: Some(DatasetEnvVarsConfig::sample()),
            engine: Some(EngineConfig::sample()),
            frontend: Some(FrontendConfig::sample()),
            identity: Some(IdentityConfig::sample()),
            outbox: Some(OutboxConfig::sample()),
            protocol: Some(ProtocolConfig::sample()),
            source: Some(SourceConfig::sample()),
            users: Some(PredefinedAccountsConfig::sample()),
            uploads: Some(UploadsConfig::sample()),
            flow_system: Some(FlowSystemConfig::sample()),
        }
    }
}

impl Default for CLIConfig {
    fn default() -> Self {
        Self {
            database: None,
            dataset_env_vars: Some(DatasetEnvVarsConfig::default()),
            engine: Some(EngineConfig::default()),
            frontend: Some(FrontendConfig::default()),
            identity: Some(IdentityConfig::default()),
            outbox: Some(OutboxConfig::default()),
            protocol: Some(ProtocolConfig::default()),
            source: Some(SourceConfig::default()),
            users: Some(PredefinedAccountsConfig::default()),
            uploads: Some(UploadsConfig::default()),
            flow_system: Some(FlowSystemConfig::default()),
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
    /// HTTP-specific configuration
    pub http: Option<HttpSourceConfig>,
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
            http: None,
            mqtt: None,
            ethereum: None,
        }
    }

    fn sample() -> Self {
        Self {
            http: Some(HttpSourceConfig::sample()),
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
            http: Some(HttpSourceConfig::default()),
            mqtt: Some(MqttSourceConfig::default()),
            ethereum: Some(EthereumSourceConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HttpSourceConfig {
    /// Value to use for User-Agent header
    pub user_agent: Option<String>,
    /// Timeout for the connect phase of the HTTP client
    pub connect_timeout: Option<DurationString>,
    /// Maximum number of redirects to follow
    pub max_redirects: Option<usize>,
}

impl HttpSourceConfig {
    pub fn new() -> Self {
        Self {
            user_agent: None,
            connect_timeout: None,
            max_redirects: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }

    pub fn to_infra_cfg(&self) -> kamu::ingest::HttpSourceConfig {
        kamu::ingest::HttpSourceConfig {
            user_agent: self.user_agent.clone().unwrap(),
            connect_timeout: (*self.connect_timeout.as_ref().unwrap()).into(),
            max_redirects: self.max_redirects.unwrap(),
        }
    }
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        let infra_cfg = kamu::ingest::HttpSourceConfig::default();
        Self {
            user_agent: Some(concat!("kamu-cli/", env!("CARGO_PKG_VERSION")).to_string()),
            connect_timeout: Some(DurationString::from(infra_cfg.connect_timeout)),
            max_redirects: Some(infra_cfg.max_redirects),
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

    /// FlightSQL configuration
    #[merge(strategy = merge_recursive)]
    pub flight_sql: Option<FlightSqlConfig>,
}

impl ProtocolConfig {
    pub fn new() -> Self {
        Self {
            ipfs: None,
            flight_sql: None,
        }
    }

    fn sample() -> Self {
        Self {
            ipfs: Some(IpfsConfig::sample()),
            flight_sql: Some(FlightSqlConfig::sample()),
        }
    }
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            ipfs: Some(IpfsConfig::default()),
            flight_sql: Some(FlightSqlConfig::default()),
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
    /// List of public gateways can be found here: `https://ipfs.github.io/public-gateway-checker/`
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

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FlightSqlConfig {
    /// Whether clients can authenticate as 'anonymous' user
    pub allow_anonymous: Option<bool>,

    /// Time after which FlightSQL client session will be forgotten and client
    /// will have to re-authroize (for authenticated clients)
    pub authed_session_expiration_timeout: Option<DurationString>,

    /// Time after which FlightSQL session context will be released to free the
    /// resources (for authenticated clients)
    pub authed_session_inactivity_timeout: Option<DurationString>,

    /// Time after which FlightSQL client session will be forgotten and client
    /// will have to re-authroize (for anonymous clients)
    pub anon_session_expiration_timeout: Option<DurationString>,

    /// Time after which FlightSQL session context will be released to free the
    /// resources (for anonymous clients)
    pub anon_session_inactivity_timeout: Option<DurationString>,
}

impl FlightSqlConfig {
    pub fn new() -> Self {
        Self {
            allow_anonymous: None,
            authed_session_expiration_timeout: None,
            authed_session_inactivity_timeout: None,
            anon_session_expiration_timeout: None,
            anon_session_inactivity_timeout: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }

    pub fn to_session_auth_config(&self) -> kamu_adapter_flight_sql::SessionAuthConfig {
        kamu_adapter_flight_sql::SessionAuthConfig {
            allow_anonymous: self.allow_anonymous.unwrap(),
        }
    }

    pub fn to_session_caching_config(&self) -> kamu_adapter_flight_sql::SessionCachingConfig {
        kamu_adapter_flight_sql::SessionCachingConfig {
            authed_session_expiration_timeout: self
                .authed_session_expiration_timeout
                .unwrap()
                .into(),
            authed_session_inactivity_timeout: self
                .authed_session_inactivity_timeout
                .unwrap()
                .into(),
            anon_session_expiration_timeout: self.anon_session_expiration_timeout.unwrap().into(),
            anon_session_inactivity_timeout: self.anon_session_inactivity_timeout.unwrap().into(),
        }
    }
}

impl Default for FlightSqlConfig {
    fn default() -> Self {
        Self {
            allow_anonymous: Some(true),
            authed_session_expiration_timeout: Some(
                DurationString::from_string("30m".to_owned()).unwrap(),
            ),
            authed_session_inactivity_timeout: Some(
                DurationString::from_string("5s".to_owned()).unwrap(),
            ),
            anon_session_expiration_timeout: Some(
                DurationString::from_string("30m".to_owned()).unwrap(),
            ),
            anon_session_inactivity_timeout: Some(
                DurationString::from_string("5s".to_owned()).unwrap(),
            ),
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
            credentials_policy: DatabaseCredentialsPolicyConfig {
                source: DatabaseCredentialSourceConfig::RawPassword(
                    RawDatabasePasswordPolicyConfig {
                        user_name: String::from("root"),
                        raw_password: String::from("p455w0rd"),
                    },
                ),
                rotation_frequency_in_minutes: None,
            },
            database_name: String::from("kamu"),
            host: String::from("localhost"),
            port: Some(DatabaseProvider::Postgres.default_port()),
            acquire_timeout_secs: None,
            max_connections: None,
            max_lifetime_secs: None,
        })
    }

    pub fn sqlite(database_path: &Path) -> Self {
        Self::Sqlite(SqliteDatabaseConfig {
            database_path: database_path.to_str().unwrap().into(),
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
    pub credentials_policy: DatabaseCredentialsPolicyConfig,
    pub database_name: String,
    pub host: String,
    pub port: Option<u16>,
    pub max_connections: Option<u32>,
    pub max_lifetime_secs: Option<u64>,
    pub acquire_timeout_secs: Option<u64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCredentialsPolicyConfig {
    pub source: DatabaseCredentialSourceConfig,
    pub rotation_frequency_in_minutes: Option<u64>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "kind")]
pub enum DatabaseCredentialSourceConfig {
    RawPassword(RawDatabasePasswordPolicyConfig),
    AwsSecret(AwsSecretDatabasePasswordPolicyConfig),
    AwsIamToken(AwsIamTokenPasswordPolicyConfig),
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct RawDatabasePasswordPolicyConfig {
    pub user_name: String,
    pub raw_password: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AwsSecretDatabasePasswordPolicyConfig {
    pub secret_name: String,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct AwsIamTokenPasswordPolicyConfig {
    pub user_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Identity
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Default, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IdentityConfig {
    /// Private key used to sign API responses.
    /// Currently only `ed25519` keys are supported.
    ///
    /// To generate use:
    ///
    ///     dd if=/dev/urandom bs=1 count=32 status=none |
    ///         base64 -w0 |
    ///         tr '+/' '-_' |
    ///         tr -d '=' |
    ///         (echo -n u && cat)
    ///
    /// The command above:
    /// - reads 32 random bytes
    /// - base64-encodes them
    /// - converts default base64 encoding to base64url and removes padding
    /// - prepends a multibase prefix
    pub private_key: Option<odf::metadata::PrivateKey>,
}

impl IdentityConfig {
    pub fn new() -> Self {
        Self { private_key: None }
    }

    fn sample() -> Self {
        Self {
            private_key: Some(odf::metadata::PrivateKey::from_bytes(&[0; 32])),
        }
    }

    pub fn to_infra_cfg(&self) -> Option<kamu_adapter_http::data::query_types::IdentityConfig> {
        self.private_key
            .clone()
            .map(|private_key| kamu_adapter_http::data::query_types::IdentityConfig { private_key })
    }
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

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct OutboxConfig {
    pub awaiting_step_secs: Option<i64>,
    pub batch_size: Option<i64>,
}

impl OutboxConfig {
    pub fn sample() -> Self {
        Default::default()
    }
}

impl Default for OutboxConfig {
    fn default() -> Self {
        Self {
            awaiting_step_secs: Some(1),
            batch_size: Some(20),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FlowSystemConfig {
    #[merge(strategy = merge_recursive)]
    pub flow_agent: Option<FlowAgentConfig>,

    #[merge(strategy = merge_recursive)]
    pub task_agent: Option<TaskAgentConfig>,
}

impl FlowSystemConfig {
    pub fn sample() -> Self {
        Self {
            flow_agent: Some(FlowAgentConfig::sample()),
            task_agent: Some(TaskAgentConfig::sample()),
        }
    }
}

impl Default for FlowSystemConfig {
    fn default() -> Self {
        Self {
            flow_agent: Some(FlowAgentConfig::default()),
            task_agent: Some(TaskAgentConfig::default()),
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FlowAgentConfig {
    pub awaiting_step_secs: Option<i64>,
    pub mandatory_throttling_period_secs: Option<i64>,
}

impl FlowAgentConfig {
    pub fn new() -> Self {
        Self {
            awaiting_step_secs: None,
            mandatory_throttling_period_secs: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for FlowAgentConfig {
    fn default() -> Self {
        Self {
            awaiting_step_secs: Some(1),
            mandatory_throttling_period_secs: Some(60),
        }
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TaskAgentConfig {
    pub mandatory_throttling_period_secs: Option<i64>,
}

impl TaskAgentConfig {
    pub fn new() -> Self {
        Self {
            mandatory_throttling_period_secs: None,
        }
    }

    fn sample() -> Self {
        Self { ..Self::default() }
    }
}

impl Default for TaskAgentConfig {
    fn default() -> Self {
        Self {
            mandatory_throttling_period_secs: Some(1),
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
