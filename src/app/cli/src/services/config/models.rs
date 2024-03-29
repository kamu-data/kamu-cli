// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use container_runtime::{ContainerRuntimeType, NetworkNamespaceType};
use duration_string::DurationString;
use kamu::domain::auth::{self, AccountType};
use kamu::utils::docker_images;
use merge::Merge;
use opendatafabric::{AccountName, FAKE_ACCOUNT_ID};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CLIConfig {
    /// Engine configuration
    #[merge(strategy = merge_recursive)]
    pub engine: Option<EngineConfig>,
    /// Network protocols configuration
    #[merge(strategy = merge_recursive)]
    pub protocol: Option<ProtocolConfig>,
    /// Data access and visualization configuration
    #[merge(strategy = merge_recursive)]
    pub frontend: Option<FrontendConfig>,
    /// Users configuration
    #[merge(strategy = merge_recursive)]
    pub users: Option<UsersConfig>,
}

impl CLIConfig {
    pub fn new() -> Self {
        Self {
            engine: None,
            protocol: None,
            frontend: None,
            users: None,
        }
    }

    // TODO: Remove this workaround
    // Returns config with all values set to non-None
    // This is used to walk the key tree where values that default to None would
    // otherwise be omitted
    pub fn sample() -> Self {
        Self {
            engine: Some(EngineConfig::sample()),
            protocol: Some(ProtocolConfig::sample()),
            frontend: Some(FrontendConfig::sample()),
            users: Some(UsersConfig::sample()),
        }
    }
}

impl Default for CLIConfig {
    fn default() -> Self {
        Self {
            engine: Some(EngineConfig::default()),
            protocol: Some(ProtocolConfig::default()),
            frontend: Some(FrontendConfig::default()),
            users: Some(UsersConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////

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
}

impl EngineImagesConfig {
    pub fn new() -> Self {
        Self {
            spark: None,
            flink: None,
            datafusion: None,
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
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IpfsConfig {
    /// HTTP Gateway URL to use for downloads.
    /// For safety it defaults to `http://localhost:8080` - a local IPFS daemon.
    /// If you don't have IPFS installed you can set this URL to
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

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UsersConfig {
    #[merge(strategy = merge::vec::append)]
    pub predefined: Vec<auth::AccountInfo>,
    pub allow_login_unknown: Option<bool>,
}

impl UsersConfig {
    pub fn new() -> Self {
        Self::default()
    }

    fn sample() -> Self {
        Self::default()
    }

    pub fn single_tenant() -> Self {
        Self {
            predefined: vec![auth::AccountInfo {
                account_id: FAKE_ACCOUNT_ID.to_string(),
                account_name: AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME),
                account_type: AccountType::User,
                display_name: String::from(auth::DEFAULT_ACCOUNT_NAME),
                avatar_url: Some(String::from(auth::DEFAULT_AVATAR_URL)),
                is_admin: true,
            }],
            allow_login_unknown: Some(false),
        }
    }
}

impl Default for UsersConfig {
    fn default() -> Self {
        Self {
            predefined: Vec::new(),
            allow_login_unknown: Some(true),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigScope {
    User,
    Workspace,
    Flattened,
}

////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////
