/*
 * Kamu REST API
 *
 * You are currently running Kamu CLI in the API server mode. For a fully-featured server consider using [Kamu Node](https://docs.kamu.dev/node/).  ## Auth Some operation require an **API token**. Pass `--get-token` command line argument for CLI to generate a token for you.  ## Resources - [Documentation](https://docs.kamu.dev) - [Discord](https://discord.gg/nU6TXRQNXC) - [Other protocols](https://docs.kamu.dev/node/protocols/) - [Open Data Fabric specification](https://docs.kamu.dev/odf/) 
 *
 * The version of the OpenAPI document: 0.233.0
 * 
 * Generated by: https://openapi-generator.tech
 */

use crate::models;
use serde::{Deserialize, Serialize};

/// 
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum SchemaFormat {
    #[serde(rename = "ArrowJson")]
    ArrowJson,
    #[serde(rename = "Parquet")]
    Parquet,
    #[serde(rename = "ParquetJson")]
    ParquetJson,

}

impl std::fmt::Display for SchemaFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::ArrowJson => write!(f, "ArrowJson"),
            Self::Parquet => write!(f, "Parquet"),
            Self::ParquetJson => write!(f, "ParquetJson"),
        }
    }
}

impl Default for SchemaFormat {
    fn default() -> SchemaFormat {
        Self::ArrowJson
    }
}

