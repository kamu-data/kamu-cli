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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValidationError {
    InvalidRequest(Box<models::InvalidRequest>),
    VerificationFailed(Box<models::VerificationFailed>),
}

impl Default for ValidationError {
    fn default() -> Self {
        Self::InvalidRequest(Default::default())
    }
}
/// 
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Kind {
    #[serde(rename = "VerificationFailed::DatasetBlockNotFound")]
    VerificationFailedColonColonDatasetBlockNotFound,
}

impl Default for Kind {
    fn default() -> Kind {
        Self::VerificationFailedColonColonDatasetBlockNotFound
    }
}

