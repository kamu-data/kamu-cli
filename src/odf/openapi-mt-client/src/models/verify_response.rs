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

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct VerifyResponse {
    /// Will contain error details if validation was unsuccessful
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<Box<models::ValidationError>>,
    /// Whether validation was successful
    #[serde(rename = "ok")]
    pub ok: bool,
}

impl VerifyResponse {
    pub fn new(ok: bool) -> VerifyResponse {
        VerifyResponse {
            error: None,
            ok,
        }
    }
}

