/*
 * Kamu REST API
 *
 * You are currently running Kamu CLI in the API server mode. For a fully-featured server consider using [Kamu Node](https://docs.kamu.dev/node/).  ## Auth Some operation require an **API token**. Pass `--get-token` command line argument for CLI to generate a token for you.  ## Resources - [Documentation](https://docs.kamu.dev) - [Discord](https://discord.gg/nU6TXRQNXC) - [Other protocols](https://docs.kamu.dev/node/protocols/) - [Open Data Fabric specification](https://docs.kamu.dev/odf/)
 *
 * The version of the OpenAPI document: 0.233.0
 *
 * Generated by: https://openapi-generator.tech
 */

use serde::{Deserialize, Serialize};

use crate::models;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountResponse {
    #[serde(rename = "accountName")]
    pub account_name: String,
    #[serde(rename = "id")]
    pub id: String,
}

impl AccountResponse {
    pub fn new(account_name: String, id: String) -> AccountResponse {
        AccountResponse { account_name, id }
    }
}
