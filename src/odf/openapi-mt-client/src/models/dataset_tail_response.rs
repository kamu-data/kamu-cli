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
pub struct DatasetTailResponse {
    /// Resulting data
    #[serde(rename = "data")]
    pub data: serde_json::Value,
    /// How data is laid out in the response
    #[serde(rename = "dataFormat")]
    pub data_format: models::DataFormat,
    #[serde(rename = "schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<serde_json::Value>,
    /// What representation is used for the schema
    #[serde(rename = "schemaFormat", skip_serializing_if = "Option::is_none")]
    pub schema_format: Option<models::SchemaFormat>,
}

impl DatasetTailResponse {
    pub fn new(data: serde_json::Value, data_format: models::DataFormat) -> DatasetTailResponse {
        DatasetTailResponse {
            data,
            data_format,
            schema: None,
            schema_format: None,
        }
    }
}
