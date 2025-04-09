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
pub struct Proof {
    #[serde(rename = "proofValue")]
    pub proof_value: String,
    /// Type of the proof provided
    #[serde(rename = "type")]
    pub r#type: models::ProofType,
    #[serde(rename = "verificationMethod")]
    pub verification_method: String,
}

impl Proof {
    pub fn new(proof_value: String, r#type: models::ProofType, verification_method: String) -> Proof {
        Proof {
            proof_value,
            r#type,
            verification_method,
        }
    }
}

