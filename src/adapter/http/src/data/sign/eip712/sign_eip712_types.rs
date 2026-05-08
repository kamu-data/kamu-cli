// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::data::query_types::ProofType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct SignEip712QueryParams {
    // TODO: Molecule: Phase 3: stricter type?
    /// DID of the managed key to sign with
    pub key: String,

    /// Include a node proof over the resulting signature
    #[serde(default)]
    pub include_node_proof: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Eip712TypedDataRequestBody {
    pub domain: Eip712Domain,
    pub primary_type: String,
    pub types: HashMap<String, Vec<Eip712TypeDetails>>,
    #[schema(value_type = Object)]
    pub message: serde_json::Map<String, serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Eip712Domain {
    pub name: String,
    pub version: String,
    pub chain_id: u64,
    pub verifying_contract: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Eip712TypeDetails {
    pub name: String,
    pub r#type: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Response
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignEip712Response {
    pub r#type: ProofType,
    pub signature: odf::metadata::Signature,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proof: Option<SignEip712Proof>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SignEip712Proof {
    pub r#type: ProofType,
    pub verification_method: String,
    pub signature: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
