// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[into_params(parameter_in = Query)]
pub struct SignEip712QueryParams {
    /// DID of the managed key to sign with
    pub key: odf::metadata::DidOdf,

    /// Include a node proof over the resulting signature
    #[serde(default)]
    pub include_node_proof: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Request (schema)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[schema(as = Eip712TypedData)]
pub struct Eip712TypedDataSchema {
    pub domain: Eip712DomainSchema,
    pub primary_type: String,
    pub types: HashMap<String, Vec<Eip712TypeDetailsSchema>>,
    #[schema(value_type = Object)]
    pub message: serde_json::Map<String, serde_json::Value>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Eip712DomainSchema {
    pub name: String,
    pub version: String,
    pub chain_id: u64,
    pub verifying_contract: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Eip712TypeDetailsSchema {
    pub name: String,
    pub r#type: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
