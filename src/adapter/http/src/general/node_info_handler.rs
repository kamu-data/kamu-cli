// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::Extension;
use axum::response::Json;
use dill::Catalog;
use http_common::*;
use kamu_core::TenancyConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfoResponse {
    pub is_multi_tenant: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get ODF node description
#[utoipa::path(
    get,
    path = "/info",
    responses((status = OK, body = NodeInfoResponse)),
    tag = "odf-core",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn node_info_handler(
    Extension(catalog): Extension<Catalog>,
) -> Result<Json<NodeInfoResponse>, ApiError> {
    let response = get_node_info(&catalog);
    tracing::debug!(?response, "Get node info response");
    Ok(response)
}

fn get_node_info(catalog: &Catalog) -> Json<NodeInfoResponse> {
    let tenancy_config = catalog.get_one::<TenancyConfig>().unwrap();

    Json(NodeInfoResponse {
        is_multi_tenant: *tenancy_config == TenancyConfig::MultiTenant,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
