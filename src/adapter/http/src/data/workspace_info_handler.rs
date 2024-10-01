// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use kamu_core::DatasetRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub is_multi_tenant: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn workspace_info_handler(
    Extension(catalog): Extension<Catalog>,
) -> Result<Json<Response>, ApiError> {
    let response = get_workspace_info(&catalog);
    tracing::debug!(?response, "Get workspace info response");
    Ok(response)
}

fn get_workspace_info(catalog: &Catalog) -> Json<Response> {
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    Json(Response {
        is_multi_tenant: dataset_repo.is_multi_tenant(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
