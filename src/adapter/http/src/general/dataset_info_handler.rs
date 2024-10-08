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

use axum::extract::{Extension, Path};
use axum::response::Json;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use kamu_core::{DatasetRepository, GetDatasetError};
use opendatafabric::{AccountName, DatasetHandle, DatasetID, DatasetName};

use crate::axum_utils::ensure_authenticated_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetInfoResponse {
    pub id: DatasetID,
    pub account_name: Option<AccountName>,
    pub dataset_name: DatasetName,
}

impl From<DatasetHandle> for DatasetInfoResponse {
    fn from(value: DatasetHandle) -> Self {
        Self {
            id: value.id,
            account_name: value.alias.account_name,
            dataset_name: value.alias.dataset_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn dataset_info_handler(
    Extension(catalog): Extension<Catalog>,
    Path(dataset_id): Path<DatasetID>,
) -> Result<Json<DatasetInfoResponse>, ApiError> {
    let response = get_dataset_by_id(&catalog, &dataset_id).await?;
    tracing::debug!(?response, "Get dataset by id info response");
    Ok(response)
}

async fn get_dataset_by_id(
    catalog: &Catalog,
    dataset_id: &DatasetID,
) -> Result<Json<DatasetInfoResponse>, ApiError> {
    ensure_authenticated_account(catalog).api_err()?;

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_handle = dataset_repo
        .resolve_dataset_ref(&dataset_id.clone().as_local_ref())
        .await
        .map_err(|err| match err {
            GetDatasetError::NotFound(e) => ApiError::not_found(e),
            GetDatasetError::Internal(e) => e.api_err(),
        })?;

    Ok(Json(dataset_handle.into()))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
