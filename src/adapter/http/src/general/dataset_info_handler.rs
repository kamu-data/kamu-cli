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
use internal_error::ErrorIntoInternal;
use kamu_accounts::AuthenticationService;
use kamu_datasets::{ViewDatasetUseCase, ViewDatasetUseCaseError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetOwnerInfo {
    pub account_name: odf::AccountName,

    // TODO: This should not be optional. Awaiting dataset repository refactoring.
    #[schema(value_type = String, required = false)]
    pub account_id: Option<odf::AccountID>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetInfoResponse {
    pub id: odf::DatasetID,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = DatasetOwnerInfo, required = false)]
    pub owner: Option<DatasetOwnerInfo>,

    pub dataset_name: odf::DatasetName,
}

impl DatasetInfoResponse {
    fn into_response(
        dataset_handle: odf::DatasetHandle,
        account_id_maybe: Option<odf::AccountID>,
    ) -> Self {
        Self {
            id: dataset_handle.id,
            owner: dataset_handle
                .alias
                .account_name
                .map(|account_name| DatasetOwnerInfo {
                    account_name,
                    account_id: account_id_maybe,
                }),
            dataset_name: dataset_handle.alias.dataset_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get dataset info by ID
#[utoipa::path(
    get,
    path = "/datasets/{id}",
    params(
        ("id" = String, Path, description = "Dataset ID")
    ),
    responses(
        (status = OK, body = DatasetInfoResponse),
        (status = UNAUTHORIZED, body = ApiErrorResponse),
        (status = NOT_FOUND, body = ApiErrorResponse),
    ),
    tag = "kamu",
    security(
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn dataset_info_handler(
    Extension(catalog): Extension<Catalog>,
    Path(dataset_id): Path<odf::DatasetID>,
) -> Result<Json<DatasetInfoResponse>, ApiError> {
    let response = get_dataset_by_id(&catalog, &dataset_id).await?;
    tracing::debug!(?response, "Get dataset by id info response");
    Ok(response)
}

async fn get_dataset_by_id(
    catalog: &Catalog,
    dataset_id: &odf::DatasetID,
) -> Result<Json<DatasetInfoResponse>, ApiError> {
    let view_dataset_use_case = catalog.get_one::<dyn ViewDatasetUseCase>().unwrap();

    let dataset_handle = view_dataset_use_case
        .execute(&dataset_id.as_local_ref())
        .await
        .map_err(|e| match e {
            ViewDatasetUseCaseError::NotFound(_) | ViewDatasetUseCaseError::Access(_) => {
                ApiError::not_found_without_reason()
            }
            unexpected_error => unexpected_error.int_err().api_err(),
        })?;

    // TODO: Private Datasets: Use the real owner_id, not the alias name
    //       Context: In the case of single-tenant, we have None
    let account_id = if let Some(account_name) = dataset_handle.alias.account_name.as_ref() {
        let auth_service = catalog.get_one::<dyn AuthenticationService>().unwrap();
        auth_service
            .account_by_name(account_name)
            .await?
            .map(|account| account.id)
    } else {
        None
    };

    Ok(Json(DatasetInfoResponse::into_response(
        dataset_handle,
        account_id,
    )))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
