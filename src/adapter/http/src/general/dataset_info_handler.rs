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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::AccountService;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::auth;
use kamu_datasets::DatasetEntryService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetOwnerInfo {
    pub account_name: odf::AccountName,

    // TODO: This should not be optional
    //
    //       REST API: `GET /datasets/{id}`: make the `owner` field mandatory
    //       https://github.com/kamu-data/kamu-cli/issues/1140
    #[schema(value_type = String, required = false)]
    pub account_id: Option<odf::AccountID>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetInfoResponse {
    pub id: odf::DatasetID,

    // TODO: This should not be optional
    //
    //       REST API: `GET /datasets/{id}`: make the `owner` field mandatory
    //       https://github.com/kamu-data/kamu-cli/issues/1140
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = DatasetOwnerInfo, required = false)]
    pub owner: Option<DatasetOwnerInfo>,

    pub dataset_name: odf::DatasetName,
}

impl DatasetInfoResponse {
    fn into_response(
        dataset_handle: odf::DatasetHandle,
        account_id: odf::AccountID,
        account_name: odf::AccountName,
    ) -> Self {
        Self {
            id: dataset_handle.id,
            owner: Some(DatasetOwnerInfo {
                account_name,
                account_id: Some(account_id),
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
    let rebac_dataset_registry_facade =
        catalog.get_one::<dyn RebacDatasetRegistryFacade>().unwrap();

    let dataset_handle = rebac_dataset_registry_facade
        .resolve_dataset_handle_by_ref(&dataset_id.as_local_ref(), auth::DatasetAction::Read)
        .await
        .map_err(|e| {
            use RebacDatasetRefUnresolvedError as E;
            match e {
                E::NotFound(_) | E::Access(_) => ApiError::not_found_without_reason(),
                e @ E::Internal(_) => e.int_err().api_err(),
            }
        })?;

    let dataset_entry_service = catalog.get_one::<dyn DatasetEntryService>().unwrap();
    let account_service = catalog.get_one::<dyn AccountService>().unwrap();

    let dataset_entry = dataset_entry_service
        .get_entry(dataset_id)
        .await
        .int_err()?;
    let owner = account_service
        .get_account_by_id(&dataset_entry.owner_id)
        .await
        .int_err()?;

    Ok(Json(DatasetInfoResponse::into_response(
        dataset_handle,
        owner.id,
        owner.account_name,
    )))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
