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
use kamu_accounts::AuthenticationService;
use kamu_core::{DatasetRepository, GetDatasetError};
use opendatafabric::{AccountID, AccountName, DatasetHandle, DatasetID, DatasetName};

use crate::axum_utils::ensure_authenticated_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetOwnerInfo {
    #[schema(value_type = String)]
    pub account_name: AccountName,

    // TODO: This should not be optional. Awaiting dataset repository refactoring.
    #[schema(value_type = Option<String>)]
    pub account_id: Option<AccountID>,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetInfoResponse {
    #[schema(value_type = String)]
    pub id: DatasetID,

    pub owner: Option<DatasetOwnerInfo>,

    #[schema(value_type = String)]
    pub dataset_name: DatasetName,
}

impl DatasetInfoResponse {
    fn into_response(dataset_handle: DatasetHandle, account_id_maybe: Option<AccountID>) -> Self {
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
        ("id", description = "Dataset ID")
    ),
    responses((status = OK, body = DatasetInfoResponse)),
    tag = "kamu",
    security(
        (),
        ("api_key" = [])
    )
)]
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
    // TODO: FIXME: This is incorrect - the endpoint should check permissions to
    // access dataset and not reject non-authed users
    ensure_authenticated_account(catalog).api_err()?;

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_handle = dataset_repo
        .resolve_dataset_ref(&dataset_id.clone().as_local_ref())
        .await
        .map_err(|err| match err {
            GetDatasetError::NotFound(e) => ApiError::not_found(e),
            GetDatasetError::Internal(e) => e.api_err(),
        })?;

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
