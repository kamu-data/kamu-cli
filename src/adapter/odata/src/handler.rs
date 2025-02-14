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

use std::sync::Arc;

use axum::Extension;
use database_common_macros::transactional_handler;
use datafusion_odata::collection::{CollectionAddr, QueryParamsRaw};
use datafusion_odata::error::ODataError;
use dill::Catalog;
use http_common::ApiError;
use kamu_core::*;
use kamu_datasets::{ViewDatasetUseCase, ViewDatasetUseCaseError};

use crate::context::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers
// TODO: Replace these variations with middleware
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// OData root service description
#[utoipa::path(
    get,
    path = "/",
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_service_handler_st(
    Extension(catalog): Extension<Catalog>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_service_handler_common(catalog, None).await
}

/// OData root service description
#[utoipa::path(
    get,
    path = "/{account_name}",
    params(
        ("account_name" = String, Path, description = "Account name")
    ),
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_service_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<odf::AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_service_handler_common(catalog, Some(account_name)).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// OData service metadata
#[utoipa::path(
    get,
    path = "/$metadata",
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_metadata_handler_st(
    Extension(catalog): Extension<Catalog>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_metadata_handler_common(catalog, None).await
}

/// OData service metadata
#[utoipa::path(
    get,
    path = "/{account_name}/$metadata",
    params(
        ("account_name" = String, Path, description = "Account name")
    ),
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_metadata_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<odf::AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_metadata_handler_common(catalog, Some(account_name)).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// OData collection
#[utoipa::path(
    get,
    path = "/{dataset_name}",
    params(
        ("dataset_name" = String, Path, description = "Dataset name")
    ),
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_collection_handler_st(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(collection_addr): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_collection_handler_common(catalog, None, collection_addr, headers, query).await
}

/// OData collection
#[utoipa::path(
    get,
    path = "/{account_name}/{dataset_name}",
    params(
        ("account_name" = String, Path, description = "Account name"),
        ("dataset_name" = String, Path, description = "Dataset name"),
    ),
    responses((status = OK, body = String)),
    tag = "kamu-odata",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn odata_collection_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path((account_name, collection_addr)): axum::extract::Path<(
        odf::AccountName,
        String,
    )>,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_collection_handler_common(catalog, Some(account_name), collection_addr, headers, query)
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers Common
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler_common(
    catalog: Catalog,
    account_name: Option<odf::AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    let ctx = ODataServiceContext::new(catalog, account_name);
    let response = datafusion_odata::handlers::odata_service_handler(Extension(Arc::new(ctx)))
        .await
        .map_err(map_err)?;

    Ok(response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler_common(
    catalog: Catalog,
    account_name: Option<odf::AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    let ctx = ODataServiceContext::new(catalog, account_name);
    let response = datafusion_odata::handlers::odata_metadata_handler(Extension(Arc::new(ctx)))
        .await
        .map_err(map_err)?;

    Ok(response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler_common(
    catalog: Catalog,
    account_name: Option<odf::AccountName>,
    collection_addr: String,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> Result<axum::response::Response<String>, ApiError> {
    let Some(addr) = CollectionAddr::decode(&collection_addr) else {
        return Err(ApiError::not_found_without_reason());
    };

    let Ok(dataset_name) = odf::DatasetName::try_from(&addr.name) else {
        return Err(ApiError::not_found_without_reason());
    };

    let view_dataset_use_case: Arc<dyn ViewDatasetUseCase> = catalog.get_one().unwrap();

    let requested_dataset_alias = odf::DatasetAlias::new(account_name, dataset_name);
    let dataset_handle = match view_dataset_use_case
        .execute(&requested_dataset_alias.into_local_ref())
        .await
    {
        Ok(hdl) => Ok(hdl),
        Err(ViewDatasetUseCaseError::NotFound(_) | ViewDatasetUseCaseError::Access(_)) => {
            return Err(ApiError::not_found_without_reason());
        }
        Err(e) => Err(e),
    }
    .unwrap();

    let registry: Arc<dyn DatasetRegistry> = catalog.get_one().unwrap();
    let resolved_dataset = registry.get_dataset_by_handle(&dataset_handle).await;

    let ctx = ODataCollectionContext::new(catalog, addr, resolved_dataset);
    let response = datafusion_odata::handlers::odata_collection_handler(
        Extension(Arc::new(ctx)),
        query,
        headers,
    )
    .await
    .map_err(map_err)?;

    Ok(response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_err(err: ODataError) -> ApiError {
    match err {
        ODataError::BadRequest(err) => ApiError::bad_request(err),
        ODataError::FromUtf8Error(err) => ApiError::bad_request(err),
        ODataError::UnsupportedNetProtocol(err) => ApiError::not_implemented(err),
        ODataError::UnsupportedDataType(err) => ApiError::not_implemented(err),
        ODataError::UnsupportedFeature(err) => ApiError::not_implemented(err),
        ODataError::CollectionNotFound(err) => ApiError::not_found(err),
        ODataError::CollectionAddressNotAssigned(err) => ApiError::not_implemented(err),
        ODataError::KeyColumnNotAssigned(err) => ApiError::not_implemented(err),
        ODataError::Internal(err) => ApiError::new(err, http::StatusCode::INTERNAL_SERVER_ERROR),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
