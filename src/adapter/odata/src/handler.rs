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
use opendatafabric::*;

use crate::context::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers
// TODO: Replace these variations with middleware
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn odata_service_handler_st(
    Extension(catalog): Extension<Catalog>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_service_handler_common(catalog, None).await
}

#[transactional_handler]
pub async fn odata_service_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_service_handler_common(catalog, Some(account_name)).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn odata_metadata_handler_st(
    Extension(catalog): Extension<Catalog>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_metadata_handler_common(catalog, None).await
}

#[transactional_handler]
pub async fn odata_metadata_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<AccountName>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_metadata_handler_common(catalog, Some(account_name)).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn odata_collection_handler_st(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path(collection_addr): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> Result<axum::response::Response<String>, ApiError> {
    odata_collection_handler_common(catalog, None, collection_addr, headers, query).await
}

#[transactional_handler]
pub async fn odata_collection_handler_mt(
    Extension(catalog): Extension<Catalog>,
    axum::extract::Path((account_name, collection_addr)): axum::extract::Path<(
        AccountName,
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
    account_name: Option<AccountName>,
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
    account_name: Option<AccountName>,
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
    account_name: Option<AccountName>,
    collection_addr: String,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> Result<axum::response::Response<String>, ApiError> {
    let Some(addr) = CollectionAddr::decode(&collection_addr) else {
        return Err(ApiError::not_found_without_body());
    };

    let Ok(dataset_name) = DatasetName::try_from(&addr.name) else {
        return Err(ApiError::not_found_without_body());
    };

    let repo: Arc<dyn DatasetRepository> = catalog.get_one().unwrap();

    let dataset_handle = match repo
        .resolve_dataset_ref(&DatasetAlias::new(account_name, dataset_name).into())
        .await
    {
        Ok(hdl) => Ok(hdl),
        Err(GetDatasetError::NotFound(_)) => {
            return Err(ApiError::not_found_without_body());
        }
        Err(e) => Err(e),
    }
    .unwrap();

    let dataset = repo.get_dataset_by_handle(&dataset_handle);

    let ctx = ODataCollectionContext::new(catalog, addr, dataset_handle, dataset);
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
        ODataError::UnsupportedDataType(err) => ApiError::not_implemented(err),
        ODataError::UnsupportedFeature(err) => ApiError::not_implemented(err),
        ODataError::CollectionNotFound(err) => ApiError::not_found(err),
        ODataError::CollectionAddressNotAssigned(err) => ApiError::not_implemented(err),
        ODataError::KeyColumnNotAssigned(err) => ApiError::not_implemented(err),
        ODataError::Internal(err) => ApiError::new(err, http::StatusCode::INTERNAL_SERVER_ERROR),
    }
}
