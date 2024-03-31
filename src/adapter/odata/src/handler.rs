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

use datafusion_odata::collection::QueryParamsRaw;
use dill::Catalog;
use kamu_core::*;
use opendatafabric::*;

use crate::context::*;

///////////////////////////////////////////////////////////////////////////////
// Handlers
// TODO: Replace these variations with middleware
///////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler_st(
    catalog: axum::extract::Extension<Catalog>,
) -> axum::response::Response<String> {
    odata_service_handler_common(catalog, None).await
}

pub async fn odata_service_handler_mt(
    catalog: axum::extract::Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<AccountName>,
) -> axum::response::Response<String> {
    odata_service_handler_common(catalog, Some(account_name)).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler_st(
    catalog: axum::extract::Extension<Catalog>,
) -> axum::response::Response<String> {
    odata_metadata_handler_common(catalog, None).await
}

pub async fn odata_metadata_handler_mt(
    catalog: axum::extract::Extension<Catalog>,
    axum::extract::Path(account_name): axum::extract::Path<AccountName>,
) -> axum::response::Response<String> {
    odata_metadata_handler_common(catalog, Some(account_name)).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler_st(
    catalog: axum::extract::Extension<Catalog>,
    axum::extract::Path(dataset_name): axum::extract::Path<DatasetName>,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> axum::response::Response<String> {
    odata_collection_handler_common(catalog, None, dataset_name, headers, query).await
}

pub async fn odata_collection_handler_mt(
    catalog: axum::extract::Extension<Catalog>,
    axum::extract::Path((account_name, dataset_name)): axum::extract::Path<(
        AccountName,
        DatasetName,
    )>,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> axum::response::Response<String> {
    odata_collection_handler_common(catalog, Some(account_name), dataset_name, headers, query).await
}

///////////////////////////////////////////////////////////////////////////////
// Handlers Common
///////////////////////////////////////////////////////////////////////////////

pub async fn odata_service_handler_common(
    axum::extract::Extension(catalog): axum::extract::Extension<Catalog>,
    account_name: Option<AccountName>,
) -> axum::response::Response<String> {
    let ctx = ODataServiceContext::new(catalog, account_name);
    datafusion_odata::handlers::odata_service_handler(axum::Extension(Arc::new(ctx))).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_metadata_handler_common(
    axum::extract::Extension(catalog): axum::extract::Extension<Catalog>,
    account_name: Option<AccountName>,
) -> axum::response::Response<String> {
    let ctx = ODataServiceContext::new(catalog, account_name);
    datafusion_odata::handlers::odata_metadata_handler(axum::Extension(Arc::new(ctx))).await
}

///////////////////////////////////////////////////////////////////////////////

pub async fn odata_collection_handler_common(
    axum::extract::Extension(catalog): axum::extract::Extension<Catalog>,
    account_name: Option<AccountName>,
    dataset_name: DatasetName,
    headers: axum::http::HeaderMap,
    query: axum::extract::Query<QueryParamsRaw>,
) -> axum::response::Response<String> {
    let repo: Arc<dyn DatasetRepository> = catalog.get_one().unwrap();

    let dataset_handle = match repo
        .resolve_dataset_ref(&DatasetAlias::new(account_name, dataset_name).into())
        .await
    {
        Ok(hdl) => Ok(hdl),
        Err(GetDatasetError::NotFound(_)) => {
            return axum::response::Response::builder()
                .status(http::StatusCode::NOT_FOUND)
                .body(Default::default())
                .unwrap()
        }
        Err(e) => Err(e),
    }
    .unwrap();

    let dataset = repo
        .get_dataset(&dataset_handle.as_local_ref())
        .await
        .unwrap();

    let ctx = ODataCollectionContext::new(catalog, dataset_handle, dataset);
    datafusion_odata::handlers::odata_collection_handler(
        axum::Extension(Arc::new(ctx)),
        query,
        headers,
    )
    .await
}
