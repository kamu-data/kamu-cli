// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::IntoResponse;
use database_common_macros::transactional_handler;
use file_utils::MediaType;
use http_common::{ApiError, IntoApiError, ResultIntoApiError};
use internal_error::ResultIntoInternal;
use kamu_core::services::download_service::{DecodeDownloadTokenError, FileDownloadService};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use thiserror::Error;

use crate::axum_utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct DownloadFromPath {
    /// Signed token from a file's content URL
    download_token: String,
}

/// Download a dataset file by a signed token
#[utoipa::path(
    get,
    path = "/file/download/{download_token}",
    params(DownloadFromPath),
    responses(
        (status = OK, description = "file content", content_type = "application/octet-stream", body = ()),
        (status = UNAUTHORIZED, description = "token has expired"),
        (status = FORBIDDEN, description = "token is invalid"),
        (status = NOT_FOUND, description = "dataset or file not found"),
    ),
    tag = "kamu",
    security(
        (),  // Note: the token in the path is the credential
    )
)]
#[transactional_handler]
pub async fn file_download_get_handler(
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(download_param): axum::extract::Path<DownloadFromPath>,
) -> Result<impl IntoResponse, ApiError> {
    let (download_service, dataset_registry) =
        from_catalog_n!(catalog, dyn FileDownloadService, dyn DatasetRegistry);

    let file = download_service
        .decode_download_token(&download_param.download_token)
        .map_err(|e| match e {
            DecodeDownloadTokenError::Invalid(_) => ApiError::new_forbidden(),
            DecodeDownloadTokenError::Expired(e) => ApiError::new_unauthorized_from(e),
        })?;

    let Some(dataset) = dataset_registry
        .try_get_dataset_by_ref(&file.dataset_id.as_local_ref())
        .await
        .api_err()?
    else {
        return Err(ApiError::not_found(DownloadDatasetNotFoundError {}));
    };

    let stream = match dataset.as_data_repo().get_stream(&file.content_hash).await {
        Ok(stream) => stream,
        Err(e @ odf::storage::GetError::NotFound(_)) => return Err(ApiError::not_found(e)),
        Err(e) => return Err(e.api_err()),
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&file.content_type.0)
            .int_err()
            .api_err()?,
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(file.content_length),
    );
    // The file comes from a user and is served from the node's origin, which
    // the Web UI may share: its scripts must not run with that origin
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    if !file.content_type.has_essence(MediaType::PDF) {
        // Browsers do not render a PDF in a sandboxed document. Their PDF
        // viewers do not run scripts with the page's origin anyway
        headers.insert(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static("sandbox"),
        );
    }

    Ok((
        headers,
        axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(stream)),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset not found")]
struct DownloadDatasetNotFoundError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
