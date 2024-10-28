// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use http_common::{ApiError, IntoApiError, ResultIntoApiError};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::MediaType;
use serde::de::IntoDeserializer as _;
use serde::Deserialize as _;
use thiserror::Error;

use super::{UploadContext, UploadTokenBase64Json, UploadTokenIntoStreamError};
use crate::axum_utils::ensure_authenticated_account;
use crate::{MakeUploadContextError, SaveUploadError, UploadService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFileUploadQuery {
    file_name: String,

    content_length: usize,

    #[param(value_type = Option<String>)]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    content_type: Option<MediaType>,
}

/// Prepare file upload
#[utoipa::path(
    post,
    path = "/platform/file/upload/prepare",
    params(PlatformFileUploadQuery),
    responses((status = OK, body = UploadContext)),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
pub async fn platform_file_upload_prepare_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Query(query): axum::extract::Query<PlatformFileUploadQuery>,
) -> Result<axum::Json<UploadContext>, ApiError> {
    let account_id = ensure_authenticated_account(&catalog).api_err()?;

    let upload_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_service
        .make_upload_context(
            &account_id,
            query.file_name,
            query.content_type,
            query.content_length,
        )
        .await
    {
        Ok(upload_context) => Ok(axum::Json(upload_context)),
        Err(e) => match e {
            MakeUploadContextError::TooLarge(e) => Err(ApiError::bad_request(e)),
            MakeUploadContextError::Internal(e) => Err(e.api_err()),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
pub struct UploadFromPath {
    upload_token: UploadTokenBase64Json,
}

/// Upload file to temporary storage
#[utoipa::path(
    post,
    path = "/platform/file/upload/{upload_token}",
    request_body = Vec<u8>,
    responses((status = OK, body = UploadContext)),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[allow(clippy::unused_async)]
pub async fn platform_file_upload_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
    mut multipart: axum::extract::Multipart,
) -> Result<(), ApiError> {
    let account_id = ensure_authenticated_account(&catalog).api_err()?;
    if account_id.as_multibase().to_stack_string().as_str()
        != upload_param.upload_token.0.owner_account_id.as_str()
    {
        return Err(ApiError::new_forbidden());
    }

    let file_data = match find_correct_multi_part_field(&mut multipart).await {
        Ok(file_data) => file_data,
        Err(api_error) => return Err(api_error),
    };

    let upload_local_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_local_service
        .save_upload(&upload_param.upload_token.0, file_data.len(), file_data)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => match e {
            SaveUploadError::TooLarge(e) => Err(ApiError::bad_request(e)),
            SaveUploadError::ContentLengthMismatch(e) => Err(ApiError::bad_request(e)),
            SaveUploadError::Internal(e) => Err(e.api_err()),
            SaveUploadError::NotSupported(e) => Err(ApiError::bad_request(e)),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get file from temporary storage
#[utoipa::path(
    get,
    path = "/platform/file/upload/{upload_token}",
    responses((status = OK, body = Vec<u8>)),
    tag = "kamu",
    security(
        (),  // Note: anonymous access is fine to read uploaded files
        ("api_key" = []),
    )
)]
#[allow(clippy::unused_async)]
pub async fn platform_file_upload_get_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let upload_local_service = catalog.get_one::<dyn UploadService>().unwrap();
    let stream = upload_local_service
        .upload_token_into_stream(&upload_param.upload_token.0)
        .await
        .map_err(|e| match e {
            UploadTokenIntoStreamError::ContentNotFound(e) => ApiError::not_found(e),
            UploadTokenIntoStreamError::ContentLengthMismatch(e) => ApiError::bad_request(e),
            UploadTokenIntoStreamError::Internal(e) => e.api_err(),
        })?;

    Ok(axum::body::Body::from_stream(
        tokio_util::io::ReaderStream::new(stream),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn find_correct_multi_part_field(
    multipart: &mut axum::extract::Multipart,
) -> Result<Bytes, ApiError> {
    // Look for the only expected field "file"
    let multipart_field_result = multipart.next_field().await.int_err();
    let file_data = match multipart_field_result {
        Ok(Some(field)) => match field.name() {
            Some("file") => {
                let bytes_res = field.bytes().await;
                match bytes_res {
                    Ok(bytes) => bytes,
                    Err(e) => return Err(e.int_err().api_err()),
                }
            }
            _ => return Err(ApiError::bad_request(ExpectingFilePartOnlyError {})),
        },
        Ok(None) => return Err(ApiError::bad_request(ExpectingFilePartOnlyError {})),
        Err(e) => return Err(e.int_err().api_err()),
    };

    // Check against redundant fields
    let maybe_next_part_field_result = multipart.next_field().await;
    match maybe_next_part_field_result {
        Ok(Some(_)) => return Err(ApiError::bad_request(ExpectingFilePartOnlyError {})),
        Ok(None) => { /* Expected state - no more fields */ }
        Err(e) => return Err(e.int_err().api_err()),
    }

    Ok(file_data)
}

#[derive(Debug, Error)]
#[error("Expected 'file' to be the only part of upload form")]
struct ExpectingFilePartOnlyError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn empty_string_as_none<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => T::deserialize(s.into_deserializer()).map(Some),
    }
}
