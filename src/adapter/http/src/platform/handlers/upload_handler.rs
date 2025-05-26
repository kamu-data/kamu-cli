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
use kamu_core::services::upload_service::{
    MakeUploadContextError,
    SaveUploadError,
    UploadContext as UploadContextDto,
    UploadService,
    UploadTokenBase64Json,
    UploadTokenIntoStreamError,
};
use kamu_core::MediaType;
use serde::de::IntoDeserializer as _;
use serde::Deserialize as _;
use thiserror::Error;

use crate::axum_utils::{ensure_authenticated_account, from_catalog_n};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UploadContext {
    pub upload_url: String,
    pub method: String,
    pub use_multipart: bool,
    pub headers: Vec<(String, String)>,
    pub fields: Vec<(String, String)>,

    #[schema(value_type = String)]
    pub upload_token: UploadTokenBase64Json,
}

impl From<UploadContextDto> for UploadContext {
    fn from(value: UploadContextDto) -> Self {
        Self {
            upload_url: value.upload_url,
            method: value.method,
            use_multipart: value.use_multipart,
            headers: value.headers,
            fields: value.fields,
            upload_token: value.upload_token,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFileUploadQuery {
    pub file_name: String,

    pub content_length: usize,

    #[param(value_type = Option<String>)]
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub content_type: Option<MediaType>,
}

/// Prepare file upload
#[utoipa::path(
    post,
    path = "/file/upload/prepare",
    params(PlatformFileUploadQuery),
    responses((status = OK, body = UploadContext)),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
pub async fn file_upload_prepare_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Query(query): axum::extract::Query<PlatformFileUploadQuery>,
) -> Result<axum::Json<UploadContext>, ApiError> {
    let account_id = ensure_authenticated_account(&catalog).api_err()?;

    let upload_service = from_catalog_n!(catalog, dyn UploadService);

    match upload_service
        .make_upload_context(
            &account_id,
            query.file_name,
            query.content_type,
            query.content_length,
        )
        .await
    {
        Ok(upload_context) => Ok(axum::Json(upload_context.into())),
        Err(e) => match e {
            MakeUploadContextError::TooLarge(e) => Err(ApiError::bad_request(e)),
            MakeUploadContextError::Internal(e) => Err(e.api_err()),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct UploadFromPath {
    #[param(value_type = String)]
    upload_token: UploadTokenBase64Json,
}

/// Upload a file to temporary storage
#[utoipa::path(
    post,
    path = "/file/upload/{upload_token}",
    params(UploadFromPath),
    request_body = Vec<u8>,
    responses((status = OK, body = UploadContext)),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[allow(clippy::unused_async)]
pub async fn file_upload_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
    mut multipart: axum::extract::Multipart,
) -> Result<(), ApiError> {
    let account_id = ensure_authenticated_account(&catalog).api_err()?;
    if account_id.to_string() != upload_param.upload_token.0.owner_account_id {
        return Err(ApiError::new_forbidden());
    }

    let file_data = match find_correct_multi_part_field(&mut multipart).await {
        Ok(file_data) => file_data,
        Err(api_error) => return Err(api_error),
    };

    let upload_service = from_catalog_n!(catalog, dyn UploadService);

    match upload_service
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
    path = "/file/upload/{upload_token}",
    params(UploadFromPath),
    responses((status = OK, description = "file content", content_type = "application/octet-stream", body = ())),
    tag = "kamu",
    security(
        (),  // Note: anonymous access is fine to read uploaded files
        ("api_key" = []),
    )
)]
#[allow(clippy::unused_async)]
pub async fn file_upload_get_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let upload_service = from_catalog_n!(catalog, dyn UploadService);

    let stream = upload_service
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
