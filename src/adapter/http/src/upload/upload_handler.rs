// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::Bytes;
use kamu::domain::{ErrorIntoInternal, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use serde_json::{json, Value};
use thiserror::Error;

use crate::api_error::{ApiError, IntoApiError};
use crate::axum_utils::response_for_anonymous_denial;
use crate::{AccessToken, MakeUploadContextError, SaveUploadError, UploadService};

/////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlatformFileUploadQuery {
    file_name: String,
    content_length: usize,
}

pub async fn platform_file_upload_prepare_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Query(query): axum::extract::Query<PlatformFileUploadQuery>,
) -> Result<axum::Json<Value>, ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    let account_id = match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        CurrentAccountSubject::Anonymous(reason) => {
            return Err(response_for_anonymous_denial(*reason));
        }
    };

    let access_token = catalog.get_one::<AccessToken>().unwrap();

    let upload_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_service
        .make_upload_context(
            &account_id,
            query.file_name,
            query.content_length,
            access_token.as_ref(),
        )
        .await
    {
        Ok(upload_context) => Ok(axum::Json(json!(upload_context))),
        Err(e) => match e {
            MakeUploadContextError::TooLarge(e) => Err(ApiError::bad_request(e)),
            MakeUploadContextError::Internal(e) => Err(e.api_err()),
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
pub struct UploadFromPath {
    upload_id: String,
    file_name: String,
}

#[allow(clippy::unused_async)]
pub async fn platform_file_upload_post_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
    axum::extract::Path(upload_param): axum::extract::Path<UploadFromPath>,
    mut multipart: axum::extract::Multipart,
) -> Result<(), ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    let account_id = match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        CurrentAccountSubject::Anonymous(reason) => {
            return Err(response_for_anonymous_denial(*reason))
        }
    };

    let file_data = match find_correct_multi_part_field(&mut multipart).await {
        Ok(file_data) => file_data,
        Err(api_error) => return Err(api_error),
    };

    let upload_local_service = catalog.get_one::<dyn UploadService>().unwrap();
    match upload_local_service
        .save_upload(
            &account_id,
            upload_param.upload_id,
            upload_param.file_name,
            file_data.len(),
            file_data,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => match e {
            SaveUploadError::TooLarge(e) => Err(ApiError::bad_request(e)),
            SaveUploadError::Internal(e) => Err(e.api_err()),
            SaveUploadError::NotSupported(e) => Err(ApiError::bad_request(e)),
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////
