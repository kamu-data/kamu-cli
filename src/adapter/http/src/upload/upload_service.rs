// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use base64::Engine;
use bytes::Bytes;
use kamu::domain::{InternalError, ResultIntoInternal};
use opendatafabric::AccountID;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::AccessToken;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UploadService: Send + Sync {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
        content_type: String,
        content_length: usize,
        access_token: &AccessToken,
    ) -> Result<UploadContext, MakeUploadContextError>;

    async fn upload_reference_into_stream(
        &self,
        account_id: &AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, InternalError>;

    async fn save_upload(
        &self,
        account_id: &AccountID,
        upload_token: &str,
        content_length: usize,
        file_data: Bytes,
    ) -> Result<(), SaveUploadError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadContext {
    pub upload_url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub fields: Vec<(String, String)>,
    pub upload_token: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum MakeUploadContextError {
    #[error(transparent)]
    TooLarge(ContentTooLargeError),
    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
pub enum SaveUploadError {
    #[error(transparent)]
    NotSupported(UploadNotSupportedError),
    #[error(transparent)]
    TooLarge(ContentTooLargeError),
    #[error(transparent)]
    ContentLengthMismatch(ContentLengthMismatchError),
    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Content too large")]
pub struct ContentTooLargeError {}

#[derive(Debug, Error)]
#[error("Actual content length {actual} does not match the initially declared length {declared}")]
pub struct ContentLengthMismatchError {
    pub actual: usize,
    pub declared: usize,
}

#[derive(Debug, Error)]
#[error("Direct file uploads are not supported in this environment")]
pub struct UploadNotSupportedError {}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FileUploadLimitConfig {
    pub max_file_size_in_bytes: usize,
}

///////////////////////////////////////////////////////////////////////////////

pub fn make_upload_token(
    upload_id: String,
    file_name: String,
    content_type: String,
    content_length: usize,
) -> String {
    let payload = UploadTokenPayload {
        upload_id,
        file_name,
        content_type,
        content_length,
    };

    let payload_json = json!(payload).to_string();
    base64::engine::general_purpose::STANDARD.encode(payload_json)
}

///////////////////////////////////////////////////////////////////////////////

pub fn decode_upload_token_payload(
    upload_token: &str,
) -> Result<UploadTokenPayload, InternalError> {
    let payload_json_bytes = base64::engine::general_purpose::STANDARD
        .decode(upload_token)
        .int_err()?;
    let payload_json = String::from_utf8(payload_json_bytes).int_err()?;
    serde_json::from_str(&payload_json).int_err()
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadTokenPayload {
    pub upload_id: String,
    pub file_name: String,
    pub content_type: String,
    pub content_length: usize,
}

///////////////////////////////////////////////////////////////////////////////
