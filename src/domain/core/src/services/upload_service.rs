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
use file_utils::MediaType;
use internal_error::{InternalError, ResultIntoInternal};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncRead;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UploadService: Send + Sync {
    async fn make_upload_context(
        &self,
        owner_account_id: &odf::AccountID,
        file_name: String,
        content_type: Option<MediaType>,
        content_length: usize,
    ) -> Result<UploadContext, MakeUploadContextError>;

    async fn upload_reference_size(
        &self,
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<usize, UploadTokenIntoStreamError>;

    async fn upload_reference_into_stream(
        &self,
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, UploadTokenIntoStreamError>;

    async fn save_upload(
        &self,
        upload_token: &UploadToken,
        content_length: usize,
        file_data: Bytes,
    ) -> Result<(), SaveUploadError>;

    async fn upload_token_into_stream(
        &self,
        upload_token: &UploadToken,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, UploadTokenIntoStreamError> {
        let owner_account_id =
            odf::AccountID::parse_id_without_did_prefix(&upload_token.owner_account_id)
                .int_err()?;

        let actual_data_size = self
            .upload_reference_size(
                &owner_account_id,
                &upload_token.upload_id,
                &upload_token.file_name,
            )
            .await?;

        if actual_data_size != upload_token.content_length {
            let e = ContentLengthMismatchError {
                actual: actual_data_size,
                declared: upload_token.content_length,
            };
            return Err(UploadTokenIntoStreamError::ContentLengthMismatch(e));
        }

        let stream = self
            .upload_reference_into_stream(
                &owner_account_id,
                &upload_token.upload_id,
                &upload_token.file_name,
            )
            .await?;

        Ok(stream)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadContext {
    pub upload_url: String,
    pub method: String,
    pub use_multipart: bool,
    pub headers: Vec<(String, String)>,
    pub fields: Vec<(String, String)>,
    pub upload_token: UploadTokenBase64Json,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum MakeUploadContextError {
    #[error(transparent)]
    TooLarge(ContentTooLargeError),
    #[error(transparent)]
    Internal(#[from] InternalError),
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
    Internal(#[from] InternalError),
}

#[derive(Debug, Error)]
pub enum UploadTokenIntoStreamError {
    #[error(transparent)]
    ContentLengthMismatch(ContentLengthMismatchError),
    #[error(transparent)]
    ContentNotFound(ContentNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
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

#[derive(Debug, Error)]
#[error("Uploaded file not found on the server")]
pub struct ContentNotFoundError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FileUploadLimitConfig {
    max_file_size_in_bytes: usize,
}

impl FileUploadLimitConfig {
    #[inline]
    pub fn new_in_bytes(max_file_size_in_bytes: usize) -> Self {
        Self {
            max_file_size_in_bytes,
        }
    }

    #[inline]
    pub fn new_in_mb(max_file_size_in_mb: usize) -> Self {
        Self {
            max_file_size_in_bytes: max_file_size_in_mb * 1024 * 1024,
        }
    }

    #[inline]
    pub fn max_file_size_in_bytes(&self) -> usize {
        self.max_file_size_in_bytes
    }

    #[inline]
    pub fn max_file_size_in_mb(&self) -> usize {
        self.max_file_size_in_bytes / 1024 / 1024
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: SEC: It is currently possible to spoof `content_length` - we should
// consider singing/encrypting a token
#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct UploadToken {
    pub upload_id: String,
    pub file_name: String,
    pub owner_account_id: String,
    pub content_length: usize,
    pub content_type: Option<MediaType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UploadTokenBase64Json(pub UploadToken);

impl std::fmt::Display for UploadTokenBase64Json {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload_json = serde_json::to_string(&self.0).unwrap();
        let payload_b64 = base64::engine::general_purpose::URL_SAFE.encode(payload_json);
        write!(f, "{payload_b64}")
    }
}

impl std::str::FromStr for UploadTokenBase64Json {
    type Err = UploadTokenBase64JsonDecodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let payload_json_bytes = base64::engine::general_purpose::URL_SAFE
            .decode(s)
            .map_err(UploadTokenBase64JsonDecodeError::new)?;
        let payload_json = std::str::from_utf8(&payload_json_bytes)
            .map_err(UploadTokenBase64JsonDecodeError::new)?;
        let upload_token =
            serde_json::from_str(payload_json).map_err(UploadTokenBase64JsonDecodeError::new)?;
        Ok(Self(upload_token))
    }
}

impl Serialize for UploadTokenBase64Json {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let payload_b64 = self.to_string();
        serializer.serialize_str(&payload_b64)
    }
}

struct UploadTokenBase64JsonVisitor;
impl serde::de::Visitor<'_> for UploadTokenBase64JsonVisitor {
    type Value = UploadTokenBase64Json;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a base64-encoded json for UploadToken")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse().map_err(E::custom)
    }
}
impl<'de> Deserialize<'de> for UploadTokenBase64Json {
    fn deserialize<D>(deserializer: D) -> Result<UploadTokenBase64Json, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(UploadTokenBase64JsonVisitor)
    }
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct UploadTokenBase64JsonDecodeError {
    pub message: String,
}

impl UploadTokenBase64JsonDecodeError {
    pub fn new(err: impl std::error::Error) -> Self {
        Self {
            message: err.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
