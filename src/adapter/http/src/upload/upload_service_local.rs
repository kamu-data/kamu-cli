// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{CacheDir, MediaType, ServerUrlConfig};
use thiserror::Error;
use tokio::io::AsyncRead;
use uuid::Uuid;

use super::{ContentNotFoundError, UploadToken, UploadTokenBase64Json, UploadTokenIntoStreamError};
use crate::{
    AccessToken,
    ContentLengthMismatchError,
    ContentTooLargeError,
    FileUploadLimitConfig,
    MakeUploadContextError,
    SaveUploadError,
    UploadContext,
    UploadService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn UploadService)]
pub struct UploadServiceLocal {
    server_url_config: Arc<ServerUrlConfig>,
    uploads_config: Arc<FileUploadLimitConfig>,
    maybe_access_token: Option<Arc<AccessToken>>,
    cache_dir: Arc<CacheDir>,
}

impl UploadServiceLocal {
    fn make_account_folder_path(&self, account_id_str: &str) -> PathBuf {
        self.cache_dir.join("uploads").join(account_id_str)
    }

    async fn file_to_stream(
        path: &Path,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, InternalError> {
        let file = tokio::fs::File::open(path).await.int_err()?;
        Ok(Box::new(file))
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceLocal {
    async fn make_upload_context(
        &self,
        owner_account_id: &odf::AccountID,
        file_name: String,
        content_type: Option<MediaType>,
        content_length: usize,
    ) -> Result<UploadContext, MakeUploadContextError> {
        assert!(self.maybe_access_token.is_some());

        if content_length > self.uploads_config.max_file_size_in_bytes() {
            return Err(MakeUploadContextError::TooLarge(ContentTooLargeError {}));
        }

        let upload_id = Uuid::new_v4().simple().to_string();

        let owner_account_id_mb = owner_account_id.as_multibase().to_stack_string();

        let upload_folder_path = self
            .make_account_folder_path(owner_account_id_mb.as_str())
            .join(upload_id.clone());
        std::fs::create_dir_all(upload_folder_path).map_err(ErrorIntoInternal::int_err)?;

        let upload_token = UploadTokenBase64Json(UploadToken {
            upload_id,
            file_name,
            owner_account_id: owner_account_id_mb.to_string(),
            content_length,
            content_type,
        });

        let upload_url = format!(
            "{}platform/file/upload/{upload_token}",
            self.server_url_config.protocols.base_url_rest,
        );

        let context = UploadContext {
            upload_url,
            upload_token,
            method: "POST".to_string(),
            use_multipart: true,
            headers: vec![(
                String::from("Authorization"),
                format!(
                    "Bearer {}",
                    self.maybe_access_token
                        .as_ref()
                        .expect("access token must be present")
                        .token
                ),
            )],
            fields: vec![],
        };
        Ok(context)
    }

    async fn upload_reference_size(
        &self,
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<usize, UploadTokenIntoStreamError> {
        let upload_file_path = self
            .make_account_folder_path(owner_account_id.as_multibase().to_stack_string().as_str())
            .join(upload_id)
            .join(file_name);

        let metadata = tokio::fs::metadata(upload_file_path).await.map_err(|e| {
            tracing::error!(error=?e, "Reading upload file failed");
            UploadTokenIntoStreamError::ContentNotFound(ContentNotFoundError {})
        })?;

        Ok(usize::try_from(metadata.len()).unwrap())
    }

    async fn upload_reference_into_stream(
        &self,
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, UploadTokenIntoStreamError> {
        let upload_file_path = self
            .make_account_folder_path(owner_account_id.as_multibase().to_stack_string().as_str())
            .join(upload_id)
            .join(file_name);

        UploadServiceLocal::file_to_stream(&upload_file_path)
            .await
            .map_err(|e| {
                tracing::error!(error=?e, "Reading upload file failed");
                UploadTokenIntoStreamError::ContentNotFound(ContentNotFoundError {})
            })
    }

    async fn save_upload(
        &self,
        upload_token: &UploadToken,
        content_length: usize,
        file_data: Bytes,
    ) -> Result<(), SaveUploadError> {
        assert!(self.maybe_access_token.is_some());

        if content_length > self.uploads_config.max_file_size_in_bytes() {
            return Err(SaveUploadError::TooLarge(ContentTooLargeError {}));
        }

        if content_length != upload_token.content_length {
            return Err(SaveUploadError::ContentLengthMismatch(
                ContentLengthMismatchError {
                    declared: upload_token.content_length,
                    actual: content_length,
                },
            ));
        }

        let upload_folder_path = self
            .make_account_folder_path(&upload_token.owner_account_id)
            .join(upload_token.upload_id.clone());
        if !upload_folder_path.is_dir() {
            return Err(SaveUploadError::Internal(
                SaveUploadFailure {
                    reason: String::from("Upload folder does not exists"),
                }
                .int_err(),
            ));
        }

        let file_path = upload_folder_path.join(&upload_token.file_name);
        tokio::fs::write(&file_path, file_data).await.map_err(|e| {
            tracing::error!(
                error = ?e,
                error_msg = %e,
                file_path = %file_path.display(),
                "Writing file failed"
            );
            SaveUploadError::Internal(e.int_err())
        })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Failed to save uploaded file: {reason}")]
struct SaveUploadFailure {
    pub reason: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
