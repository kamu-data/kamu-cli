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
use opendatafabric::AccountID;
use thiserror::Error;
use tokio::io::AsyncRead;
use uuid::Uuid;

use super::{UploadToken, UploadTokenBase64Json};
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

#[component(pub)]
#[interface(dyn UploadService)]
pub struct UploadServiceLocal {
    server_url_config: Arc<ServerUrlConfig>,
    uploads_config: Arc<FileUploadLimitConfig>,
    access_token: Arc<AccessToken>,
    cache_dir: Arc<CacheDir>,
}

impl UploadServiceLocal {
    pub fn new(
        server_url_config: Arc<ServerUrlConfig>,
        uploads_config: Arc<FileUploadLimitConfig>,
        access_token: Arc<AccessToken>,
        cache_dir: Arc<CacheDir>,
    ) -> Self {
        Self {
            server_url_config,
            uploads_config,
            access_token,
            cache_dir,
        }
    }

    fn make_account_folder_path(&self, account_id: &AccountID) -> PathBuf {
        self.cache_dir
            .join("uploads")
            .join(account_id.as_multibase().to_string())
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
        account_id: &AccountID,
        file_name: String,
        content_type: Option<MediaType>,
        content_length: usize,
    ) -> Result<UploadContext, MakeUploadContextError> {
        if content_length > self.uploads_config.max_file_size_in_bytes() {
            return Err(MakeUploadContextError::TooLarge(ContentTooLargeError {}));
        }

        let upload_id = Uuid::new_v4().simple().to_string();

        let upload_folder_path = self
            .make_account_folder_path(account_id)
            .join(upload_id.clone());
        std::fs::create_dir_all(upload_folder_path)
            .map_err(|e| MakeUploadContextError::Internal(e.int_err()))?;

        let upload_token = UploadTokenBase64Json(UploadToken {
            upload_id,
            file_name,
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
                format!("Bearer {}", self.access_token.token),
            )],
            fields: vec![],
        };
        Ok(context)
    }

    async fn upload_reference_size(
        &self,
        account_id: &AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<usize, InternalError> {
        let upload_file_path = self
            .make_account_folder_path(account_id)
            .join(upload_id)
            .join(file_name);

        let metadata = tokio::fs::metadata(upload_file_path).await.int_err()?;
        Ok(usize::try_from(metadata.len()).unwrap())
    }

    async fn upload_reference_into_stream(
        &self,
        account_id: &AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, InternalError> {
        let upload_file_path = self
            .make_account_folder_path(account_id)
            .join(upload_id)
            .join(file_name);

        UploadServiceLocal::file_to_stream(&upload_file_path).await
    }

    async fn save_upload(
        &self,
        account_id: &AccountID,
        upload_token: &UploadToken,
        content_length: usize,
        file_data: Bytes,
    ) -> Result<(), SaveUploadError> {
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
            .make_account_folder_path(account_id)
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
                error=?e,
                error_msg=%e,
                file_path=file_path.into_os_string().to_str(),
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
