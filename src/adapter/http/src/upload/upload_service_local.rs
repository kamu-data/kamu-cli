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

use dill::*;
use kamu::domain::{ErrorIntoInternal, InternalError, ResultIntoInternal, ServerUrlConfig};
use opendatafabric::AccountID;
use thiserror::Error;
use tokio::io::AsyncRead;
use uuid::Uuid;

use crate::{UploadContext, UploadService};

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UploadService)]
pub struct UploadServiceLocal {
    server_url_config: Arc<ServerUrlConfig>,
    cache_dir: PathBuf,
}

impl UploadServiceLocal {
    pub fn new(server_url_config: Arc<ServerUrlConfig>, cache_dir: PathBuf) -> Self {
        Self {
            server_url_config,
            cache_dir,
        }
    }

    fn make_account_folder_path(&self, account_id: &AccountID) -> PathBuf {
        self.cache_dir
            .join("uploads")
            .join(account_id.as_multibase().to_string())
    }

    async fn copy_stream_to_file(
        mut data: Box<dyn AsyncRead + Send + Unpin>,
        target_path: &Path,
    ) -> Result<(), std::io::Error> {
        let mut file = tokio::fs::File::create(target_path).await?;
        tokio::io::copy(&mut data, &mut file).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceLocal {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError> {
        let upload_id = Uuid::new_v4().simple().to_string();

        let upload_folder_path = self
            .make_account_folder_path(account_id)
            .join(upload_id.clone());
        std::fs::create_dir_all(upload_folder_path).int_err()?;

        let context = UploadContext {
            upload_url: format!(
                "{}platform/file/upload/{}/{}",
                self.server_url_config.protocols.base_url_rest, upload_id, file_name
            ),
            method: "POST".to_string(),
            fields: vec![],
        };
        Ok(context)
    }

    async fn save_upload(
        &self,
        account_id: &AccountID,
        upload_id: String,
        file_name: String,
        file_data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), InternalError> {
        let upload_folder_path = self
            .make_account_folder_path(account_id)
            .join(upload_id.clone());
        if !upload_folder_path.is_dir() {
            return Err(SaveUploadError {
                reason: String::from("Upload folder does not exists"),
            }
            .int_err());
        }

        let file_path = upload_folder_path.join(file_name);
        Self::copy_stream_to_file(file_data, &file_path)
            .await
            .map_err(|e| {
                tracing::error!("{:?}", e);
                e.int_err()
            })?;

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Failed to save uploaded file: {reason}")]
struct SaveUploadError {
    pub reason: String,
}

///////////////////////////////////////////////////////////////////////////////
