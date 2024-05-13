// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use dill::*;
use kamu::domain::{InternalError, ResultIntoInternal, ServerUrlConfig};
use opendatafabric::AccountID;
use uuid::Uuid;

use crate::{UploadContext, UploadFormField, UploadService};

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
}

#[async_trait::async_trait]
impl UploadService for UploadServiceLocal {
    async fn organize_file_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError> {
        let account_folder_path = self.make_account_folder_path(account_id);
        std::fs::create_dir_all(account_folder_path.clone()).int_err()?;

        let context = UploadContext {
            upload_url: format!(
                "{}/platform/file/upload",
                self.server_url_config.protocols.base_url_rest
            ),
            method: "POST",
            fields: vec![
                UploadFormField {
                    name: "upload-id",
                    value: Uuid::new_v4().simple().to_string(),
                },
                UploadFormField {
                    name: "file-name",
                    value: file_name,
                },
            ],
        };
        Ok(context)
    }
}

///////////////////////////////////////////////////////////////////////////////
