// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use file_utils::{list_files_recursively, list_relative_files_recursively};
use internal_error::{InternalError, ResultIntoInternal};

use super::{DatasetTransferScope, ServerSideDatasetFixture};
use crate::harness::PROTOCOL_TRANSFER_SUBDIRS;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct S3DatasetFixture {
    s3_context: s3_utils::S3Context,
}

impl S3DatasetFixture {
    pub(crate) fn new(s3_context: s3_utils::S3Context) -> Self {
        Self { s3_context }
    }

    fn server_dataset_prefix(dataset_handle: &odf::DatasetHandle) -> String {
        dataset_handle
            .id
            .as_multibase()
            .to_stack_string()
            .to_string()
    }

    async fn read_server_dataset_head(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<String, InternalError> {
        let head_key = format!("{}/refs/head", Self::server_dataset_prefix(dataset_handle));

        let head_object = self.s3_context.get_object(head_key).await.int_err()?;
        let head_bytes = head_object.body.collect().await.int_err()?.into_bytes();

        String::from_utf8(head_bytes.to_vec()).int_err()
    }

    async fn list_server_dir_files(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dir_name: &str,
    ) -> Result<Vec<PathBuf>, InternalError> {
        let dir_prefix = format!(
            "{}/{dir_name}/",
            Self::server_dataset_prefix(dataset_handle)
        );

        let mut files = self
            .s3_context
            .bucket_list_object_keys(&dir_prefix)
            .await?
            .into_iter()
            .map(|key| PathBuf::from(key.strip_prefix(&dir_prefix).unwrap()))
            .collect::<Vec<_>>();

        files.sort();
        Ok(files)
    }

    fn list_local_dir_files(dir: &Path) -> Vec<PathBuf> {
        list_relative_files_recursively(dir).unwrap()
    }

    fn dataset_root_relative_path(
        dataset_layout: &odf::dataset::DatasetLayout,
        path: &Path,
    ) -> PathBuf {
        path.strip_prefix(&dataset_layout.root_dir)
            .unwrap()
            .to_owned()
    }

    fn list_local_files_for_prefix(
        dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Vec<(PathBuf, PathBuf)> {
        let files = match scope {
            DatasetTransferScope::Full => PROTOCOL_TRANSFER_SUBDIRS
                .into_iter()
                .map(|subdir| dataset_layout.root_dir.join(subdir))
                .filter(|path| path.exists())
                .flat_map(|path| list_files_recursively(&path).unwrap())
                .collect::<Vec<_>>(),
            DatasetTransferScope::DataOnly => {
                if !dataset_layout.data_dir.exists() {
                    Vec::new()
                } else {
                    list_files_recursively(&dataset_layout.data_dir).unwrap()
                }
            }
        };

        files
            .into_iter()
            .map(|path| {
                let rel_path = Self::dataset_root_relative_path(dataset_layout, &path);
                (path, rel_path)
            })
            .collect()
    }

    async fn list_server_keys_for_prefix(
        &self,
        dataset_handle: &odf::DatasetHandle,
        scope: DatasetTransferScope,
    ) -> Result<Vec<String>, InternalError> {
        match scope {
            DatasetTransferScope::Full => {
                let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
                let mut keys = Vec::new();
                for dir_name in PROTOCOL_TRANSFER_SUBDIRS {
                    let dir_prefix = format!("{dataset_prefix}/{dir_name}");
                    let mut dir_keys = self.s3_context.bucket_list_object_keys(&dir_prefix).await?;
                    keys.append(&mut dir_keys);
                }
                Ok(keys)
            }
            DatasetTransferScope::DataOnly => {
                let exact_prefix = format!("{}/data", Self::server_dataset_prefix(dataset_handle));
                let subtree_prefix = format!("{exact_prefix}/");
                let keys = self
                    .s3_context
                    .bucket_list_object_keys(&exact_prefix)
                    .await?;
                Ok(keys
                    .into_iter()
                    .filter(|key| key == &exact_prefix || key.starts_with(&subtree_prefix))
                    .collect())
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl ServerSideDatasetFixture for S3DatasetFixture {
    async fn assert_dataset_in_sync(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
    ) -> Result<(), InternalError> {
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "blocks").await?,
            Self::list_local_dir_files(&client_dataset_layout.blocks_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "refs").await?,
            Self::list_local_dir_files(&client_dataset_layout.refs_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "data").await?,
            Self::list_local_dir_files(&client_dataset_layout.data_dir)
        );
        assert_eq!(
            self.list_server_dir_files(dataset_handle, "checkpoints")
                .await?,
            Self::list_local_dir_files(&client_dataset_layout.checkpoints_dir)
        );
        assert_eq!(
            self.read_server_dataset_head(dataset_handle).await?,
            std::fs::read_to_string(client_dataset_layout.refs_dir.join("head")).unwrap()
        );

        Ok(())
    }

    async fn download_dataset_to(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
        let server_keys = self
            .list_server_keys_for_prefix(dataset_handle, scope)
            .await?;

        for key in server_keys {
            let rel_path = key.strip_prefix(&format!("{dataset_prefix}/")).unwrap();
            let dst_path = client_dataset_layout.root_dir.join(rel_path);

            if let Some(parent_dir) = dst_path.parent() {
                std::fs::create_dir_all(parent_dir).int_err()?;
            }

            let object = self.s3_context.get_object(key).await.int_err()?;
            let data = object.body.collect().await.int_err()?.into_bytes();
            std::fs::write(dst_path, data).int_err()?;
        }

        Ok(())
    }

    async fn upload_dataset_from(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let dataset_prefix = Self::server_dataset_prefix(dataset_handle);
        let local_files = Self::list_local_files_for_prefix(client_dataset_layout, scope);

        for (local_path, rel_path) in local_files {
            let key = format!("{dataset_prefix}/{}", rel_path.to_string_lossy());
            let data = std::fs::read(local_path).int_err()?;
            self.s3_context.put_object(key, &data).await.int_err()?;
        }

        Ok(())
    }

    async fn write_dataset_alias(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dataset_alias: &odf::DatasetAlias,
    ) -> Result<(), InternalError> {
        let key = format!("{}/info/alias", Self::server_dataset_prefix(dataset_handle));
        self.s3_context
            .put_object(key, dataset_alias.to_string().as_bytes())
            .await
            .int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
