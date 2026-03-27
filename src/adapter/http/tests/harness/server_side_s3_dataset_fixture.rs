// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use file_utils::list_relative_files_recursively;
use internal_error::{InternalError, ResultIntoInternal};
use s3_utils::S3PrefixFiles;

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

    fn list_local_dir_files(dir: &Path) -> Vec<PathBuf> {
        list_relative_files_recursively(dir).unwrap()
    }

    fn scope_subdirs(scope: DatasetTransferScope) -> &'static [&'static str] {
        match scope {
            DatasetTransferScope::Full => &PROTOCOL_TRANSFER_SUBDIRS,
            DatasetTransferScope::DataOnly => &["data"],
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
        let dataset_prefix = Self::server_dataset_prefix(dataset_handle);

        assert_eq!(
            S3PrefixFiles::list_prefix_relative_keys(
                &self.s3_context,
                &format!("{dataset_prefix}/blocks"),
            )
            .await?,
            Self::list_local_dir_files(&client_dataset_layout.blocks_dir)
        );
        assert_eq!(
            S3PrefixFiles::list_prefix_relative_keys(
                &self.s3_context,
                &format!("{dataset_prefix}/refs"),
            )
            .await?,
            Self::list_local_dir_files(&client_dataset_layout.refs_dir)
        );
        assert_eq!(
            S3PrefixFiles::list_prefix_relative_keys(
                &self.s3_context,
                &format!("{dataset_prefix}/data"),
            )
            .await?,
            Self::list_local_dir_files(&client_dataset_layout.data_dir)
        );
        assert_eq!(
            S3PrefixFiles::list_prefix_relative_keys(
                &self.s3_context,
                &format!("{dataset_prefix}/checkpoints"),
            )
            .await?,
            Self::list_local_dir_files(&client_dataset_layout.checkpoints_dir)
        );
        assert_eq!(
            S3PrefixFiles::read_object_as_string(
                &self.s3_context,
                format!("{dataset_prefix}/refs/head"),
            )
            .await?,
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

        for subdir in Self::scope_subdirs(scope) {
            S3PrefixFiles::download_prefix_to_dir(
                &self.s3_context,
                &format!("{dataset_prefix}/{subdir}"),
                &client_dataset_layout.root_dir.join(subdir),
            )
            .await?;
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

        for subdir in Self::scope_subdirs(scope) {
            S3PrefixFiles::upload_dir_to_prefix(
                &self.s3_context,
                &client_dataset_layout.root_dir.join(subdir),
                &format!("{dataset_prefix}/{subdir}"),
            )
            .await?;
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
