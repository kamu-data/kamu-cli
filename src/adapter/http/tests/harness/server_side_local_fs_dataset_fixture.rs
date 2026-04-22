// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io;
use std::path::PathBuf;

use file_utils::copy_dir_contents_recursively;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::testing::DatasetTestHelper;
use odf::dataset::DatasetLayout;

use super::{DatasetTransferScope, ServerSideDatasetFixture};
use crate::harness::PROTOCOL_TRANSFER_SUBDIRS;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct LocalFsDatasetFixture {
    datasets_dir: PathBuf,
}

impl LocalFsDatasetFixture {
    pub(crate) fn new(datasets_dir: PathBuf) -> Self {
        Self { datasets_dir }
    }

    fn copy_dataset_scope(
        src_layout: &DatasetLayout,
        dst_layout: &DatasetLayout,
        scope: DatasetTransferScope,
    ) -> io::Result<()> {
        match scope {
            DatasetTransferScope::Full => {
                // Only copy the protocol-relevant directories. Storage-specific
                // extensions are intentionally ignored.
                for subdir in PROTOCOL_TRANSFER_SUBDIRS {
                    copy_dir_contents_recursively(
                        &src_layout.root_dir.join(subdir),
                        &dst_layout.root_dir.join(subdir),
                    )?;
                }
                Ok(())
            }
            DatasetTransferScope::DataOnly => {
                copy_dir_contents_recursively(&src_layout.data_dir, &dst_layout.data_dir)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl ServerSideDatasetFixture for LocalFsDatasetFixture {
    async fn assert_dataset_in_sync(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
    ) -> Result<(), InternalError> {
        let server_dataset_layout = odf::dataset::DatasetLayout::new(
            self.datasets_dir
                .join(dataset_handle.id.as_multibase().to_stack_string())
                .as_path(),
        );

        DatasetTestHelper::assert_datasets_in_sync(&server_dataset_layout, client_dataset_layout);

        Ok(())
    }

    async fn download_dataset_to(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let server_dataset_layout = odf::dataset::DatasetLayout::new(
            self.datasets_dir
                .join(dataset_handle.id.as_multibase().to_stack_string())
                .as_path(),
        );

        Self::copy_dataset_scope(&server_dataset_layout, client_dataset_layout, scope).int_err()
    }

    async fn upload_dataset_from(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError> {
        let server_dataset_layout = odf::dataset::DatasetLayout::new(
            self.datasets_dir
                .join(dataset_handle.id.as_multibase().to_stack_string())
                .as_path(),
        );

        Self::copy_dataset_scope(client_dataset_layout, &server_dataset_layout, scope).int_err()
    }

    async fn write_dataset_alias(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dataset_alias: &odf::DatasetAlias,
    ) -> Result<(), InternalError> {
        let server_dataset_layout = odf::dataset::DatasetLayout::new(
            self.datasets_dir
                .join(dataset_handle.id.as_multibase().to_stack_string())
                .as_path(),
        );

        write_lfs_dataset_alias(&server_dataset_layout, dataset_alias).await;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn write_lfs_dataset_alias(
    dataset_layout: &DatasetLayout,
    alias: &odf::DatasetAlias,
) {
    if !dataset_layout.info_dir.is_dir() {
        std::fs::create_dir_all(dataset_layout.info_dir.clone()).unwrap();
    }

    use tokio::io::AsyncWriteExt;

    let alias_path = dataset_layout.info_dir.join("alias");
    let mut alias_file = tokio::fs::File::create(alias_path).await.unwrap();

    alias_file
        .write_all(alias.to_string().as_bytes())
        .await
        .unwrap();
    alias_file.flush().await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
