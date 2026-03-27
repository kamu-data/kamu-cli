// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub(crate) trait ServerSideDatasetFixture {
    async fn assert_dataset_in_sync(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
    ) -> Result<(), InternalError>;

    async fn download_dataset_to(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError>;

    async fn upload_dataset_from(
        &self,
        dataset_handle: &odf::DatasetHandle,
        client_dataset_layout: &odf::dataset::DatasetLayout,
        scope: DatasetTransferScope,
    ) -> Result<(), InternalError>;

    async fn write_dataset_alias(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dataset_alias: &odf::DatasetAlias,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub(crate) enum DatasetTransferScope {
    Full,
    DataOnly,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
