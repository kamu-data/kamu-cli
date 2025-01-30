// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf_metadata::{DatasetAlias, DatasetHandle, DatasetName, MetadataBlockTyped, Seed};

use crate::{CreateDatasetError, CreateDatasetResult, DeleteDatasetError, RenameDatasetError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(any(feature = "testing", test), mockall::automock)]
#[async_trait::async_trait]
pub trait DatasetStorageUnitWriter: Sync + Send {
    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;

    async fn rename_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError>;

    async fn delete_dataset(
        &self,
        dataset_handle: &DatasetHandle,
    ) -> Result<(), DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
