// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::*;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRepositoryWriter: Sync + Send {
    async fn create_dataset(
        &self,
        dataset_alias: &DatasetAlias,
        seed_block: MetadataBlockTyped<Seed>,
    ) -> Result<CreateDatasetResult, CreateDatasetError>;

    async fn create_dataset_from_snapshot(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetFromSnapshotResult, CreateDatasetFromSnapshotError>;

    async fn rename_dataset(
        &self,
        dataset_ref: &DatasetRef,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError>;

    async fn delete_dataset(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
