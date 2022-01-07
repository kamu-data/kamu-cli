// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use opendatafabric::*;

pub trait MetadataRepository: Send + Sync {
    fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, DomainError>;

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetHandle> + 's>;

    fn add_dataset(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), DomainError>;

    fn add_dataset_from_blocks(
        &self,
        dataset_name: &DatasetName,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(DatasetHandle, Multihash), DomainError>;

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetName, Result<(DatasetHandle, Multihash), DomainError>)>;

    fn delete_dataset(&self, dataset_ref: &DatasetRefLocal) -> Result<(), DomainError>;

    fn get_metadata_chain(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;

    fn get_summary(&self, dataset_ref: &DatasetRefLocal) -> Result<DatasetSummary, DomainError>;
}
