// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::infra::DatasetSummary;
use opendatafabric::*;

use url::Url;

pub trait MetadataRepository: Send + Sync {
    // Datasets
    fn resolve_dataset_ref(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<DatasetHandle, DomainError>;

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetHandle> + 's>;

    fn add_dataset(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<(DatasetHandle, Multihash), DomainError>;

    fn add_dataset_from_block(
        &self,
        dataset_name: &DatasetName,
        first_block: MetadataBlock,
    ) -> Result<(DatasetHandle, Multihash), DomainError>;

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetName, Result<(DatasetHandle, Multihash), DomainError>)>;

    fn delete_dataset(&self, dataset_ref: &DatasetRefLocal) -> Result<(), DomainError>;

    // Metadata

    // TODO: Separate mutable and immutable paths
    // See: https://github.com/rust-lang/rfcs/issues/2035
    fn get_metadata_chain(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;

    // Dataset Extras

    fn get_summary(&self, dataset_ref: &DatasetRefLocal) -> Result<DatasetSummary, DomainError>;

    fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRefLocal,
    ) -> Result<Box<dyn RemoteAliases>, DomainError>;

    // Repositories

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's>;

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError>;

    fn add_repository(&self, repo_name: &RepositoryName, url: Url) -> Result<(), DomainError>;

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError>;
}
