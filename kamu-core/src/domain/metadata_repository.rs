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

    fn get_all_datasets<'s>(&'s self) -> Box<dyn Iterator<Item = DatasetIDBuf> + 's>;

    fn add_dataset(&self, snapshot: DatasetSnapshot) -> Result<Sha3_256, DomainError>;

    fn add_dataset_from_block(
        &self,
        dataset_id: &DatasetID,
        first_block: MetadataBlock,
    ) -> Result<Sha3_256, DomainError>;

    fn add_datasets(
        &self,
        snapshots: &mut dyn Iterator<Item = DatasetSnapshot>,
    ) -> Vec<(DatasetIDBuf, Result<Sha3_256, DomainError>)>;

    fn delete_dataset(&self, dataset_id: &DatasetID) -> Result<(), DomainError>;

    // Metadata

    // TODO: Separate mutable and immutable paths
    // See: https://github.com/rust-lang/rfcs/issues/2035
    fn get_metadata_chain(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn MetadataChain>, DomainError>;

    // Dataset Extras

    fn get_summary(&self, dataset_id: &DatasetID) -> Result<DatasetSummary, DomainError>;

    fn get_remote_aliases(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Box<dyn RemoteAliases>, DomainError>;

    // Repositories

    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryBuf> + 's>;

    fn get_repository(&self, repo_id: &RepositoryID) -> Result<Repository, DomainError>;

    fn add_repository(&self, repo_id: &RepositoryID, url: Url) -> Result<(), DomainError>;

    fn delete_repository(&self, repo_id: &RepositoryID) -> Result<(), DomainError>;
}

pub trait DatasetDependencyVisitor {
    fn enter(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain) -> bool;
    fn exit(&mut self, dataset_id: &DatasetID, meta_chain: &dyn MetadataChain);
}
