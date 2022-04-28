// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use async_trait::async_trait;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetImpl<MetaChain, DataRepo, CheckpointRepo> {
    metadata_chain: MetaChain,
    data_repo: DataRepo,
    checkpoint_repo: CheckpointRepo,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<MetaChain, DataRepo, CheckpointRepo> DatasetImpl<MetaChain, DataRepo, CheckpointRepo>
where
    MetaChain: MetadataChain2 + Sync + Send,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
{
    pub fn new(
        metadata_chain: MetaChain,
        data_repo: DataRepo,
        checkpoint_repo: CheckpointRepo,
    ) -> Self {
        Self {
            metadata_chain,
            data_repo,
            checkpoint_repo,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<MetaChain, DataRepo, CheckpointRepo> Dataset
    for DatasetImpl<MetaChain, DataRepo, CheckpointRepo>
where
    MetaChain: MetadataChain2 + Sync + Send,
    DataRepo: ObjectRepository + Sync + Send,
    CheckpointRepo: ObjectRepository + Sync + Send,
{
    async fn commit(&self, block: MetadataBlock) -> Result<CommitResult, CommitError> {
        todo!()
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain2 {
        &self.metadata_chain
    }

    fn as_data_repo(&self) -> &dyn ObjectRepository {
        &self.data_repo
    }

    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        &self.checkpoint_repo
    }
}
