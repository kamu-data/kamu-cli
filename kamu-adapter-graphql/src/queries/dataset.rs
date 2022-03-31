// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use chrono::prelude::*;
use kamu::domain;
use opendatafabric as odf;

#[derive(Debug, Clone)]
pub struct Dataset {
    account: Account,
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new(account: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            account,
            dataset_handle,
        }
    }

    #[graphql(skip)]
    pub fn from_ref(ctx: &Context<'_>, dataset_ref: &odf::DatasetRefLocal) -> Result<Dataset> {
        let cat = ctx.data::<dill::Catalog>().unwrap();
        let dataset_reg = cat.get_one::<dyn domain::DatasetRegistry>().unwrap();

        // TODO: Should we resolve reference at this point or allow unresolved and fail later?
        let hdl = dataset_reg.resolve_dataset_ref(dataset_ref)?;
        Ok(Dataset::new(Account::mock(), hdl))
    }

    #[graphql(skip)]
    fn get_chain(&self, ctx: &Context<'_>) -> Result<Box<dyn domain::MetadataChain>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        Ok(dataset_reg.get_metadata_chain(&self.dataset_handle.as_local_ref())?)
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        self.dataset_handle.id.clone().into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use `id()`.
    async fn name(&self) -> DatasetName {
        self.dataset_handle.name.clone().into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        &self.account
    }

    /// Returns the kind of a dataset (Root or Derivative)
    async fn kind(&self, ctx: &Context<'_>) -> Result<DatasetKind> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        let summary = dataset_reg.get_summary(&self.dataset_handle.as_local_ref())?;
        Ok(summary.kind.into())
    }

    /// Access to the data of the dataset
    async fn data(&self) -> DatasetData {
        DatasetData::new(self.dataset_handle.clone())
    }

    /// Access to the metadata of the dataset
    async fn metadata(&self) -> DatasetMetadata {
        DatasetMetadata::new(self.dataset_handle.clone())
    }

    // TODO: Performance
    /// Creation time of the first metadata block in the chain
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let chain = self.get_chain(ctx)?;
        let first_block = chain
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map(|(_, b)| b)
            .last()
            .expect("Dataset without blocks");
        Ok(first_block.system_time)
    }

    /// Creation time of the most recent metadata block in the chain
    async fn last_updated_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let chain = self.get_chain(ctx)?;
        let last_block = chain
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map(|(_, b)| b)
            .next()
            .expect("Dataset without blocks");
        Ok(last_block.system_time)
    }
}
