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
use futures::TryStreamExt;
use kamu::domain;
use kamu::domain::{MetadataChainExt, TryStreamExtExt};
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
    pub async fn from_ref(
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRefLocal,
    ) -> Result<Dataset> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        // TODO: Should we resolve reference at this point or allow unresolved and fail later?
        let hdl = local_repo.resolve_dataset_ref(dataset_ref).await?;
        Ok(Dataset::new(Account::mock(), hdl))
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = local_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await?;
        Ok(dataset)
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
        let dataset = self.get_dataset(ctx).await?;
        let summary = dataset
            .get_summary(domain::GetSummaryOpts::default())
            .await?;
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

    // TODO: PERF: Avoid traversing the entire chain
    /// Creation time of the first metadata block in the chain
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;
        let seed = dataset
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map_ok(|(_, b)| b)
            .try_last()
            .await?
            .expect("Dataset without blocks");
        Ok(seed.system_time)
    }

    /// Creation time of the most recent metadata block in the chain
    async fn last_updated_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;
        let head = dataset
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .map_ok(|(_, b)| b)
            .try_first()
            .await?
            .expect("Dataset without blocks");
        Ok(head.system_time)
    }
}
