// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use kamu_core::{self as domain, MetadataChainExt, SearchSeedVisitor, ServerUrlConfig};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::*;

#[derive(Debug, Clone)]
pub struct Dataset {
    owner: Account,
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            owner,
            dataset_handle,
        }
    }

    #[graphql(skip)]
    pub async fn from_ref(ctx: &Context<'_>, dataset_ref: &odf::DatasetRef) -> Result<Dataset> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        // TODO: Should we resolve reference at this point or allow unresolved and fail
        // later?
        let hdl = dataset_repo
            .resolve_dataset_ref(dataset_ref)
            .await
            .int_err()?;
        let account = Account::from_dataset_alias(ctx, &hdl.alias)
            .await?
            .expect("Account must exist");
        Ok(Dataset::new(account, hdl))
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        self.dataset_handle.id.clone().into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use
    /// `id()`.
    async fn name(&self) -> DatasetName {
        self.dataset_handle.alias.dataset_name.clone().into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        &self.owner
    }

    /// Returns dataset alias (user + name)
    async fn alias(&self) -> DatasetAlias {
        self.dataset_handle.alias.clone().into()
    }

    /// Returns the kind of dataset (Root or Derivative)
    async fn kind(&self, ctx: &Context<'_>) -> Result<DatasetKind> {
        let dataset = self.get_dataset(ctx).await?;
        let summary = dataset
            .get_summary(domain::GetSummaryOpts::default())
            .await
            .int_err()?;
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

    /// Access to the flow configurations of this dataset
    async fn flows(&self) -> DatasetFlows {
        DatasetFlows::new(self.dataset_handle.clone())
    }

    // TODO: PERF: Avoid traversing the entire chain
    /// Creation time of the first metadata block in the chain
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSeedVisitor::new())
            .await
            .int_err()?
            .into_block()
            .expect("Dataset without blocks")
            .system_time)
    }

    /// Creation time of the most recent metadata block in the chain
    async fn last_updated_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .get_block_by_ref(&domain::BlockRef::Head)
            .await?
            .system_time)
    }

    /// Permissions of the current user
    async fn permissions(&self, ctx: &Context<'_>) -> Result<DatasetPermissions> {
        use kamu_core::auth;
        let dataset_action_authorizer =
            from_catalog::<dyn auth::DatasetActionAuthorizer>(ctx).unwrap();

        let allowed_actions = dataset_action_authorizer
            .get_allowed_actions(&self.dataset_handle)
            .await;
        let can_read = allowed_actions.contains(&auth::DatasetAction::Read);
        let can_write = allowed_actions.contains(&auth::DatasetAction::Write);

        Ok(DatasetPermissions {
            can_view: can_read,
            can_delete: can_write,
            can_rename: can_write,
            can_commit: can_write,
            can_schedule: can_write,
        })
    }

    /// Various endpoints for interacting with data
    async fn endpoints(&self, ctx: &Context<'_>) -> DatasetEndpoints<'_> {
        let config = from_catalog::<ServerUrlConfig>(ctx).unwrap();

        DatasetEndpoints::new(&self.owner, self.dataset_handle.clone(), config)
    }
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DatasetPermissions {
    can_view: bool,
    can_delete: bool,
    can_rename: bool,
    can_commit: bool,
    can_schedule: bool,
}
