// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use kamu_core::{auth, ServerUrlConfig};
use kamu_datasets::{ViewDatasetUseCase, ViewDatasetUseCaseError};

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Dataset {
    dataset_request_state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new_access_checked(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(dataset_handle).with_owner(owner),
        }
    }

    #[graphql(skip)]
    pub async fn from_ref(ctx: &Context<'_>, dataset_ref: &odf::DatasetRef) -> Result<Dataset> {
        let view_dataset_use_case = from_catalog_n!(ctx, dyn ViewDatasetUseCase);

        let handle = view_dataset_use_case.execute(dataset_ref).await.int_err()?;
        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");

        Ok(Dataset::new_access_checked(account, handle))
    }

    #[graphql(skip)]
    pub async fn try_from_ref(
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<TransformInputDataset> {
        let view_dataset_use_case = from_catalog_n!(ctx, dyn ViewDatasetUseCase);

        let handle = match view_dataset_use_case.execute(dataset_ref).await {
            Ok(handle) => Ok(handle),
            Err(e) => match e {
                ViewDatasetUseCaseError::Access(_) => {
                    return Ok(TransformInputDataset::not_accessible(dataset_ref.clone()))
                }
                unexpected_error => Err(unexpected_error.int_err()),
            },
        }?;

        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");
        let dataset = Dataset::new_access_checked(account, handle);

        Ok(TransformInputDataset::accessible(dataset))
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        self.dataset_request_state.dataset_id().into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use
    /// `id()`.
    async fn name(&self) -> DatasetName {
        self.dataset_request_state.dataset_name().into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        self.dataset_request_state.owner()
    }

    /// Returns dataset alias (user + name)
    async fn alias(&self) -> DatasetAlias {
        self.dataset_request_state.dataset_alias().into()
    }

    /// Returns the kind of dataset (Root or Derivative)
    #[tracing::instrument(level = "info", name = Dataset_kind, skip_all)]
    async fn kind(&self, ctx: &Context<'_>) -> Result<DatasetKind> {
        let summary = self.dataset_request_state.dataset_summary(ctx).await?;

        Ok(summary.kind.into())
    }

    /// Returns the visibility of dataset
    #[tracing::instrument(level = "info", name = Dataset_visibility, skip_all)]
    async fn visibility(&self, ctx: &Context<'_>) -> Result<DatasetVisibilityOutput> {
        let rebac_svc = from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacService);

        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        let properties = rebac_svc
            .get_dataset_properties(resolved_dataset.get_id())
            .await
            .int_err()?;

        let visibility = if properties.allows_public_read {
            DatasetVisibilityOutput::public(properties.allows_anonymous_read)
        } else {
            DatasetVisibilityOutput::private()
        };

        Ok(visibility)
    }

    /// Access to the data of the dataset
    async fn data(&self) -> DatasetData {
        DatasetData::new(&self.dataset_request_state)
    }

    /// Access to the metadata of the dataset
    async fn metadata(&self) -> DatasetMetadata {
        DatasetMetadata::new(&self.dataset_request_state)
    }

    /// Access to the environment variable of this dataset
    #[expect(clippy::unused_async)]
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVars> {
        DatasetEnvVars::new(ctx, &self.dataset_request_state)
    }

    /// Access to the flow configurations of this dataset
    async fn flows(&self) -> DatasetFlows {
        DatasetFlows::new(&self.dataset_request_state)
    }

    // TODO: PERF: Avoid traversing the entire chain
    /// Creation time of the first metadata block in the chain
    #[tracing::instrument(level = "info", name = Dataset_created_at, skip_all)]
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        use odf::dataset::MetadataChainExt;
        Ok(resolved_dataset
            .as_metadata_chain()
            .accept_one(odf::dataset::SearchSeedVisitor::new())
            .await
            .int_err()?
            .into_block()
            .expect("Dataset without blocks")
            .system_time)
    }

    /// Creation time of the most recent metadata block in the chain
    #[tracing::instrument(level = "info", name = Dataset_last_updated_at, skip_all)]
    async fn last_updated_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;

        use odf::dataset::MetadataChainExt;
        Ok(resolved_dataset
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await?
            .system_time)
    }

    /// Permissions of the current user
    #[tracing::instrument(level = "info", name = Dataset_permissions, skip_all)]
    async fn permissions(&self, ctx: &Context<'_>) -> Result<DatasetPermissions> {
        let allowed_actions = self
            .dataset_request_state
            .allowed_dataset_actions(ctx)
            .await?;
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
        let config = utils::unsafe_from_catalog_n!(ctx, ServerUrlConfig);

        DatasetEndpoints::new(&self.dataset_request_state, config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, PartialEq, Eq)]
pub enum DatasetVisibilityOutput {
    Private(PrivateDatasetVisibility),
    Public(PublicDatasetVisibility),
}

impl DatasetVisibilityOutput {
    pub fn private() -> Self {
        Self::Private(PrivateDatasetVisibility { _dummy: None })
    }

    pub fn public(anonymous_available: bool) -> Self {
        Self::Public(PublicDatasetVisibility {
            anonymous_available,
        })
    }
}

#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "PrivateDatasetVisibilityInput")]
pub struct PrivateDatasetVisibility {
    _dummy: Option<String>,
}

#[derive(SimpleObject, InputObject, Debug, PartialEq, Eq)]
#[graphql(input_name = "PublicDatasetVisibilityInput")]
pub struct PublicDatasetVisibility {
    pub anonymous_available: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetPermissions {
    can_view: bool,
    can_delete: bool,
    can_rename: bool,
    can_commit: bool,
    can_schedule: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
