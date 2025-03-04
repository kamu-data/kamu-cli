// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use tokio::sync::OnceCell;

use crate::prelude::*;
use crate::queries::*;
use crate::utils::{ensure_dataset_env_vars_enabled, get_dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Dataset {
    owner: Account,
    dataset_handle: odf::DatasetHandle,
    resolved_dataset: OnceCell<kamu_core::ResolvedDataset>,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            owner,
            dataset_handle,
            resolved_dataset: OnceCell::new(),
        }
    }

    #[graphql(skip)]
    pub fn from_resolved_dataset(
        owner: Account,
        resolved_dataset: kamu_core::ResolvedDataset,
    ) -> Self {
        Self {
            owner,
            dataset_handle: resolved_dataset.get_handle().clone(),
            resolved_dataset: OnceCell::new_with(Some(resolved_dataset)),
        }
    }

    #[graphql(skip)]
    pub async fn try_from_ref(
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<TransformInputDataset> {
        let view_dataset_use_case = from_catalog_n!(ctx, dyn kamu_datasets::ViewDatasetUseCase);

        let handle = match view_dataset_use_case.execute(dataset_ref).await {
            Ok(handle) => Ok(handle),
            Err(e) => match e {
                kamu_datasets::ViewDatasetUseCaseError::Access(_) => {
                    return Ok(TransformInputDataset::not_accessible(dataset_ref.clone()))
                }
                unexpected_error => Err(unexpected_error.int_err()),
            },
        }?;

        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");
        let dataset = Dataset::new(account, handle);

        Ok(TransformInputDataset::accessible(dataset))
    }

    #[graphql(skip)]
    async fn resolve_dataset(&self, ctx: &Context<'_>) -> Result<kamu_core::ResolvedDataset> {
        let resolved_dataset = get_dataset(ctx, &self.dataset_handle).await;
        Ok(resolved_dataset)
    }

    #[graphql(skip)]
    #[inline]
    async fn get_resolved_dataset(&self, ctx: &Context<'_>) -> Result<&kamu_core::ResolvedDataset> {
        self.resolved_dataset
            .get_or_try_init(|| self.resolve_dataset(ctx))
            .await
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        (&self.dataset_handle.id).into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use
    /// `id()`.
    async fn name(&self) -> DatasetName {
        (&self.dataset_handle.alias.dataset_name).into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        &self.owner
    }

    /// Returns dataset alias (user + name)
    async fn alias(&self) -> DatasetAlias {
        (&self.dataset_handle.alias).into()
    }

    /// Returns the kind of dataset (Root or Derivative)
    #[tracing::instrument(level = "info", name = Dataset_kind, skip_all)]
    async fn kind(&self, ctx: &Context<'_>) -> Result<DatasetKind> {
        let resolved_dataset = self.get_resolved_dataset(ctx).await?;
        let summary = resolved_dataset
            .get_summary(odf::dataset::GetSummaryOpts::default())
            .await
            .int_err()?;
        Ok(summary.kind.into())
    }

    /// Returns the visibility of dataset
    #[tracing::instrument(level = "info", name = Dataset_visibility, skip_all)]
    async fn visibility(&self, ctx: &Context<'_>) -> Result<DatasetVisibilityOutput> {
        let rebac_svc = from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacService);

        let resolved_dataset = self.get_resolved_dataset(ctx).await?;
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
    async fn data(&self, ctx: &Context<'_>) -> Result<DatasetData> {
        let resolved_dataset = self.get_resolved_dataset(ctx).await?;
        Ok(DatasetData::new(resolved_dataset.clone()))
    }

    /// Access to the metadata of the dataset
    async fn metadata(&self, ctx: &Context<'_>) -> Result<DatasetMetadata> {
        let resolved_dataset = self.get_resolved_dataset(ctx).await?;
        Ok(DatasetMetadata::new(resolved_dataset.clone()))
    }

    /// Access to the environment variable of this dataset
    #[expect(clippy::unused_async)]
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVars> {
        ensure_dataset_env_vars_enabled(ctx)?;
        Ok(DatasetEnvVars::new(self.dataset_handle.id.clone()))
    }

    /// Access to the flow configurations of this dataset
    async fn flows(&self) -> DatasetFlows {
        DatasetFlows::new(self.dataset_handle.clone())
    }

    // TODO: PERF: Avoid traversing the entire chain
    /// Creation time of the first metadata block in the chain
    #[tracing::instrument(level = "info", name = Dataset_created_at, skip_all)]
    async fn created_at(&self, ctx: &Context<'_>) -> Result<DateTime<Utc>> {
        let resolved_dataset = self.get_resolved_dataset(ctx).await?;

        use odf::dataset::MetadataChainExt as _;
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
        let resolved_dataset = self.get_resolved_dataset(ctx).await?;

        use odf::dataset::MetadataChainExt as __;
        Ok(resolved_dataset
            .as_metadata_chain()
            .get_block_by_ref(&odf::BlockRef::Head)
            .await?
            .system_time)
    }

    /// Permissions of the current user
    #[tracing::instrument(level = "info", name = Dataset_permissions, skip_all)]
    async fn permissions(&self, ctx: &Context<'_>) -> Result<DatasetPermissions> {
        use kamu_core::auth;

        let dataset_action_authorizer = from_catalog_n!(ctx, dyn auth::DatasetActionAuthorizer);

        let allowed_actions = dataset_action_authorizer
            .get_allowed_actions(&self.dataset_handle.id)
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
        let config = crate::utils::unsafe_from_catalog_n!(ctx, kamu_core::ServerUrlConfig);

        DatasetEndpoints::new(&self.owner, self.dataset_handle.clone(), config)
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
