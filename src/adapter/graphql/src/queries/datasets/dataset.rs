// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::prelude::*;
use kamu_core::{auth, ResolvedDataset, ServerUrlConfig};
use kamu_datasets::{ViewDatasetUseCase, ViewDatasetUseCaseError};
use tokio::sync::OnceCell;

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Dataset {
    dataset_request_state: DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Dataset {
    #[graphql(skip)]
    pub fn new(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(owner, dataset_handle),
        }
    }

    #[graphql(skip)]
    pub async fn from_ref(ctx: &Context<'_>, dataset_ref: &odf::DatasetRef) -> Result<Dataset> {
        let view_dataset_use_case = from_catalog_n!(ctx, dyn ViewDatasetUseCase);

        let handle = view_dataset_use_case.execute(dataset_ref).await.int_err()?;
        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");

        Ok(Dataset::new(account, handle))
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
        let dataset = Dataset::new(account, handle);

        Ok(TransformInputDataset::accessible(dataset))
    }

    /// Unique identifier of the dataset
    async fn id(&self) -> DatasetID {
        (&self.dataset_request_state.dataset_handle.id).into()
    }

    /// Symbolic name of the dataset.
    /// Name can change over the dataset's lifetime. For unique identifier use
    /// `id()`.
    async fn name(&self) -> DatasetName {
        (&self.dataset_request_state.dataset_handle.alias.dataset_name).into()
    }

    /// Returns the user or organization that owns this dataset
    async fn owner(&self) -> &Account {
        &self.dataset_request_state.owner
    }

    /// Returns dataset alias (user + name)
    async fn alias(&self) -> DatasetAlias {
        (&self.dataset_request_state.dataset_handle.alias).into()
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
        // TODO: Eliminate cloning
        //       GQL: Dataset: cache `ResolvedDataset`
        //       https://github.com/kamu-data/kamu-cli/issues/1114
        DatasetData::new(self.dataset_request_state.dataset_handle.clone())
    }

    /// Access to the metadata of the dataset
    async fn metadata(&self) -> DatasetMetadata {
        // TODO: Eliminate cloning
        //       GQL: Dataset: cache `ResolvedDataset`
        //       https://github.com/kamu-data/kamu-cli/issues/1114
        DatasetMetadata::new(self.dataset_request_state.dataset_handle.clone())
    }

    /// Access to the environment variable of this dataset
    #[expect(clippy::unused_async)]
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVars> {
        utils::ensure_dataset_env_vars_enabled(ctx)?;

        // TODO: Eliminate cloning
        //       GQL: Dataset: cache `ResolvedDataset`
        //       https://github.com/kamu-data/kamu-cli/issues/1114
        Ok(DatasetEnvVars::new(
            self.dataset_request_state.dataset_handle.clone(),
        ))
    }

    /// Access to the flow configurations of this dataset
    async fn flows(&self) -> DatasetFlows {
        // TODO: Eliminate cloning
        //       GQL: Dataset: cache `ResolvedDataset`
        //       https://github.com/kamu-data/kamu-cli/issues/1114
        DatasetFlows::new(self.dataset_request_state.dataset_handle.clone())
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
        let can_maintain = allowed_actions.contains(&auth::DatasetAction::Maintain);
        let is_owner = allowed_actions.contains(&auth::DatasetAction::Own);

        Ok(DatasetPermissions {
            collaboration: DatasetCollaborationPermissions {
                can_view: can_maintain,
                can_update: can_maintain,
            },
            env_vars: DatasetEnvVarsPermissions {
                can_view: can_read,
                can_update: can_maintain,
            },
            flows: DatasetFlowsPermissions {
                can_view: can_read,
                can_run: can_maintain,
            },
            general: DatasetGeneralPermissions {
                can_rename: can_maintain,
                can_set_visibility: can_maintain,
                can_delete: is_owner,
            },
            metadata: DatasetMetadataPermissions {
                can_commit: can_write,
            },
        })
    }

    /// Access to the dataset collaboration data
    async fn collaboration(&self) -> DatasetCollaboration {
        DatasetCollaboration::new(&self.dataset_request_state)
    }

    /// Various endpoints for interacting with data
    async fn endpoints(&self, ctx: &Context<'_>) -> DatasetEndpoints<'_> {
        let config = crate::utils::unsafe_from_catalog_n!(ctx, ServerUrlConfig);

        // TODO: Eliminate cloning
        //       GQL: Dataset: cache `ResolvedDataset`
        //       https://github.com/kamu-data/kamu-cli/issues/1114
        DatasetEndpoints::new(
            &self.dataset_request_state.owner,
            self.dataset_request_state.dataset_handle.clone(),
            config,
        )
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DatasetRequestState {
    owner: Account,
    dataset_handle: odf::DatasetHandle,
    allowed_dataset_actions: OnceCell<HashSet<auth::DatasetAction>>,
    resolved_dataset: OnceCell<ResolvedDataset>,
    dataset_summary: OnceCell<odf::DatasetSummary>,
}

impl DatasetRequestState {
    pub fn new(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            owner,
            dataset_handle,
            allowed_dataset_actions: OnceCell::new(),
            resolved_dataset: OnceCell::new(),
            dataset_summary: OnceCell::new(),
        }
    }

    #[inline]
    pub fn dataset_handle(&self) -> &odf::DatasetHandle {
        &self.dataset_handle
    }

    pub async fn check_dataset_maintain_access(&self, ctx: &Context<'_>) -> Result<()> {
        utils::check_dataset_access(
            ctx,
            &self.allowed_dataset_actions,
            &self.dataset_handle,
            auth::DatasetAction::Maintain,
        )
        .await
    }

    async fn allowed_dataset_actions(
        &self,
        ctx: &Context<'_>,
    ) -> Result<&HashSet<auth::DatasetAction>> {
        utils::get_allowed_dataset_actions(
            ctx,
            &self.allowed_dataset_actions,
            &self.dataset_handle.id,
        )
        .await
    }

    pub async fn resolved_dataset(&self, ctx: &Context<'_>) -> Result<&ResolvedDataset> {
        self.resolved_dataset
            .get_or_try_init(|| async {
                let resolved_dataset = utils::get_dataset(ctx, &self.dataset_handle).await;

                Ok(resolved_dataset)
            })
            .await
    }

    pub async fn dataset_summary(&self, ctx: &Context<'_>) -> Result<&odf::DatasetSummary> {
        self.dataset_summary
            .get_or_try_init(|| async {
                let resolved_dataset = self.resolved_dataset(ctx).await?;

                let summary = resolved_dataset
                    .get_summary(odf::dataset::GetSummaryOpts::default())
                    .await
                    .int_err()?;

                Ok(summary)
            })
            .await
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
    collaboration: DatasetCollaborationPermissions,
    env_vars: DatasetEnvVarsPermissions,
    flows: DatasetFlowsPermissions,
    general: DatasetGeneralPermissions,
    metadata: DatasetMetadataPermissions,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetCollaborationPermissions {
    can_view: bool,
    can_update: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetEnvVarsPermissions {
    can_view: bool,
    can_update: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetFlowsPermissions {
    can_view: bool,
    can_run: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetGeneralPermissions {
    can_rename: bool,
    can_set_visibility: bool,
    can_delete: bool,
}

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetMetadataPermissions {
    can_commit: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
