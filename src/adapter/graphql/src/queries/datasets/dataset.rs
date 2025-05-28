// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{ServerUrlConfig, auth};
use odf::dataset::MetadataChainExt;

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Dataset {
    dataset_request_state: DatasetRequestStateWithOwner,
}

impl Dataset {
    pub fn new_access_checked(owner: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(dataset_handle).with_owner(owner),
        }
    }

    pub async fn try_from_ref(
        ctx: &Context<'_>,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<TransformInputDataset> {
        let rebac_dataset_registry_facade = from_catalog_n!(ctx, dyn RebacDatasetRegistryFacade);

        let resolve_res = rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(dataset_ref, auth::DatasetAction::Read)
            .await;
        let handle = match resolve_res {
            Ok(handle) => Ok(handle),
            Err(e) => {
                use RebacDatasetRefUnresolvedError as E;
                match e {
                    E::Access(_) => {
                        return Ok(TransformInputDataset::not_accessible(dataset_ref.clone()));
                    }
                    e @ (E::NotFound(_) | E::Internal(_)) => Err(e.int_err()),
                }
            }
        }?;

        let account = Account::from_dataset_alias(ctx, &handle.alias)
            .await?
            .expect("Account must exist");
        let dataset = Dataset::new_access_checked(account, handle);

        Ok(TransformInputDataset::accessible(dataset))
    }

    pub fn from_resolved_authorized_dataset(
        owner: Account,
        resolved_dataset: &kamu_core::ResolvedDataset,
    ) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(resolved_dataset.get_handle().clone())
                .with_owner(owner),
        }
    }

    async fn push_source_has_columns(&self, ctx: &Context<'_>, columns: &[&str]) -> Result<bool> {
        let dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        let (source, _merge) = match dataset
            .as_metadata_chain()
            .accept_one(kamu_core::WriterSourceEventVisitor::new(None))
            .await
            .int_err()?
            .get_source_event_and_merge_strategy()
        {
            Ok(src) => src,
            Err(kamu_core::ScanMetadataError::SourceNotFound(_)) => return Ok(false),
            Err(kamu_core::ScanMetadataError::Internal(err)) => return Err(err.into()),
        };

        let Some(odf::MetadataEvent::AddPushSource(source)) = source else {
            return Ok(false);
        };

        let Some(schema_ddl) = source.read.schema() else {
            return Ok(false);
        };

        let push_source_columns: std::collections::BTreeSet<String> = schema_ddl
            .iter()
            .filter_map(|c| c.split_once(' ').map(|s| s.0.to_string()))
            .collect();

        Ok(columns
            .iter()
            .all(|name| push_source_columns.contains(*name)))
    }
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl Dataset {
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
        let resolved_dataset = self.dataset_request_state.resolved_dataset(ctx).await?;
        Ok(resolved_dataset.get_kind().into())
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

    /// Quck access to `head` block hash
    async fn head(&self, ctx: &Context<'_>) -> Result<Multihash<'static>> {
        let head = self
            .dataset_request_state
            .resolved_dataset(ctx)
            .await?
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;

        Ok(head.into())
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
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVars> {
        DatasetEnvVars::new_with_access_check(ctx, &self.dataset_request_state).await
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
        let logged = utils::logged_account(ctx);

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
                can_view: can_maintain,
                can_update: can_maintain,
            },
            flows: DatasetFlowsPermissions {
                can_view: logged && can_read,
                can_run: can_maintain,
            },
            webhooks: DatasetWebhooksPermissions {
                can_view: can_maintain,
                can_update: can_maintain,
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

    /// Current user's role in relation to the dataset
    #[tracing::instrument(level = "info", name = Dataset_role, skip_all)]
    async fn role(&self, ctx: &Context<'_>) -> Result<Option<DatasetAccessRole>> {
        let current_account_subject = from_catalog_n!(ctx, kamu_accounts::CurrentAccountSubject);

        let Some(logged_account_id) = current_account_subject.get_maybe_logged_account_id() else {
            return Ok(None);
        };

        let authorized_accounts = self.dataset_request_state.authorized_accounts(ctx).await?;
        let maybe_current_account_role = authorized_accounts.iter().find_map(|a| {
            if a.account_id == *logged_account_id {
                Some(a.role)
            } else {
                None
            }
        });

        Ok(maybe_current_account_role.map(Into::into))
    }

    /// Access to the dataset collaboration data
    async fn collaboration(&self, ctx: &Context<'_>) -> Result<DatasetCollaboration> {
        DatasetCollaboration::new_with_access_check(ctx, &self.dataset_request_state).await
    }

    /// Access to the dataset's webhooks management functionality
    async fn webhooks(&self) -> DatasetWebhooks {
        DatasetWebhooks::new(&self.dataset_request_state)
    }

    /// Various endpoints for interacting with data
    async fn endpoints(&self, ctx: &Context<'_>) -> DatasetEndpoints<'_> {
        let config = from_catalog_n!(ctx, ServerUrlConfig);

        DatasetEndpoints::new(&self.dataset_request_state, config)
    }

    /// Downcast a dataset to a versioned file interface
    async fn as_versioned_file(&self, ctx: &Context<'_>) -> Result<Option<VersionedFile>> {
        // TODO: Currently guessing whether its OK to cast by push source. Replace with
        // some archetype metadata on ODF layer.
        if !self
            .push_source_has_columns(ctx, &["version", "content_hash"])
            .await?
        {
            return Ok(None);
        }

        Ok(Some(VersionedFile::new(
            self.dataset_request_state
                .resolved_dataset(ctx)
                .await?
                .clone(),
        )))
    }

    /// Downcast a dataset to a collection interface
    async fn as_collection(&self, ctx: &Context<'_>) -> Result<Option<Collection>> {
        // TODO: Currently guessing whether its OK to cast by push source. Replace with
        // some archetype metadata on ODF layer.
        if !self.push_source_has_columns(ctx, &["path", "ref"]).await? {
            return Ok(None);
        }

        Ok(Some(Collection::new(
            self.dataset_request_state
                .resolved_dataset(ctx)
                .await?
                .clone(),
        )))
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
