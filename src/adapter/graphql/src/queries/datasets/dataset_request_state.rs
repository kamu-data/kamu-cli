// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::ops::Deref;

use fallible_map::FallibleMapExt as _;
use kamu_auth_rebac::AuthorizedAccount;
use kamu_core::{DatasetRegistry, ResolvedDataset, auth};
use odf::dataset::MetadataChainExt as _;
use tokio::sync::OnceCell;

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct DatasetRequestState {
    dataset_handle: odf::DatasetHandle,
    resolved_dataset: OnceCell<ResolvedDataset>,
    dataset_statistics: OnceCell<Box<kamu_datasets::DatasetStatistics>>,
    allowed_dataset_actions: OnceCell<HashSet<auth::DatasetAction>>,
    authorized_accounts: OnceCell<Vec<AuthorizedAccount>>,
    archetype: OnceCell<Option<odf::schema::ext::DatasetArchetype>>,
}

impl DatasetRequestState {
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_handle,
            resolved_dataset: OnceCell::new(),
            dataset_statistics: OnceCell::new(),
            allowed_dataset_actions: OnceCell::new(),
            authorized_accounts: OnceCell::new(),
            archetype: OnceCell::new(),
        }
    }

    pub fn with_owner(self, owner: Account) -> DatasetRequestStateWithOwner {
        DatasetRequestStateWithOwner { inner: self, owner }
    }

    #[inline]
    pub fn dataset_handle(&self) -> &odf::DatasetHandle {
        &self.dataset_handle
    }

    #[inline]
    pub fn dataset_id(&self) -> &odf::DatasetID {
        &self.dataset_handle.id
    }

    #[inline]
    pub fn dataset_name(&self) -> &odf::DatasetName {
        &self.dataset_handle.alias.dataset_name
    }

    #[inline]
    pub fn dataset_alias(&self) -> &odf::DatasetAlias {
        &self.dataset_handle.alias
    }

    #[inline]
    pub fn dataset_kind(&self) -> odf::DatasetKind {
        self.dataset_handle.kind
    }

    pub async fn resolved_dataset(&self, ctx: &Context<'_>) -> Result<&ResolvedDataset> {
        self.resolved_dataset
            .get_or_try_init(|| async {
                let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

                let resolved_dataset = dataset_registry
                    .get_dataset_by_handle(&self.dataset_handle)
                    .await;

                Ok(resolved_dataset)
            })
            .await
    }

    pub async fn dataset_statistics(
        &self,
        ctx: &Context<'_>,
    ) -> Result<&kamu_datasets::DatasetStatistics> {
        let statistics = self
            .dataset_statistics
            .get_or_try_init(async || {
                let dataset_statistics_service =
                    from_catalog_n!(ctx, dyn kamu_datasets::DatasetStatisticsService);

                dataset_statistics_service
                    .get_statistics(self.dataset_id(), &odf::BlockRef::Head)
                    .await
                    .map(Box::new)
            })
            .await?;

        Ok(statistics)
    }

    pub async fn authorized_accounts(&self, ctx: &Context<'_>) -> Result<&Vec<AuthorizedAccount>> {
        self.authorized_accounts
            .get_or_try_init(|| async {
                let rebac_service = from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacService);

                let authorized_accounts = rebac_service
                    .get_authorized_accounts(self.dataset_id())
                    .await
                    .int_err()?;

                Ok(authorized_accounts)
            })
            .await
    }

    pub(crate) async fn allowed_dataset_actions(
        &self,
        ctx: &Context<'_>,
    ) -> Result<&HashSet<auth::DatasetAction>> {
        self.allowed_dataset_actions
            .get_or_try_init(|| async {
                let dataset_action_authorizer =
                    from_catalog_n!(ctx, dyn auth::DatasetActionAuthorizer);

                let allowed_actions = dataset_action_authorizer
                    .get_allowed_actions(&self.dataset_handle.id)
                    .await?;

                Ok(allowed_actions)
            })
            .await
    }

    pub async fn archetype(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<odf::schema::ext::DatasetArchetype>> {
        let archetype = self
            .archetype
            .get_or_try_init::<GqlError, _, _>(async || {
                tracing::debug!(dataset_id = %self.dataset_id(), "Resolving archetype");

                let dataset = self.resolved_dataset(ctx).await?;

                let archetype = dataset
                    .as_metadata_chain()
                    .accept_one(odf::dataset::SearchSetDataSchemaVisitor::new())
                    .await
                    .int_err()?
                    .into_event()
                    .and_then(|e| e.schema)
                    .try_and_then(|schema| {
                        schema
                            .extra
                            .unwrap_or_default()
                            .get::<odf::schema::ext::AttrArchetype>()
                    })
                    .int_err()?
                    .map(|attr| attr.archetype);

                Ok(archetype)
            })
            .await?;

        Ok(*archetype)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct DatasetRequestStateWithOwner {
    inner: DatasetRequestState,
    owner: Account,
}

impl DatasetRequestStateWithOwner {
    #[inline]
    pub fn owner(&self) -> &Account {
        &self.owner
    }
}

impl Deref for DatasetRequestStateWithOwner {
    type Target = DatasetRequestState;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
