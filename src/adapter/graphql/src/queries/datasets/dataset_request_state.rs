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

use kamu_core::{auth, DatasetRegistry, ResolvedDataset};
use tokio::sync::OnceCell;

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DatasetRequestState {
    dataset_handle: odf::DatasetHandle,
    resolved_dataset: OnceCell<ResolvedDataset>,
    allowed_dataset_actions: OnceCell<HashSet<auth::DatasetAction>>,
}

impl DatasetRequestState {
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_handle,
            allowed_dataset_actions: OnceCell::new(),
            resolved_dataset: OnceCell::new(),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
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
