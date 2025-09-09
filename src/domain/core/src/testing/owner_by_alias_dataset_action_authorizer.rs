// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};

use crate::auth::{
    ClassifyByAllowanceIdsResponse,
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
    MultipleDatasetActionUnauthorizedError,
};
use crate::{DatasetRegistry, DatasetRegistryExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetActionAuthorizer)]
pub struct OwnerByAliasDatasetActionAuthorizer {
    maybe_owner: Arc<Option<odf::AccountName>>,
    dataset_registry: Arc<dyn DatasetRegistry>,
}

impl OwnerByAliasDatasetActionAuthorizer {
    fn owns_dataset_by_alias(&self, dataset_alias: &odf::DatasetAlias) -> bool {
        dataset_alias.account_name.as_ref() == (*self.maybe_owner).as_ref()
    }

    async fn owns_dataset_by_id(&self, dataset_id: &odf::DatasetID) -> Result<bool, InternalError> {
        let dataset = self
            .dataset_registry
            .get_dataset_by_ref(&dataset_id.as_local_ref())
            .await
            .int_err()?;

        Ok(self.owns_dataset_by_alias(dataset.get_alias()))
    }
}

#[async_trait::async_trait]
impl DatasetActionAuthorizer for OwnerByAliasDatasetActionAuthorizer {
    async fn check_action_allowed(
        &self,
        dataset_id: &odf::DatasetID,
        action: DatasetAction,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        if self.owns_dataset_by_id(dataset_id).await? {
            Ok(())
        } else {
            Err(DatasetActionUnauthorizedError::not_enough_permissions(
                dataset_id.as_local_ref(),
                action,
            ))
        }
    }

    async fn get_allowed_actions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetAction>, InternalError> {
        let allowed_actions = if self.owns_dataset_by_id(dataset_id).await? {
            HashSet::from([
                DatasetAction::Read,
                DatasetAction::Write,
                DatasetAction::Maintain,
                DatasetAction::Own,
            ])
        } else {
            HashSet::new()
        };

        Ok(allowed_actions)
    }

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _action: DatasetAction,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        Ok(dataset_handles
            .into_iter()
            .filter(|dataset_handle| self.owns_dataset_by_alias(&dataset_handle.alias))
            .collect())
    }

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let (allowed, not_allowed): (Vec<_>, Vec<_>) = dataset_handles
            .into_iter()
            .partition(|dataset_handle| self.owns_dataset_by_alias(&dataset_handle.alias));

        Ok(ClassifyByAllowanceResponse {
            authorized_handles: allowed,
            unauthorized_handles_with_errors: not_allowed
                .into_iter()
                .map(|dataset_handle| {
                    let dataset_ref = dataset_handle.as_local_ref();
                    (
                        dataset_handle,
                        MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                            dataset_ref,
                            action,
                        ),
                    )
                })
                .collect(),
        })
    }

    async fn classify_dataset_ids_by_allowance<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
        action: DatasetAction,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        let mut allowed = Vec::new();
        let mut not_allowed = Vec::new();

        for dataset_id in dataset_ids {
            if self.owns_dataset_by_id(dataset_id).await? {
                allowed.push(dataset_id.as_ref().clone());
            } else {
                let error = MultipleDatasetActionUnauthorizedError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    action,
                );

                not_allowed.push((dataset_id.as_ref().clone(), error));
            }
        }

        Ok(ClassifyByAllowanceIdsResponse {
            authorized_ids: allowed,
            unauthorized_ids_with_errors: not_allowed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
