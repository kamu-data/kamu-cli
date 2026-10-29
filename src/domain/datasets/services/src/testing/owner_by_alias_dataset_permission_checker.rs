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
use kamu_accounts::CurrentAccountSubject;
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetPermissionChecker)]
pub struct OwnerByAliasDatasetPermissionChecker {
    maybe_owner: Arc<Option<odf::AccountName>>,
    dataset_registry: Arc<dyn DatasetRegistry>,
}

impl OwnerByAliasDatasetPermissionChecker {
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

    fn classify_response(
        allowed: Vec<odf::DatasetHandle>,
        not_allowed: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> ClassifyByAllowanceResponse {
        ClassifyByAllowanceResponse {
            authorized_handles: allowed,
            unauthorized_handles_with_errors: not_allowed
                .into_iter()
                .map(|dataset_handle| {
                    let dataset_ref = dataset_handle.as_local_ref();
                    (
                        dataset_handle,
                        ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                            dataset_ref,
                            permission,
                        ),
                    )
                })
                .collect(),
        }
    }
}

#[async_trait::async_trait]
impl DatasetPermissionChecker for OwnerByAliasDatasetPermissionChecker {
    async fn check_permission(
        &self,
        dataset_id: &odf::DatasetID,
        permission: DatasetPermission,
    ) -> Result<(), DatasetPermissionError> {
        if self.owns_dataset_by_id(dataset_id).await? {
            Ok(())
        } else {
            Err(DatasetPermissionError::not_enough_permissions(
                dataset_id.as_local_ref(),
                permission,
            ))
        }
    }

    async fn get_granted_permissions(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashSet<DatasetPermission>, GetGrantedPermissionsError> {
        let granted_permissions = if self.owns_dataset_by_id(dataset_id).await? {
            HashSet::from([
                DatasetPermission::Read,
                DatasetPermission::Write,
                DatasetPermission::Maintain,
                DatasetPermission::Own,
            ])
        } else {
            HashSet::new()
        };

        Ok(granted_permissions)
    }

    async fn filter_datasets_allowing(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        _permission: DatasetPermission,
    ) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        Ok(dataset_handles
            .into_iter()
            .filter(|dataset_handle| self.owns_dataset_by_alias(&dataset_handle.alias))
            .collect())
    }

    async fn classify_dataset_handles_by_allowance(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let (allowed, not_allowed): (Vec<_>, Vec<_>) = dataset_handles
            .into_iter()
            .partition(|dataset_handle| self.owns_dataset_by_alias(&dataset_handle.alias));

        Ok(Self::classify_response(allowed, not_allowed, permission))
    }

    async fn classify_dataset_handles_by_allowance_for(
        &self,
        principal: &CurrentAccountSubject,
        dataset_handles: Vec<odf::DatasetHandle>,
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceResponse, InternalError> {
        let (allowed, not_allowed): (Vec<_>, Vec<_>) =
            dataset_handles.into_iter().partition(|dataset_handle| {
                dataset_handle.alias.account_name.as_ref() == principal.maybe_account_name()
            });

        Ok(Self::classify_response(allowed, not_allowed, permission))
    }

    async fn classify_dataset_ids_by_allowance<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
        permission: DatasetPermission,
    ) -> Result<ClassifyByAllowanceIdsResponse, InternalError> {
        let mut allowed = Vec::new();
        let mut not_allowed = Vec::new();

        for dataset_id in dataset_ids {
            if self.owns_dataset_by_id(dataset_id).await? {
                allowed.push(dataset_id.as_ref().clone());
            } else {
                let error = ClassifyByAllowanceDatasetPermissionError::not_enough_permissions(
                    dataset_id.as_local_ref(),
                    permission,
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
