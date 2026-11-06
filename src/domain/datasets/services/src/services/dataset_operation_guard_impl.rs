// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::iter::once;
use std::slice;
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::Principal;
use kamu_datasets::{
    ClassifyByAllowanceDatasetPermissionError,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermission,
    DatasetPermissionChecker,
    DatasetPermissionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetOperationGuardImpl {
    dataset_permission_checker: Arc<dyn DatasetPermissionChecker>,
}

#[component(pub)]
#[interface(dyn DatasetOperationGuard)]
impl DatasetOperationGuardImpl {
    pub fn new(dataset_permission_checker: Arc<dyn DatasetPermissionChecker>) -> Self {
        Self {
            dataset_permission_checker,
        }
    }

    /// Classifies every distinct (permission, dataset) pair of the requests, one
    /// call per permission, then `Read` of every denied dataset not classified
    /// for it yet, so a denial can tell an unreadable dataset apart
    async fn classify(
        &self,
        principal: &Principal,
        requests: &[DatasetOperationRequest],
    ) -> Result<HashMap<(DatasetPermission, odf::DatasetID), Verdict>, InternalError> {
        let mut handles_by_permission: HashMap<
            DatasetPermission,
            HashMap<odf::DatasetID, odf::DatasetHandle>,
        > = HashMap::new();
        for request in requests {
            let checks = once((request.operation.target_permission(), &request.target)).chain(
                request
                    .operation
                    .read_inputs()
                    .iter()
                    .map(|input| (DatasetPermission::Read, input)),
            );
            for (permission, handle) in checks {
                handles_by_permission
                    .entry(permission)
                    .or_default()
                    .entry(handle.id.clone())
                    .or_insert_with(|| handle.clone());
            }
        }

        let mut verdicts = HashMap::new();
        let mut denied_handles = HashMap::new();
        for (permission, handles) in handles_by_permission {
            self.classify_permission(
                principal,
                permission,
                handles.into_values().collect(),
                &mut verdicts,
                &mut denied_handles,
            )
            .await?;
        }

        let unclassified_for_read = denied_handles
            .into_values()
            .filter(|handle: &odf::DatasetHandle| {
                !verdicts.contains_key(&(DatasetPermission::Read, handle.id.clone()))
            })
            .collect::<Vec<_>>();
        if !unclassified_for_read.is_empty() {
            self.classify_permission(
                principal,
                DatasetPermission::Read,
                unclassified_for_read,
                &mut verdicts,
                &mut HashMap::new(),
            )
            .await?;
        }

        Ok(verdicts)
    }

    async fn classify_permission(
        &self,
        principal: &Principal,
        permission: DatasetPermission,
        handles: Vec<odf::DatasetHandle>,
        verdicts: &mut HashMap<(DatasetPermission, odf::DatasetID), Verdict>,
        denied_handles: &mut HashMap<odf::DatasetID, odf::DatasetHandle>,
    ) -> Result<(), InternalError> {
        let response = self
            .dataset_permission_checker
            .classify_dataset_handles_by_allowance(principal, handles, permission)
            .await?;

        for handle in response.authorized_handles {
            verdicts.insert((permission, handle.id), Verdict::Allowed);
        }
        for (handle, error) in response.unauthorized_handles_with_errors {
            use ClassifyByAllowanceDatasetPermissionError as E;
            let verdict = match error {
                E::NotFound(_) => Verdict::NotFound,
                E::Access(_) => {
                    denied_handles.insert(handle.id.clone(), handle.clone());
                    Verdict::Denied
                }
                e @ E::Internal(_) => return Err(e.int_err()),
            };
            verdicts.insert((permission, handle.id), verdict);
        }

        Ok(())
    }

    fn check_request(
        verdicts: &HashMap<(DatasetPermission, odf::DatasetID), Verdict>,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetPermissionError> {
        Self::check(
            verdicts,
            request.operation.target_permission(),
            &request.target,
        )?;
        for input in request.operation.read_inputs() {
            Self::check(verdicts, DatasetPermission::Read, input)?;
        }
        Ok(())
    }

    fn check(
        verdicts: &HashMap<(DatasetPermission, odf::DatasetID), Verdict>,
        permission: DatasetPermission,
        handle: &odf::DatasetHandle,
    ) -> Result<(), DatasetPermissionError> {
        match Self::verdict(verdicts, permission, handle)? {
            Verdict::Allowed => Ok(()),
            Verdict::NotFound => Err(odf::DatasetNotFoundError::new(handle.as_local_ref()).into()),
            // A dataset the principal may not read is not found for it
            Verdict::Denied => match Self::verdict(verdicts, DatasetPermission::Read, handle)? {
                Verdict::Allowed => Err(DatasetPermissionError::not_enough_permissions(
                    handle.as_local_ref(),
                    permission,
                )),
                Verdict::NotFound | Verdict::Denied => {
                    Err(odf::DatasetNotFoundError::new(handle.as_local_ref()).into())
                }
            },
        }
    }

    fn verdict<'a>(
        verdicts: &'a HashMap<(DatasetPermission, odf::DatasetID), Verdict>,
        permission: DatasetPermission,
        handle: &odf::DatasetHandle,
    ) -> Result<&'a Verdict, InternalError> {
        verdicts
            .get(&(permission, handle.id.clone()))
            .ok_or_else(|| {
                format!(
                    "Dataset '{}' was not classified for '{permission}'",
                    handle.alias
                )
                .int_err()
            })
    }
}

#[async_trait::async_trait]
impl DatasetOperationGuard for DatasetOperationGuardImpl {
    async fn check_operation(
        &self,
        principal: &Principal,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetPermissionError> {
        let verdicts = self.classify(principal, slice::from_ref(request)).await?;
        Self::check_request(&verdicts, request)
    }

    async fn check_operations(
        &self,
        principal: &Principal,
        requests: &[DatasetOperationRequest],
    ) -> Result<Vec<Result<(), DatasetPermissionError>>, InternalError> {
        let verdicts = self.classify(principal, requests).await?;
        Ok(requests
            .iter()
            .map(|request| Self::check_request(&verdicts, request))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of one (permission, dataset) check. No error is stored; `check`
/// rebuilds it per request.
enum Verdict {
    Allowed,
    NotFound,
    Denied,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
