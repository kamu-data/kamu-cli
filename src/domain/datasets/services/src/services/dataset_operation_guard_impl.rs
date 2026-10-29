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
use kamu_accounts::CurrentAccountSubject;
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
    /// call per permission
    async fn classify(
        &self,
        principal: &CurrentAccountSubject,
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
        for (permission, handles) in handles_by_permission {
            let response = self
                .dataset_permission_checker
                .classify_dataset_handles_by_allowance_for(
                    principal,
                    handles.into_values().collect(),
                    permission,
                )
                .await?;

            for handle in response.authorized_handles {
                verdicts.insert((permission, handle.id), Verdict::Allowed);
            }
            for (handle, error) in response.unauthorized_handles_with_errors {
                use ClassifyByAllowanceDatasetPermissionError as E;
                let verdict = match error {
                    E::NotFound(_) => Verdict::NotFound,
                    E::Access(_) => Verdict::Denied,
                    e @ E::Internal(_) => return Err(e.int_err()),
                };
                verdicts.insert((permission, handle.id), verdict);
            }
        }

        Ok(verdicts)
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
        match verdicts.get(&(permission, handle.id.clone())) {
            Some(Verdict::Allowed) => Ok(()),
            Some(Verdict::NotFound) => {
                Err(odf::DatasetNotFoundError::new(handle.as_local_ref()).into())
            }
            Some(Verdict::Denied) => Err(DatasetPermissionError::not_enough_permissions(
                handle.as_local_ref(),
                permission,
            )),
            None => Err(format!(
                "Dataset '{}' was not classified for '{permission}'",
                handle.alias
            )
            .int_err()
            .into()),
        }
    }
}

#[async_trait::async_trait]
impl DatasetOperationGuard for DatasetOperationGuardImpl {
    async fn check_operation(
        &self,
        principal: &CurrentAccountSubject,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetPermissionError> {
        let verdicts = self.classify(principal, slice::from_ref(request)).await?;
        Self::check_request(&verdicts, request)
    }

    async fn check_operations(
        &self,
        principal: &CurrentAccountSubject,
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

/// Outcome of one (permission, dataset) check. Permission checkers deny only for
/// missing permissions, so the error is rebuilt for every request that
/// shares the check.
enum Verdict {
    Allowed,
    NotFound,
    Denied,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
