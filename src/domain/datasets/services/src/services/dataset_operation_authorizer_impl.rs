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
    ClassifyByAllowanceDatasetActionUnauthorizedError,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetActionUnauthorizedError,
    DatasetOperationAuthorizer,
    DatasetOperationRequest,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetOperationAuthorizerImpl {
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn DatasetOperationAuthorizer)]
impl DatasetOperationAuthorizerImpl {
    pub fn new(dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>) -> Self {
        Self {
            dataset_action_authorizer,
        }
    }

    /// Classifies every distinct (action, dataset) pair of the requests, one
    /// call per action
    async fn classify(
        &self,
        principal: &CurrentAccountSubject,
        requests: &[DatasetOperationRequest],
    ) -> Result<HashMap<(DatasetAction, odf::DatasetID), Verdict>, InternalError> {
        let mut handles_by_action: HashMap<
            DatasetAction,
            HashMap<odf::DatasetID, odf::DatasetHandle>,
        > = HashMap::new();
        for request in requests {
            let checks = once((request.operation.target_action(), &request.target)).chain(
                request
                    .operation
                    .read_inputs()
                    .iter()
                    .map(|input| (DatasetAction::Read, input)),
            );
            for (action, handle) in checks {
                handles_by_action
                    .entry(action)
                    .or_default()
                    .entry(handle.id.clone())
                    .or_insert_with(|| handle.clone());
            }
        }

        let mut verdicts = HashMap::new();
        for (action, handles) in handles_by_action {
            let response = self
                .dataset_action_authorizer
                .classify_dataset_handles_by_allowance_for(
                    principal,
                    handles.into_values().collect(),
                    action,
                )
                .await?;

            for handle in response.authorized_handles {
                verdicts.insert((action, handle.id), Verdict::Allowed);
            }
            for (handle, error) in response.unauthorized_handles_with_errors {
                use ClassifyByAllowanceDatasetActionUnauthorizedError as E;
                let verdict = match error {
                    E::NotFound(_) => Verdict::NotFound,
                    E::Access(_) => Verdict::Denied,
                    e @ E::Internal(_) => return Err(e.int_err()),
                };
                verdicts.insert((action, handle.id), verdict);
            }
        }

        Ok(verdicts)
    }

    fn check_request(
        verdicts: &HashMap<(DatasetAction, odf::DatasetID), Verdict>,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        Self::check(verdicts, request.operation.target_action(), &request.target)?;
        for input in request.operation.read_inputs() {
            Self::check(verdicts, DatasetAction::Read, input)?;
        }
        Ok(())
    }

    fn check(
        verdicts: &HashMap<(DatasetAction, odf::DatasetID), Verdict>,
        action: DatasetAction,
        handle: &odf::DatasetHandle,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        match verdicts.get(&(action, handle.id.clone())) {
            Some(Verdict::Allowed) => Ok(()),
            Some(Verdict::NotFound) => {
                Err(odf::DatasetNotFoundError::new(handle.as_local_ref()).into())
            }
            Some(Verdict::Denied) => Err(DatasetActionUnauthorizedError::not_enough_permissions(
                handle.as_local_ref(),
                action,
            )),
            None => Err(format!(
                "Dataset '{}' was not classified for '{action}'",
                handle.alias
            )
            .int_err()
            .into()),
        }
    }
}

#[async_trait::async_trait]
impl DatasetOperationAuthorizer for DatasetOperationAuthorizerImpl {
    async fn authorize_operation(
        &self,
        principal: &CurrentAccountSubject,
        request: &DatasetOperationRequest,
    ) -> Result<(), DatasetActionUnauthorizedError> {
        let verdicts = self.classify(principal, slice::from_ref(request)).await?;
        Self::check_request(&verdicts, request)
    }

    async fn authorize_operations(
        &self,
        principal: &CurrentAccountSubject,
        requests: &[DatasetOperationRequest],
    ) -> Result<Vec<Result<(), DatasetActionUnauthorizedError>>, InternalError> {
        let verdicts = self.classify(principal, requests).await?;
        Ok(requests
            .iter()
            .map(|request| Self::check_request(&verdicts, request))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of one (action, dataset) check. The authorizers deny only for
/// missing permissions, so the error is rebuilt for every request that
/// shares the check.
enum Verdict {
    Allowed,
    NotFound,
    Denied,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
