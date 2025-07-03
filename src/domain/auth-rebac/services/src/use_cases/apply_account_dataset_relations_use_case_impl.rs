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

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{
    AccountDatasetRelationOperation,
    ApplyAccountDatasetRelationsUseCase,
    ApplyRelationMatrixError,
    DatasetRoleOperation,
    RebacService,
    SetAccountDatasetRelationsOperation,
    UnsetAccountDatasetRelationsOperation,
};
use kamu_core::auth;
use kamu_core::auth::{DatasetAction, DatasetActionUnauthorizedError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ApplyAccountDatasetRelationsUseCase)]
pub struct ApplyAccountDatasetRelationsUseCaseImpl {
    rebac_service: Arc<dyn RebacService>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
}

impl ApplyAccountDatasetRelationsUseCaseImpl {
    async fn access_check(
        &self,
        dataset_ids: &[Cow<'_, odf::DatasetID>],
    ) -> Result<(), ApplyRelationMatrixError> {
        const EXPECTED_ACCESS: DatasetAction = DatasetAction::Maintain;

        let unauthorized_ids_with_errors = self
            .dataset_action_authorizer
            .classify_dataset_ids_by_allowance(dataset_ids, EXPECTED_ACCESS)
            .await?
            .unauthorized_ids_with_errors;

        if unauthorized_ids_with_errors.is_empty() {
            return Ok(());
        }

        let mut unauthorized_dataset_refs = Vec::with_capacity(unauthorized_ids_with_errors.len());
        for (dataset_id, e) in unauthorized_ids_with_errors {
            match e {
                DatasetActionUnauthorizedError::Access(_) => {
                    unauthorized_dataset_refs.push(dataset_id.as_local_ref());
                }
                e @ DatasetActionUnauthorizedError::Internal(_) => {
                    return Err(ApplyRelationMatrixError::Internal(e.int_err()));
                }
            }
        }

        Err(ApplyRelationMatrixError::not_enough_permissions(
            unauthorized_dataset_refs,
            EXPECTED_ACCESS,
        ))
    }
}

#[async_trait::async_trait]
impl ApplyAccountDatasetRelationsUseCase for ApplyAccountDatasetRelationsUseCaseImpl {
    async fn execute(
        &self,
        operations: &[AccountDatasetRelationOperation],
    ) -> Result<(), ApplyRelationMatrixError> {
        if operations.is_empty() {
            return Ok(());
        }

        {
            let mut seen = HashSet::new();
            let datasets_ids = operations
                .iter()
                .filter(|op| seen.insert(op.dataset_id.as_ref()))
                .map(|op| Cow::Borrowed(op.dataset_id.as_ref()))
                .collect::<Vec<_>>();

            self.access_check(&datasets_ids).await?;
        }

        const BATCH_SIZE: usize = 1000;

        let mut upsert_operations = Vec::with_capacity(operations.len());
        let mut delete_operations = Vec::with_capacity(operations.len());

        for AccountDatasetRelationOperation {
            account_id,
            operation,
            dataset_id,
        } in operations
        {
            match operation {
                DatasetRoleOperation::Set(role) => {
                    upsert_operations.push(SetAccountDatasetRelationsOperation {
                        account_id: Cow::Borrowed(account_id.as_ref()),
                        relationship: *role,
                        dataset_id: Cow::Borrowed(dataset_id.as_ref()),
                    });
                }
                DatasetRoleOperation::Unset => {
                    delete_operations.push(UnsetAccountDatasetRelationsOperation {
                        account_id: Cow::Borrowed(account_id.as_ref()),
                        dataset_id: Cow::Borrowed(dataset_id.as_ref()),
                    });
                }
            }
        }

        for upsert_chunk in upsert_operations.chunks(BATCH_SIZE) {
            self.rebac_service
                .set_account_dataset_relations(upsert_chunk)
                .await
                .int_err()?;
        }

        for delete_chuck in delete_operations.chunks(BATCH_SIZE) {
            self.rebac_service
                .unset_account_dataset_relations(delete_chuck)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
