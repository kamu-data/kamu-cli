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
    ApplyRelationMatrixBatchError,
    ApplyRelationMatrixError,
    DatasetRoleOperation,
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
    RebacDatasetAccountRelationsMessage,
    RebacDatasetPropertiesMessage,
    RebacService,
    SetAccountDatasetRelationsOperation,
    UnsetAccountDatasetRelationsOperation,
};
use kamu_core::auth;
use kamu_core::auth::DatasetAction;
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ApplyAccountDatasetRelationsUseCase)]
pub struct ApplyAccountDatasetRelationsUseCaseImpl {
    rebac_service: Arc<dyn RebacService>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    outbox: Arc<dyn Outbox>,
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

        let mut not_found_dataset_refs = Vec::with_capacity(unauthorized_ids_with_errors.len());
        let mut unauthorized_dataset_refs = Vec::with_capacity(unauthorized_ids_with_errors.len());
        for (dataset_id, e) in unauthorized_ids_with_errors {
            use kamu_core::auth::ClassifyByAllowanceDatasetActionUnauthorizedError as E;

            match e {
                E::NotFound(_) => {
                    not_found_dataset_refs.push(dataset_id.as_local_ref());
                }
                E::Access(_) => {
                    unauthorized_dataset_refs.push(dataset_id.as_local_ref());
                }
                e @ E::Internal(_) => {
                    return Err(ApplyRelationMatrixError::Internal(e.int_err()));
                }
            }
        }

        Err(ApplyRelationMatrixBatchError {
            action: EXPECTED_ACCESS,
            unauthorized_dataset_refs,
            not_found_dataset_refs,
        }
        .into())
    }
}

#[async_trait::async_trait]
impl ApplyAccountDatasetRelationsUseCase for ApplyAccountDatasetRelationsUseCaseImpl {
    async fn execute(
        &self,
        operation: AccountDatasetRelationOperation<'_>,
    ) -> Result<(), ApplyRelationMatrixError> {
        self.access_check(std::slice::from_ref(&operation.dataset_id))
            .await?;

        match operation.operation {
            DatasetRoleOperation::Set(role) => {
                self.rebac_service
                    .set_account_dataset_relation(
                        &operation.account_id,
                        role,
                        &operation.dataset_id,
                    )
                    .await
                    .int_err()?;

                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
                        RebacDatasetAccountRelationsMessage::modified(
                            operation.dataset_id.as_ref().clone(),
                            self.rebac_service
                                .get_authorized_accounts(&operation.dataset_id)
                                .await
                                .int_err()?,
                        ),
                    )
                    .await?;
            }
            DatasetRoleOperation::Unset => {
                self.rebac_service
                    .unset_account_dataset_relations(&[UnsetAccountDatasetRelationsOperation {
                        account_id: Cow::Borrowed(operation.account_id.as_ref()),
                        dataset_id: Cow::Borrowed(operation.dataset_id.as_ref()),
                    }])
                    .await
                    .int_err()?;

                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
                        RebacDatasetPropertiesMessage::deleted(
                            operation.dataset_id.as_ref().clone(),
                        ),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn execute_bulk(
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
        };

        const BATCH_SIZE: usize = 1000;

        let mut upsert_operations = Vec::with_capacity(operations.len());
        let mut delete_operations = Vec::with_capacity(operations.len());

        let mut upserted_dataset_ids = HashSet::new();
        let mut deleted_dataset_ids = HashSet::new();

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
                    upserted_dataset_ids.insert(dataset_id.as_ref().clone());
                }
                DatasetRoleOperation::Unset => {
                    delete_operations.push(UnsetAccountDatasetRelationsOperation {
                        account_id: Cow::Borrowed(account_id.as_ref()),
                        dataset_id: Cow::Borrowed(dataset_id.as_ref()),
                    });
                    deleted_dataset_ids.insert(dataset_id.as_ref().clone());
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

        // If we had upserts, we need to notify about the changes
        if !upserted_dataset_ids.is_empty() {
            let upserted_dataset_ids: Vec<odf::DatasetID> =
                upserted_dataset_ids.into_iter().collect();

            // Bulk load authorized accounts for all upserted datasets
            let mut authorized_accounts_by_dataset = self
                .rebac_service
                .get_authorized_accounts_by_ids(&upserted_dataset_ids)
                .await
                .int_err()?;

            // Send messages per dataset
            for dataset_id in upserted_dataset_ids {
                self.outbox
                    .post_message(
                        MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
                        RebacDatasetAccountRelationsMessage::modified(
                            dataset_id.clone(),
                            authorized_accounts_by_dataset.remove(&dataset_id).unwrap(),
                        ),
                    )
                    .await?;
            }
        }

        // If we had deletions, we need to notify about the changes
        for dataset_id in deleted_dataset_ids {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_RELATIONS_SERVICE,
                    RebacDatasetPropertiesMessage::deleted(dataset_id),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
