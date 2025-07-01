// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{
    AccountToDatasetRelation,
    ApplyRelationMatrixError,
    RebacApplyRolesMatrixUseCase,
    RebacService,
};
use kamu_core::auth;
use kamu_core::auth::{DatasetAction, DatasetActionUnauthorizedError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn RebacApplyRolesMatrixUseCase)]
pub struct RebacApplyRolesMatrixUseCaseImpl {
    rebac_service: Arc<dyn RebacService>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
}

impl RebacApplyRolesMatrixUseCaseImpl {
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
impl RebacApplyRolesMatrixUseCase for RebacApplyRolesMatrixUseCaseImpl {
    async fn execute(
        &self,
        account_ids: &[&odf::AccountID],
        datasets_with_maybe_roles: &[(odf::DatasetID, Option<AccountToDatasetRelation>)],
    ) -> Result<(), ApplyRelationMatrixError> {
        {
            let datasets_ids = datasets_with_maybe_roles
                .iter()
                .map(|(id, _)| Cow::Borrowed(id))
                .collect::<Vec<_>>();
            self.access_check(&datasets_ids).await?;
        }

        for (dataset_id, maybe_role) in datasets_with_maybe_roles {
            for account_id in account_ids {
                if let Some(role) = maybe_role {
                    self.rebac_service
                        .set_account_dataset_relation(account_id, *role, dataset_id)
                        .await
                        .int_err()?;
                } else {
                    self.rebac_service
                        .unset_accounts_dataset_relations(account_ids, dataset_id)
                        .await
                        .int_err()?;
                    // For unset, we can apply it for all accounts at once, so we stop iterating
                    // further.
                    break;
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
