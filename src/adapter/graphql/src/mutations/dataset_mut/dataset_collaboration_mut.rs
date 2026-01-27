// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetCollaborationMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetCollaborationMut<'a> {
    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::check_dataset_maintain_access(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    /// Grant account access as the specified role for the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaborationMut_set_role, skip_all)]
    async fn set_role(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
        role: DatasetAccessRole,
    ) -> Result<SetRoleResult> {
        let apply_account_dataset_relations_uc = from_catalog_n!(
            ctx,
            dyn kamu_auth_rebac::ApplyAccountDatasetRelationsUseCase
        );

        apply_account_dataset_relations_uc
            .execute(kamu_auth_rebac::AccountDatasetRelationOperation {
                account_id: Cow::Owned(account_id.into()),
                operation: kamu_auth_rebac::DatasetRoleOperation::Set(role.into()),
                dataset_id: Cow::Borrowed(&self.dataset_request_state.dataset_handle().id),
            })
            .await
            .int_err()?;

        Ok(SetRoleResultSuccess::default().into())
    }

    /// Revoking account accesses for the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaborationMut_unset_roles, skip_all)]
    async fn unset_roles(
        &self,
        ctx: &Context<'_>,
        account_ids: Vec<AccountID<'_>>,
    ) -> Result<UnsetRoleResult> {
        let unset_dataset_accounts_relations_uc = from_catalog_n!(
            ctx,
            dyn kamu_auth_rebac::UnsetDatasetAccountsRelationsUseCase
        );

        let odf_account_ids = account_ids
            .into_iter()
            .map(Into::into)
            .collect::<Vec<odf::AccountID>>();
        let odf_account_ids_refs = odf_account_ids.iter().collect::<Vec<_>>();

        unset_dataset_accounts_relations_uc
            .execute(
                &self.dataset_request_state.dataset_handle().id,
                &odf_account_ids_refs[..],
            )
            .await
            .int_err()?;

        Ok(UnsetRoleResultSuccess::default().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetRoleResult {
    Success(SetRoleResultSuccess),
}

#[derive(SimpleObject, Debug)]
pub struct SetRoleResultSuccess {
    message: String,
}

impl Default for SetRoleResultSuccess {
    fn default() -> Self {
        Self {
            message: "Success".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UnsetRoleResult {
    Success(UnsetRoleResultSuccess),
}

#[derive(SimpleObject, Debug)]
pub struct UnsetRoleResultSuccess {
    message: String,
}

impl Default for UnsetRoleResultSuccess {
    fn default() -> Self {
        Self {
            message: "Success".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
