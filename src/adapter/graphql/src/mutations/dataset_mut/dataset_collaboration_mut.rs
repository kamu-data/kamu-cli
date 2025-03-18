// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::RebacService;

use crate::mutations::DatasetMutRequestState;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetCollaborationMut<'a> {
    dataset_mut_request_state: &'a DatasetMutRequestState,
}

// TODO: Private Datasets: tests
#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetCollaborationMut<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetMutRequestState) -> Self {
        Self {
            dataset_mut_request_state: dataset_request_state,
        }
    }

    // TODO: Private Datasets: add account not found error
    /// Grant account access as the specified role for the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaborationMut_set_role, skip_all)]
    async fn set_role(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'_>,
        role: DatasetAccessRole,
    ) -> Result<SetRoleResult> {
        self.dataset_mut_request_state
            .check_dataset_maintain_access(ctx)
            .await?;

        let rebac_service = from_catalog_n!(ctx, dyn RebacService);

        rebac_service
            .set_account_dataset_relation(
                &account_id,
                role.into(),
                &self.dataset_mut_request_state.dataset_handle().id,
            )
            .await
            .int_err()?;

        Ok(SetRoleResultSuccess::default().into())
    }

    // TODO: Private Datasets: add account not found error
    /// Revoking account accesses for the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaborationMut_unset_roles, skip_all)]
    async fn unset_roles(
        &self,
        ctx: &Context<'_>,
        account_ids: Vec<AccountID<'_>>,
    ) -> Result<UnsetRoleResult> {
        self.dataset_mut_request_state
            .check_dataset_maintain_access(ctx)
            .await?;

        let rebac_service = from_catalog_n!(ctx, dyn RebacService);

        let odf_account_ids = account_ids
            .into_iter()
            .map(Into::into)
            .collect::<Vec<odf::AccountID>>();
        let odf_account_ids_refs = odf_account_ids.iter().collect::<Vec<_>>();

        rebac_service
            .unset_accounts_dataset_relations(
                &odf_account_ids_refs[..],
                &self.dataset_mut_request_state.dataset_handle().id,
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
