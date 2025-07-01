// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use kamu_auth_rebac::RebacApplyRolesMatrixUseCase;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollaborationMut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl CollaborationMut {
    /// Matrix application of dataset roles to specified accounts.
    #[tracing::instrument(level = "info", name = CollaborationMut_apply_roles_matrix, skip_all)]
    #[graphql(guard = "LoggedInGuard.and(CanProvisionAccountsGuard)")]
    pub async fn apply_roles_matrix(
        &self,
        ctx: &Context<'_>,
        account_ids: Vec<AccountID<'_>>,
        datasets_with_role_operations: Vec<DatasetWithRoleOperation>,
    ) -> Result<ApplyRolesMatrixResult> {
        let use_case = from_catalog_n!(ctx, dyn RebacApplyRolesMatrixUseCase);

        let account_ids = account_ids
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&odf::AccountID>>();
        let datasets_with_role_operations = datasets_with_role_operations
            .iter()
            .map(
                |DatasetWithRoleOperation {
                     dataset_id,
                     operation,
                 }| { (Cow::Borrowed(dataset_id.as_ref()), (*operation).into()) },
            )
            .collect::<Vec<_>>();

        use_case
            .execute(&account_ids[..], &datasets_with_role_operations)
            .await
            .int_err()?;

        Ok(ApplyRolesMatrixResult::default())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ApplyRolesMatrixResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct ApplyRolesMatrixResult {
    pub message: String,
}

impl Default for ApplyRolesMatrixResult {
    fn default() -> Self {
        Self {
            message: "Roles applied".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
