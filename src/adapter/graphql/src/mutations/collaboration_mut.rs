// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::ApplyAccountDatasetRelationsUseCase;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CollaborationMut;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl CollaborationMut {
    /// Batch application of relations between accounts and datasets
    #[tracing::instrument(level = "info", name = CollaborationMut_apply_account_dataset_relations, skip_all)]
    #[graphql(guard = "LoggedInGuard.and(CanProvisionAccountsGuard)")]
    pub async fn apply_account_dataset_relations(
        &self,
        ctx: &Context<'_>,
        operations: Vec<AccountDatasetRelationOperation>,
    ) -> Result<ApplyRolesMatrixResult> {
        let use_case = from_catalog_n!(ctx, dyn ApplyAccountDatasetRelationsUseCase);

        let operations = operations.iter().map(Into::into).collect::<Vec<_>>();

        use_case.execute_bulk(&operations).await.int_err()?;

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
