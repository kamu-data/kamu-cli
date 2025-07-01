// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use internal_error::InternalError;
use kamu_core::auth::{DatasetAction, DatasetsActionNotEnoughPermissionsError};

use crate::AccountToDatasetRelation;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RebacApplyRolesMatrixUseCase: Send + Sync {
    async fn execute(
        &self,
        account_ids: &[&odf::AccountID],
        datasets_with_role_operations: &[(Cow<odf::DatasetID>, DatasetRoleOperation)],
    ) -> Result<(), ApplyRelationMatrixError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum DatasetRoleOperation {
    Set(AccountToDatasetRelation),
    Unset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ApplyRelationMatrixError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl ApplyRelationMatrixError {
    pub fn not_enough_permissions(
        dataset_refs: Vec<odf::DatasetRef>,
        action: DatasetAction,
    ) -> Self {
        Self::Access(odf::AccessError::Unauthorized(
            DatasetsActionNotEnoughPermissionsError {
                action,
                dataset_refs,
            }
            .into(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
