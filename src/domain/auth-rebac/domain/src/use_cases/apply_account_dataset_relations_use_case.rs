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
pub trait ApplyAccountDatasetRelationsUseCase: Send + Sync {
    async fn execute(
        &self,
        operations: &[AccountDatasetRelationOperation],
    ) -> Result<(), ApplyRelationMatrixError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountDatasetRelationOperation<'a> {
    pub account_id: Cow<'a, odf::AccountID>,
    pub operation: DatasetRoleOperation,
    pub dataset_id: Cow<'a, odf::DatasetID>,
}

impl<'a> AccountDatasetRelationOperation<'a> {
    pub fn new(
        account_id: Cow<'a, odf::AccountID>,
        operation: DatasetRoleOperation,
        dataset_id: Cow<'a, odf::DatasetID>,
    ) -> Self {
        Self {
            account_id,
            operation,
            dataset_id,
        }
    }
}

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
