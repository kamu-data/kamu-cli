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
use kamu_core::auth::DatasetAction;

use crate::AccountToDatasetRelation;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ApplyAccountDatasetRelationsUseCase: Send + Sync {
    async fn execute(
        &self,
        operation: AccountDatasetRelationOperation<'_>,
    ) -> Result<(), ApplyRelationMatrixError>;

    async fn execute_bulk(
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
pub struct ApplyRelationMatrixBatchError {
    pub action: DatasetAction,
    pub unauthorized_dataset_refs: Vec<odf::DatasetRef>,
    pub not_found_dataset_refs: Vec<odf::DatasetRef>,
}

impl std::fmt::Display for ApplyRelationMatrixBatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "User has no '{}' permission in datasets: '", self.action)?;
        for (i, item) in self.unauthorized_dataset_refs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "'; not found: '")?;
        for (i, item) in self.not_found_dataset_refs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "'")?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ApplyRelationMatrixError {
    #[error(transparent)]
    BatchError(#[from] ApplyRelationMatrixBatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
