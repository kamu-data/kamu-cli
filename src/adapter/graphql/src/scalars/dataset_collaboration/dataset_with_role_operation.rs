// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use kamu_auth_rebac::{
    AccountDatasetRelationOperation as DomainAccountDatasetRelationOperation,
    DatasetRoleOperation as DomainDatasetRoleOperation,
};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Change of an account's role in a dataset
#[derive(InputObject)]
pub struct AccountDatasetRelationOperation {
    /// Account whose role changes
    pub account_id: AccountID<'static>,
    /// Change to apply
    pub operation: DatasetRoleOperation,
    /// Dataset the role applies to
    pub dataset_id: DatasetID<'static>,
}

/// Change to an account's role in a dataset
#[derive(OneofObject, Copy, Clone)]
pub enum DatasetRoleOperation {
    /// Grant the role, replacing any existing one
    Set(DatasetRoleSetOperation),
    /// Revoke the existing role
    Unset(DatasetRoleUnsetOperation),
}

/// Grants a role to an account
#[derive(InputObject, Copy, Clone)]
pub struct DatasetRoleSetOperation {
    /// Role to grant
    role: DatasetAccessRole,
}

/// Revokes the role of an account
#[derive(InputObject, Copy, Clone)]
pub struct DatasetRoleUnsetOperation {
    /// Placeholder field without meaning, its value is ignored
    _dummy: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetRoleOperation> for DomainDatasetRoleOperation {
    fn from(v: DatasetRoleOperation) -> Self {
        match v {
            DatasetRoleOperation::Set(op) => DomainDatasetRoleOperation::Set(op.role.into()),
            DatasetRoleOperation::Unset(_) => DomainDatasetRoleOperation::Unset,
        }
    }
}

impl<'a> From<&'a AccountDatasetRelationOperation> for DomainAccountDatasetRelationOperation<'a> {
    fn from(v: &'a AccountDatasetRelationOperation) -> Self {
        DomainAccountDatasetRelationOperation {
            account_id: Cow::Borrowed(v.account_id.as_ref()),
            operation: v.operation.into(),
            dataset_id: Cow::Borrowed(v.dataset_id.as_ref()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
