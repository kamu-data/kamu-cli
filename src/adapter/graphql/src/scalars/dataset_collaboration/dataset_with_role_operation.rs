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

#[derive(InputObject)]
pub struct AccountDatasetRelationOperation {
    pub account_id: AccountID<'static>,
    pub operation: DatasetRoleOperation,
    pub dataset_id: DatasetID<'static>,
}

#[derive(OneofObject, Copy, Clone)]
pub enum DatasetRoleOperation {
    Set(DatasetRoleSetOperation),
    Unset(DatasetRoleUnsetOperation),
}

#[derive(InputObject, Copy, Clone)]
pub struct DatasetRoleSetOperation {
    role: DatasetAccessRole,
}

#[derive(InputObject, Copy, Clone)]
pub struct DatasetRoleUnsetOperation {
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
