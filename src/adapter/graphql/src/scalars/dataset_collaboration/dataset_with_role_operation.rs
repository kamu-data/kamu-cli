// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::DatasetRoleOperation as DomainDatasetRoleOperation;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct DatasetWithRoleOperation {
    pub dataset_id: DatasetID<'static>,
    pub operation: DatasetRoleOperation,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
