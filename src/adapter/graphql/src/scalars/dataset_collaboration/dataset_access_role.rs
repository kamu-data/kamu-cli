// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::AccountToDatasetRelation;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum DatasetAccessRole {
    /// Role opening the possibility for read-only access
    Reader,
    /// Role opening to modify dataset data
    Editor,
    /// Role to maintain the dataset
    Maintainer,
}

impl From<DatasetAccessRole> for AccountToDatasetRelation {
    fn from(role: DatasetAccessRole) -> Self {
        match role {
            DatasetAccessRole::Reader => Self::Reader,
            DatasetAccessRole::Editor => Self::Editor,
            DatasetAccessRole::Maintainer => Self::Maintainer,
        }
    }
}

impl From<AccountToDatasetRelation> for DatasetAccessRole {
    fn from(relation: AccountToDatasetRelation) -> Self {
        match relation {
            AccountToDatasetRelation::Reader => Self::Reader,
            AccountToDatasetRelation::Editor => Self::Editor,
            AccountToDatasetRelation::Maintainer => Self::Maintainer,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
