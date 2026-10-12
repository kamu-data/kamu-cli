// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/// Collaborator of a dataset with their role
#[derive(SimpleObject, Clone)]
pub struct AccountWithRole {
    /// Collaborator account
    pub account: Account,
    /// Role of the account in the dataset
    pub role: DatasetAccessRole,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Role an account has in a dataset
#[derive(SimpleObject)]
pub struct AuthRelation {
    /// Account the role is granted to
    pub account: Account,
    /// Role of the account in the dataset
    pub role: DatasetAccessRole,
    /// Dataset the role applies to
    pub dataset: Dataset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
