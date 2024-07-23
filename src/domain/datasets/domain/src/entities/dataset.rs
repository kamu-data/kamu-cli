// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, DatasetAlias, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Dataset {
    pub id: DatasetID,
    pub owner_id: AccountID,
    pub alias: DatasetAlias,
}

impl Dataset {
    pub fn new(id: DatasetID, owner_id: AccountID, alias: DatasetAlias) -> Self {
        Self {
            id,
            owner_id,
            alias,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
