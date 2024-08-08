// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatasetEntry {
    pub id: DatasetID,
    pub owner_id: AccountID,
    pub alias: DatasetName,
}

impl DatasetEntry {
    pub fn new(id: DatasetID, owner_id: AccountID, alias: DatasetName) -> Self {
        Self {
            id,
            owner_id,
            alias,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
