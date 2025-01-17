// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatasetEntry {
    pub id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub name: odf::DatasetName,
    pub created_at: DateTime<Utc>,
}

impl DatasetEntry {
    pub fn new(
        id: odf::DatasetID,
        owner_id: odf::AccountID,
        name: odf::DatasetName,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            owner_id,
            name,
            created_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetEntryRowModel {
    pub id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

#[cfg(feature = "sqlx")]
impl From<DatasetEntryRowModel> for DatasetEntry {
    fn from(
        DatasetEntryRowModel {
            id,
            owner_id,
            name,
            created_at,
        }: DatasetEntryRowModel,
    ) -> Self {
        Self {
            id,
            owner_id,
            name: odf::DatasetName::new_unchecked(&name),
            created_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
