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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetEntry {
    pub id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub owner_name: odf::AccountName,
    pub name: odf::DatasetName,
    pub created_at: DateTime<Utc>,
    pub kind: odf::DatasetKind,
}

impl DatasetEntry {
    pub fn new(
        id: odf::DatasetID,
        owner_id: odf::AccountID,
        owner_name: odf::AccountName,
        name: odf::DatasetName,
        created_at: DateTime<Utc>,
        kind: odf::DatasetKind,
    ) -> Self {
        Self {
            id,
            owner_id,
            owner_name,
            name,
            created_at,
            kind,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetEntryRowModel {
    pub id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub owner_name: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub kind: DatasetEntryKindRowModel,
}

#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "dataset_kind", rename_all = "lowercase")
)]
#[derive(Debug, Clone, Copy, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum DatasetEntryKindRowModel {
    Root,
    Derivative,
}

#[cfg(feature = "sqlx")]
impl From<odf::DatasetKind> for DatasetEntryKindRowModel {
    fn from(value: odf::DatasetKind) -> Self {
        match value {
            odf::DatasetKind::Root => Self::Root,
            odf::DatasetKind::Derivative => Self::Derivative,
        }
    }
}

#[cfg(feature = "sqlx")]
impl From<DatasetEntryRowModel> for DatasetEntry {
    fn from(
        DatasetEntryRowModel {
            id,
            owner_id,
            owner_name,
            name,
            created_at,
            kind,
        }: DatasetEntryRowModel,
    ) -> Self {
        Self {
            id,
            owner_id,
            owner_name: odf::AccountName::new_unchecked(&owner_name),
            name: odf::DatasetName::new_unchecked(&name),
            created_at,
            kind: match kind {
                DatasetEntryKindRowModel::Root => odf::DatasetKind::Root,
                DatasetEntryKindRowModel::Derivative => odf::DatasetKind::Derivative,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
