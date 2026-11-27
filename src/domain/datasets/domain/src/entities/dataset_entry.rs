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
    /// ID of the dataset resource, permanent once assigned
    pub resource_id: odf::ResourceID,
    pub owner_id: odf::AccountID,
    pub owner_name: odf::AccountName,
    pub name: odf::DatasetName,
    pub created_at: DateTime<Utc>,
    pub kind: odf::DatasetKind,
}

impl DatasetEntry {
    pub fn new(
        id: odf::DatasetID,
        resource_id: odf::ResourceID,
        owner_id: odf::AccountID,
        owner_name: odf::AccountName,
        name: odf::DatasetName,
        created_at: DateTime<Utc>,
        kind: odf::DatasetKind,
    ) -> Self {
        Self {
            id,
            resource_id,
            owner_id,
            owner_name,
            name,
            created_at,
            kind,
        }
    }

    /// A fresh resource ID for a new entry; an existing entry keeps its own
    pub fn generate_resource_id() -> odf::ResourceID {
        odf::ResourceID::new(uuid::Uuid::new_v4())
    }

    pub fn alias(&self) -> odf::DatasetAlias {
        odf::DatasetAlias::new(Some(self.owner_name.clone()), self.name.clone())
    }

    pub fn handle(&self) -> odf::DatasetHandle {
        odf::DatasetHandle {
            id: self.id.clone(),
            alias: self.alias(),
            kind: self.kind,
        }
    }

    /// The ODF handle of this entry, owned by `account`
    pub fn to_odf_handle(
        &self,
        account: odf::ResourceOwnerHandle,
    ) -> odf::metadata::datasets::DatasetHandle {
        odf::metadata::datasets::DatasetHandle {
            account,
            id: self.resource_id,
            did: self.id.clone(),
            name: odf::metadata::resources::ResourceName::new_unchecked(self.name.as_str()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct DatasetEntryRowModel {
    pub id: odf::DatasetID,
    pub resource_id: odf::ResourceID,
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
#[derive(Debug, Clone, Copy, strum::Display, strum::EnumString, strum::IntoStaticStr)]
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
impl From<DatasetEntryKindRowModel> for odf::DatasetKind {
    fn from(value: DatasetEntryKindRowModel) -> Self {
        match value {
            DatasetEntryKindRowModel::Root => Self::Root,
            DatasetEntryKindRowModel::Derivative => Self::Derivative,
        }
    }
}

#[cfg(feature = "sqlx")]
impl From<DatasetEntryRowModel> for DatasetEntry {
    fn from(
        DatasetEntryRowModel {
            id,
            resource_id,
            owner_id,
            owner_name,
            name,
            created_at,
            kind,
        }: DatasetEntryRowModel,
    ) -> Self {
        Self {
            id,
            resource_id,
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
