// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetConfigurationSetBinding {
    pub dataset_id: odf::DatasetID,
    pub resource_uid: kamu_resources::ResourceUID,
    pub binding_order: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct DatasetConfigurationSetBindingRowModel {
    pub dataset_id: odf::DatasetID,
    pub resource_uid: Uuid,
    pub binding_order: i64,
}

impl From<DatasetConfigurationSetBindingRowModel> for DatasetConfigurationSetBinding {
    fn from(value: DatasetConfigurationSetBindingRowModel) -> Self {
        Self {
            dataset_id: value.dataset_id,
            resource_uid: kamu_resources::ResourceUID::new(value.resource_uid),
            binding_order: u64::try_from(value.binding_order).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
