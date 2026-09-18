// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ResourceListColumnDataType,
    ResourceListColumnDefinition,
    ResourceListColumnVisibility,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceListColumnDescriptor {
    pub key: String,
    pub header: String,
    pub data_type: ResourceListColumnDataType,
    pub visibility: ResourceListColumnVisibility,
}

impl From<ResourceListColumnDefinition> for ResourceListColumnDescriptor {
    fn from(value: ResourceListColumnDefinition) -> Self {
        Self {
            key: value.key.to_string(),
            header: value.header.to_string(),
            data_type: value.data_type,
            visibility: value.visibility,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceListColumnValueView {
    pub key: String,
    pub value: ResourceListColumnValue,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ResourceListColumnValue {
    String(String),
    UInt64(u64),
    Bool(bool),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
