// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::DataType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, tag = "kind")]
pub enum DataTypeExt {
    #[serde(alias = "did")]
    Did(DataTypeExtDid),

    #[serde(alias = "multihash")]
    Multihash(DataTypeExtMultihash),

    #[serde(alias = "objectlink", alias = "objectLink")]
    ObjectLink(DataTypeExtObjectLink),

    #[serde(untagged)]
    Core(#[serde_as(as = "crate::serde::yaml::DataTypeDef")] DataType),
}

impl DataTypeExt {
    pub fn did() -> Self {
        Self::Did(DataTypeExtDid {})
    }

    pub fn multihash() -> Self {
        Self::Multihash(DataTypeExtMultihash {})
    }

    pub fn object_link(link_type: impl Into<DataTypeExt>) -> Self {
        Self::ObjectLink(DataTypeExtObjectLink {
            link_type: Box::new(link_type.into()),
        })
    }

    pub fn core(typ: DataType) -> Self {
        Self::Core(typ)
    }
}

impl From<DataType> for DataTypeExt {
    fn from(value: DataType) -> Self {
        Self::Core(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DataTypeExtDid {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DataTypeExtMultihash {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DataTypeExtObjectLink {
    pub link_type: Box<DataTypeExt>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
