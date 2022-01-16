// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{convert::TryFrom, ops::Deref};

use async_graphql::*;
use opendatafabric as odf;

///////////////////////////////////////////////////////////////////////////////
// Page-based connection
///////////////////////////////////////////////////////////////////////////////

macro_rules! page_based_connection {
    ($node_type:ident, $connection_type:ident, $edge_type:ident) => {
        #[derive(SimpleObject)]
        #[graphql(complex)]
        pub(crate) struct $connection_type {
            /// A shorthand for `edges { node { ... } }`
            pub nodes: Vec<$node_type>,

            /// Approximate number of total nodes
            pub total_count: Option<usize>,

            /// Page information
            pub page_info: crate::scalars::PageBasedInfo,
        }

        #[ComplexObject]
        impl $connection_type {
            #[graphql(skip)]
            pub fn new(
                nodes: Vec<$node_type>,
                page: usize,
                per_page: usize,
                total_count: Option<usize>,
            ) -> Self {
                let (total_pages, has_next_page) = match total_count {
                    None => (None, nodes.len() != per_page),
                    Some(0) => (Some(0), false),
                    Some(tc) => (
                        Some(tc.unstable_div_ceil(per_page)),
                        (tc.unstable_div_ceil(per_page) - 1) > page,
                    ),
                };

                Self {
                    nodes,
                    total_count,
                    page_info: crate::scalars::PageBasedInfo {
                        has_previous_page: page > 0,
                        has_next_page,
                        total_pages,
                    },
                }
            }
            async fn edges(&self) -> Vec<$edge_type> {
                self.nodes
                    .iter()
                    .map(|node| $edge_type { node: node.clone() })
                    .collect()
            }
        }

        #[derive(SimpleObject)]
        pub(crate) struct $edge_type {
            pub node: $node_type,
        }
    };
}

pub(crate) use page_based_connection;

#[derive(SimpleObject)]
pub struct PageBasedInfo {
    /// When paginating backwards, are there more items?
    pub has_previous_page: bool,

    /// When paginating forwards, are there more items?
    pub has_next_page: bool,

    /// Approximate number of total pages assuming number of nodes per page stays the same
    pub total_pages: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////
// SHA
////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub(crate) struct Multihash(odf::Multihash);

impl From<odf::Multihash> for Multihash {
    fn from(value: odf::Multihash) -> Self {
        Multihash(value)
    }
}

impl Into<odf::Multihash> for Multihash {
    fn into(self) -> odf::Multihash {
        self.0
    }
}

impl Deref for Multihash {
    type Target = odf::Multihash;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for Multihash {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let sha = odf::Multihash::from_multibase_str(value.as_str())?;
            Ok(sha.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DatasetID(odf::DatasetID);

impl From<odf::DatasetID> for DatasetID {
    fn from(value: odf::DatasetID) -> Self {
        DatasetID(value)
    }
}

impl Into<odf::DatasetID> for DatasetID {
    fn into(self) -> odf::DatasetID {
        self.0
    }
}

impl Into<String> for DatasetID {
    fn into(self) -> String {
        self.0.to_did_string()
    }
}

impl Deref for DatasetID {
    type Target = odf::DatasetID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for DatasetID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetID::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetName
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DatasetName(odf::DatasetName);

impl From<odf::DatasetName> for DatasetName {
    fn from(value: odf::DatasetName) -> Self {
        DatasetName(value)
    }
}

impl Into<odf::DatasetName> for DatasetName {
    fn into(self) -> odf::DatasetName {
        self.0
    }
}

impl Into<String> for DatasetName {
    fn into(self) -> String {
        self.0.into()
    }
}

impl Deref for DatasetName {
    type Target = odf::DatasetName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for DatasetName {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetName::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// AccountID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccountID(String);

impl From<&str> for AccountID {
    fn from(value: &str) -> Self {
        AccountID(value.to_owned())
    }
}

impl From<String> for AccountID {
    fn from(value: String) -> Self {
        AccountID(value)
    }
}

impl Into<String> for AccountID {
    fn into(self) -> String {
        self.0
    }
}
impl Deref for AccountID {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for AccountID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            Ok(AccountID::from(value.as_str()))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl AccountID {
    // TODO: UNMOCK: Account ID
    pub fn mock() -> Self {
        Self("123".to_owned())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetKind
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatasetKind {
    Root,
    Derivative,
}

impl From<odf::DatasetKind> for DatasetKind {
    fn from(v: odf::DatasetKind) -> Self {
        match v {
            odf::DatasetKind::Root => DatasetKind::Root,
            odf::DatasetKind::Derivative => DatasetKind::Derivative,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataSchemaFormat {
    Parquet,
    ParquetJson,
}

#[derive(SimpleObject)]
pub(crate) struct DataSchema {
    pub format: DataSchemaFormat,
    pub content: String,
}

/////////////////////////////////////////////////////////////////////////////////////////
// DataSlice
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataSliceFormat {
    Json,
    JsonLD,
    JsonSoA,
    Csv,
}

#[derive(SimpleObject)]
pub(crate) struct DataSlice {
    pub format: DataSliceFormat,
    pub content: String,
}
