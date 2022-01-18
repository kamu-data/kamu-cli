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
