// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::ops::Deref;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetID
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetID(odf::DatasetID);

impl From<odf::DatasetID> for DatasetID {
    fn from(value: odf::DatasetID) -> Self {
        DatasetID(value)
    }
}

impl From<DatasetID> for odf::DatasetID {
    fn from(val: DatasetID) -> Self {
        val.0
    }
}

impl From<DatasetID> for String {
    fn from(val: DatasetID) -> Self {
        val.0.to_string()
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
            let val = odf::DatasetID::from_did_str(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetName(odf::DatasetName);

impl From<odf::DatasetName> for DatasetName {
    fn from(value: odf::DatasetName) -> Self {
        DatasetName(value)
    }
}

impl From<DatasetName> for odf::DatasetName {
    fn from(val: DatasetName) -> Self {
        val.0
    }
}

impl From<DatasetName> for String {
    fn from(val: DatasetName) -> Self {
        val.0.into()
    }
}

impl Deref for DatasetName {
    type Target = odf::DatasetName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for DatasetName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefAny
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRefAny(odf::DatasetRefAny);

impl From<odf::DatasetRefAny> for DatasetRefAny {
    fn from(value: odf::DatasetRefAny) -> Self {
        DatasetRefAny(value)
    }
}

impl From<DatasetRefAny> for odf::DatasetRefAny {
    fn from(val: DatasetRefAny) -> Self {
        val.0
    }
}

impl From<DatasetRefAny> for String {
    fn from(val: DatasetRefAny) -> Self {
        val.0.to_string()
    }
}

impl Deref for DatasetRefAny {
    type Target = odf::DatasetRefAny;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for DatasetRefAny {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[Scalar]
impl ScalarType for DatasetRefAny {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetRefAny::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRef
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetRef(odf::DatasetRef);

impl From<odf::DatasetRef> for DatasetRef {
    fn from(value: odf::DatasetRef) -> Self {
        DatasetRef(value)
    }
}

impl From<DatasetRef> for odf::DatasetRef {
    fn from(val: DatasetRef) -> Self {
        val.0
    }
}

impl From<DatasetRef> for String {
    fn from(val: DatasetRef) -> Self {
        val.0.to_string()
    }
}

impl Deref for DatasetRef {
    type Target = odf::DatasetRef;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for DatasetRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[Scalar]
impl ScalarType for DatasetRef {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetRef::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetAlias
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetAlias(odf::DatasetAlias);

impl From<odf::DatasetAlias> for DatasetAlias {
    fn from(value: odf::DatasetAlias) -> Self {
        DatasetAlias(value)
    }
}

impl From<DatasetAlias> for odf::DatasetAlias {
    fn from(val: DatasetAlias) -> Self {
        val.0
    }
}

impl From<DatasetAlias> for String {
    fn from(val: DatasetAlias) -> Self {
        val.0.to_string()
    }
}

impl Deref for DatasetAlias {
    type Target = odf::DatasetAlias;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for DatasetAlias {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[Scalar]
impl ScalarType for DatasetAlias {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetAlias::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefRemote
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRefRemote(odf::DatasetRefRemote);

impl From<odf::DatasetRefRemote> for DatasetRefRemote {
    fn from(value: odf::DatasetRefRemote) -> Self {
        DatasetRefRemote(value)
    }
}

impl std::fmt::Display for DatasetRefRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[Scalar]
impl ScalarType for DatasetRefRemote {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetRefRemote::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
