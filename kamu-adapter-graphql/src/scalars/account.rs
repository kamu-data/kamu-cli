// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use async_graphql::*;
use opendatafabric as odf;

/////////////////////////////////////////////////////////////////////////////////////////
// AccountID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountID(String);

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

/////////////////////////////////////////////////////////////////////////////////////////
// AccountName
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountName(odf::AccountName);

impl From<odf::AccountName> for AccountName {
    fn from(value: odf::AccountName) -> Self {
        AccountName(value)
    }
}

impl Into<odf::AccountName> for AccountName {
    fn into(self) -> odf::AccountName {
        self.0
    }
}

impl Deref for AccountName {
    type Target = odf::AccountName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for AccountName {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::AccountName::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
