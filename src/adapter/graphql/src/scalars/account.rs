// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(AccountID, odf::AccountID, from_did_str);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountName(odf::AccountName);

impl From<odf::AccountName> for AccountName {
    fn from(value: odf::AccountName) -> Self {
        Self(value)
    }
}

impl From<AccountName> for odf::AccountName {
    fn from(val: AccountName) -> Self {
        val.0
    }
}

impl Deref for AccountName {
    type Target = odf::AccountName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for AccountName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountDisplayName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountDisplayName(kamu_accounts::AccountDisplayName);

impl From<kamu_accounts::AccountDisplayName> for AccountDisplayName {
    fn from(value: kamu_accounts::AccountDisplayName) -> Self {
        Self(value)
    }
}

impl From<AccountDisplayName> for kamu_accounts::AccountDisplayName {
    fn from(val: AccountDisplayName) -> Self {
        val.0
    }
}

impl Deref for AccountDisplayName {
    type Target = kamu_accounts::AccountDisplayName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for AccountDisplayName {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = kamu_accounts::AccountDisplayName::from(value.as_str());
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
