// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::ops::Deref;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(AccountID, odf::AccountID, from_did_str);
simple_string_scalar!(AccountName, odf::AccountName);
simple_string_scalar!(Email, email_utils::Email, parse);
simple_string_scalar!(AccountPassword, kamu_accounts::Password, try_new);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// AccountDisplayName
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountDisplayName<'a>(Cow<'a, kamu_accounts::AccountDisplayName>);

impl From<kamu_accounts::AccountDisplayName> for AccountDisplayName<'_> {
    fn from(value: kamu_accounts::AccountDisplayName) -> Self {
        Self(Cow::Owned(value))
    }
}

impl<'a> From<&'a kamu_accounts::AccountDisplayName> for AccountDisplayName<'a> {
    fn from(value: &'a kamu_accounts::AccountDisplayName) -> Self {
        Self(Cow::Borrowed(value))
    }
}

impl From<AccountDisplayName<'_>> for kamu_accounts::AccountDisplayName {
    fn from(val: AccountDisplayName<'_>) -> Self {
        val.0.into_owned()
    }
}

impl Deref for AccountDisplayName<'_> {
    type Target = kamu_accounts::AccountDisplayName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for AccountDisplayName<'_> {
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

#[derive(Enum, Copy, Clone, PartialEq, Eq)]
#[graphql(remote = "kamu_accounts::AccountProvider")]
pub enum AccountProvider {
    Password,
    #[graphql(name = "OAUTH_GITHUB")]
    OAuthGitHub,
    #[graphql(name = "WEB3_WALLET")]
    Web3Wallet,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
