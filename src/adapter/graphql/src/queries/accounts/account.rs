// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone, PartialEq, Eq)]
#[graphql(
    field(name = "id", method = "id", ty = "&AccountID"),
    field(name = "name", ty = "&AccountName")
)]
pub enum Account {
    User(User),
    Organization(Organization),
}

impl Account {
    // TODO: FAKE
    pub(crate) fn fake() -> Self {
        Self::User(User::new(
            AccountID::from(FAKE_USER_ID),
            odf::AccountName::new_unchecked("kamu").into(),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

pub const FAKE_USER_ID: &str = "12345";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    account_id: AccountID,
    account_name: AccountName,
}

#[Object]
impl User {
    #[graphql(skip)]
    pub fn new(account_id: AccountID, account_name: AccountName) -> Self {
        Self {
            account_id,
            account_name,
        }
    }

    /// Unique and stable identitfier of this user account
    async fn id(&self) -> &AccountID {
        &self.account_id
    }

    /// Symbolic account name
    async fn name(&self) -> &AccountName {
        &self.account_name
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Organization {
    account_id: AccountID,
    account_name: AccountName,
}

#[Object]
impl Organization {
    #[allow(dead_code)]
    #[graphql(skip)]
    pub fn new(account_id: AccountID, account_name: AccountName) -> Self {
        Self {
            account_id,
            account_name,
        }
    }

    /// Unique and stable identitfier of this organization account
    async fn id(&self) -> &AccountID {
        &self.account_id
    }

    /// Symbolic account name
    async fn name(&self) -> &AccountName {
        &self.account_name
    }
}
