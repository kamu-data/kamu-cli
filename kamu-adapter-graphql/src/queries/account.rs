// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::scalars::*;

use async_graphql::*;
use opendatafabric as odf;

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone, PartialEq, Eq)]
#[graphql(
    field(name = "id", method = "id", type = "&AccountID"),
    field(name = "name", type = "&str")
)]
pub enum Account {
    User(User),
    Organization(Organization),
}

impl Account {
    // TODO: MOCK
    pub(crate) fn mock() -> Self {
        Self::User(User::new(
            AccountID::from("1"),
            odf::AccountName::try_from("kamu").unwrap().into(),
        ))
    }
}

///////////////////////////////////////////////////////////////////////////////

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
    async fn name(&self) -> &str {
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
    async fn name(&self) -> &str {
        &self.account_name
    }
}
