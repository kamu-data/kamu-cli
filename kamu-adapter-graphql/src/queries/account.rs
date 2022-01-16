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

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(
    field(name = "id", method = "id", type = "&AccountID"),
    field(name = "name", type = "&str")
)]
pub(crate) enum Account {
    User(User),
    Organization(Organization),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct User {
    /// Unique identifier of this user account
    account_id: AccountID,
}

#[ComplexObject]
impl User {
    #[graphql(skip)]
    pub fn new(id: AccountID) -> Self {
        Self { account_id: id }
    }

    async fn id(&self) -> &AccountID {
        &self.account_id
    }

    // TODO: UNMOCK
    /// Symbolic name
    async fn name(&self) -> &str {
        "anonymous"
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct Organization {
    /// Unique identifier of this organization account
    id: AccountID,
}

#[ComplexObject]
impl Organization {
    #[allow(dead_code)]
    #[graphql(skip)]
    pub fn new(id: AccountID) -> Self {
        Self { id }
    }

    // TODO: UNMOCK
    /// Symbolic name
    async fn name(&self) -> &str {
        "anonymous"
    }
}
