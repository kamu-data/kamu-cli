// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Account {
    account_id: AccountID,
    account_name: AccountName,
    display_name: AccountDisplayName,
    account_type: AccountType,
    avatar_url: Option<String>,
}

#[derive(Enum, Debug, Copy, Clone, PartialEq, Eq)]
pub enum AccountType {
    User,
    Organization,
}

#[Object]
impl Account {
    #[graphql(skip)]
    pub(crate) fn new(account_info: kamu_core::auth::AccountInfo) -> Self {
        let account_id = AccountID::from(account_info.account_id);
        let account_name: AccountName = AccountName::from(account_info.account_name);
        let display_name: AccountDisplayName = AccountDisplayName::from(account_info.display_name);
        let account_type: AccountType = match account_info.account_type {
            kamu_core::auth::AccountType::User => AccountType::User,
            kamu_core::auth::AccountType::Organization => AccountType::Organization,
        };
        let avatar_url = account_info.avatar_url;

        Self {
            account_id,
            account_name,
            display_name,
            account_type,
            avatar_url,
        }
    }

    /// Unique and stable identitfier of this account
    pub async fn id(&self) -> &AccountID {
        &self.account_id
    }

    /// Symbolic account name
    pub async fn account_name(&self) -> &AccountName {
        &self.account_name
    }

    /// Account name to display
    pub async fn display_name(&self) -> &AccountDisplayName {
        &self.display_name
    }

    /// Account type
    pub async fn account_type(&self) -> &AccountType {
        &self.account_type
    }

    /// Avatar URL
    pub async fn avatar_url(&self) -> &Option<String> {
        &self.avatar_url
    }
}

///////////////////////////////////////////////////////////////////////////////
