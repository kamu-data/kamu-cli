// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, AccountName};

use crate::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum CurrentAccountSubject {
    Logged(LoggedAccount),
    Anonymous(AnonymousAccountReason),
}

#[derive(Debug, Clone)]
pub struct LoggedAccount {
    pub account_id: AccountID,
    pub account_name: AccountName,
    pub is_admin: bool,
}

#[derive(Debug)]
pub enum AnonymousAccountReason {
    NoAuthenticationProvided,
    AuthenticationInvalid,
    AuthenticationExpired,
}

impl CurrentAccountSubject {
    pub fn anonymous(reason: AnonymousAccountReason) -> Self {
        Self::Anonymous(reason)
    }

    pub fn logged(account_id: AccountID, account_name: AccountName, is_admin: bool) -> Self {
        Self::Logged(LoggedAccount {
            account_id,
            account_name,
            is_admin,
        })
    }

    pub fn new_test() -> Self {
        let is_admin = false;

        Self::logged(
            DEFAULT_ACCOUNT_ID.clone(),
            DEFAULT_ACCOUNT_NAME.clone(),
            is_admin,
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
