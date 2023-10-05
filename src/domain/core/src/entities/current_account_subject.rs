// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::AccountName;

use crate::auth::DEFAULT_ACCOUNT_NAME;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum CurrentAccountSubject {
    Logged(LoggedAccount),
    Anonymous(AnonymousAccountReason),
}

#[derive(Debug)]
pub struct LoggedAccount {
    pub account_name: AccountName,
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

    pub fn logged(account_name: AccountName) -> Self {
        Self::Logged(LoggedAccount { account_name })
    }

    pub fn new_test() -> Self {
        Self::logged(AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
