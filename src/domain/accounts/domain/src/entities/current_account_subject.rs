// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum CurrentAccountSubject {
    Logged(LoggedAccount),
    Anonymous(AnonymousAccountReason),
}

#[derive(Debug, Clone)]
pub struct LoggedAccount {
    pub account_id: odf::AccountID,
    pub account_name: odf::AccountName,
    pub is_admin: bool,
}

#[derive(Debug, Copy, Clone)]
pub enum AnonymousAccountReason {
    NoAuthenticationProvided,
    AuthenticationInvalid,
    AuthenticationExpired,
}

impl CurrentAccountSubject {
    pub fn anonymous(reason: AnonymousAccountReason) -> Self {
        Self::Anonymous(reason)
    }

    pub fn logged(
        account_id: odf::AccountID,
        account_name: odf::AccountName,
        is_admin: bool,
    ) -> Self {
        Self::Logged(LoggedAccount {
            account_id,
            account_name,
            is_admin,
        })
    }

    #[cfg(any(feature = "testing", test))]
    pub fn new_test() -> Self {
        let is_admin = false;

        Self::logged(
            DEFAULT_ACCOUNT_ID.clone(),
            DEFAULT_ACCOUNT_NAME.clone(),
            is_admin,
        )
    }

    pub fn account_name(&self) -> &odf::AccountName {
        match self {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account misused");
            }
            CurrentAccountSubject::Logged(l) => &l.account_name,
        }
    }

    pub fn maybe_account_name(&self) -> Option<&odf::AccountName> {
        match self {
            CurrentAccountSubject::Logged(l) => Some(&l.account_name),
            CurrentAccountSubject::Anonymous(_) => None,
        }
    }

    pub fn account_name_or_default(&self) -> &odf::AccountName {
        match self {
            CurrentAccountSubject::Logged(l) => &l.account_name,
            CurrentAccountSubject::Anonymous(_) => &DEFAULT_ACCOUNT_NAME,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
