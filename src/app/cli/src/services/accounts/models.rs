// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::CurrentAccountSubject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct RelatedAccountIndication {
    pub target_account: TargetAccountSelection,
}

impl RelatedAccountIndication {
    pub fn new(target_account: TargetAccountSelection) -> Self {
        Self { target_account }
    }

    pub fn is_explicit(&self) -> bool {
        self.target_account != TargetAccountSelection::Current
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TargetAccountSelection {
    Current,
    Specific { account_name: String },
    AllUsers,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountIndication {
    pub account_name: odf::AccountName,
    pub user_name: String,
    pub specified_explicitly: bool,
    is_admin: bool,
}

impl CurrentAccountIndication {
    pub fn new<A, U>(
        account_name: A,
        user_name: U,
        specified_explicitly: bool,
        is_admin: bool,
    ) -> Self
    where
        A: Into<String>,
        U: Into<String>,
    {
        Self {
            account_name: odf::AccountName::try_from(account_name.into()).unwrap(),
            user_name: user_name.into(),
            specified_explicitly,
            is_admin,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }

    pub fn to_current_account_subject(&self) -> CurrentAccountSubject {
        CurrentAccountSubject::logged(
            odf::AccountID::new_seeded_ed25519(self.account_name.as_bytes()),
            self.account_name.clone(),
            self.is_admin,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
