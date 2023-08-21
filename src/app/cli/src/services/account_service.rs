// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::ArgMatches;
use dill::component;
use kamu::domain::CurrentAccountSubject;
use opendatafabric::AccountName;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {
    pub default_account_name: String,
    pub default_user_name: String,
}

#[component(pub)]
impl AccountService {
    pub fn new() -> Self {
        Self {
            default_account_name: whoami::username(),
            default_user_name: whoami::realname(),
        }
    }

    pub fn current_account_indication(&self, arg_matches: &ArgMatches) -> CurrentAccountIndication {
        let (current_account, user_name, specified_explicitly) = if let Some(account) =
            arg_matches.get_one::<String>("account")
        {
            (
                account,
                if account.eq(&self.default_account_name) {
                    &self.default_user_name
                } else {
                    &account // Use account as user name, when there is no data
                },
                true,
            )
        } else {
            (&self.default_account_name, &self.default_user_name, false)
        };

        CurrentAccountIndication::new(current_account, user_name, specified_explicitly)
    }

    pub fn related_account_indication(&self, sub_matches: &ArgMatches) -> RelatedAccountIndication {
        let target_account =
            if let Some(target_account) = sub_matches.get_one::<String>("target-account") {
                TargetAccountSelection::Specific {
                    account_name: target_account.clone(),
                }
            } else if sub_matches.get_flag("all-accounts") {
                TargetAccountSelection::AllUsers
            } else {
                TargetAccountSelection::Current
            };

        RelatedAccountIndication::new(target_account)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CurrentAccountIndication {
    pub account_name: AccountName,
    pub user_name: String,
    pub specified_explicitly: bool,
}

impl CurrentAccountIndication {
    pub fn new<A, U>(account_name: A, user_name: U, specified_explicitly: bool) -> Self
    where
        A: Into<String>,
        U: Into<String>,
    {
        Self {
            account_name: AccountName::try_from(account_name.into()).unwrap(),
            user_name: user_name.into(),
            specified_explicitly,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }

    pub fn as_current_account_subject(&self) -> CurrentAccountSubject {
        CurrentAccountSubject::new(&self.account_name, &self.user_name)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
