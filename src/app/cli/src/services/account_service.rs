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
use kamu::domain::CurrentAccountConfig;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {
    pub default_account_name: String,
}

#[component(pub)]
impl AccountService {
    pub fn new() -> Self {
        Self {
            default_account_name: whoami::username(),
        }
    }

    pub fn current_account_config(&self, arg_matches: &ArgMatches) -> CurrentAccountConfig {
        let (current_account, specified_explicitly) =
            if let Some(account) = arg_matches.get_one::<String>("account") {
                (account, true)
            } else {
                (&self.default_account_name, false)
            };

        CurrentAccountConfig::new(current_account, specified_explicitly)
    }

    pub fn related_account_indication(&self, arg_matches: &ArgMatches) -> RelatedAccountIndication {
        let target_account =
            if let Some(target_account) = arg_matches.get_one::<String>("target-account") {
                TargetAccountSelection::Specific {
                    account_name: target_account.clone(),
                }
            } else if arg_matches.get_flag("all-accounts") {
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
