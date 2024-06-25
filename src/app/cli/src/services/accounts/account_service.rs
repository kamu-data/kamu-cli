// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::ArgMatches;
use kamu_accounts::{PredefinedAccountsConfig, DEFAULT_ACCOUNT_NAME_STR};

use crate::accounts::models::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {}

impl AccountService {
    pub fn default_account_name(multi_tenant_workspace: bool) -> String {
        if multi_tenant_workspace {
            whoami::username()
        } else {
            String::from(DEFAULT_ACCOUNT_NAME_STR)
        }
    }

    pub fn default_user_name(multi_tenant_workspace: bool) -> String {
        if multi_tenant_workspace {
            whoami::realname()
        } else {
            String::from(DEFAULT_ACCOUNT_NAME_STR)
        }
    }

    pub fn current_account_indication(
        arg_matches: &ArgMatches,
        multi_tenant_workspace: bool,
        predefined_accounts_config: &PredefinedAccountsConfig,
    ) -> CurrentAccountIndication {
        let (current_account, user_name, specified_explicitly) = {
            let default_account_name = AccountService::default_account_name(multi_tenant_workspace);

            if let Some(account) = arg_matches.get_one::<String>("account") {
                (
                    account.clone(),
                    if account.eq(&default_account_name) {
                        default_account_name
                    } else {
                        account.clone() // Use account as username, when there
                                        // is no data
                    },
                    true,
                )
            } else {
                let default_user_name = AccountService::default_user_name(multi_tenant_workspace);

                (default_account_name, default_user_name, false)
            }
        };

        let is_admin = if multi_tenant_workspace {
            predefined_accounts_config
                .predefined
                .iter()
                .find(|a| a.account_name.as_str().eq(&current_account))
                .map_or(false, |a| a.is_admin)
        } else {
            true
        };

        CurrentAccountIndication::new(current_account, user_name, specified_explicitly, is_admin)
    }

    pub fn related_account_indication(sub_matches: &ArgMatches) -> RelatedAccountIndication {
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

///////////////////////////////////////////////////////////////////////////////
