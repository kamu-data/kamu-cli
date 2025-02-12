// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::TenancyConfig;
use kamu_accounts::{PredefinedAccountsConfig, DEFAULT_ACCOUNT_NAME_STR};

use crate::accounts::models::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {}

impl AccountService {
    pub fn default_account_name(tenancy_config: TenancyConfig) -> String {
        match tenancy_config {
            TenancyConfig::MultiTenant => whoami::username(),
            TenancyConfig::SingleTenant => String::from(DEFAULT_ACCOUNT_NAME_STR),
        }
    }

    pub fn default_user_name(tenancy_config: TenancyConfig) -> String {
        match tenancy_config {
            TenancyConfig::MultiTenant => whoami::realname(),
            TenancyConfig::SingleTenant => String::from(DEFAULT_ACCOUNT_NAME_STR),
        }
    }

    pub fn current_account_indication(
        account: Option<String>,
        tenancy_config: TenancyConfig,
        predefined_accounts_config: &PredefinedAccountsConfig,
    ) -> CurrentAccountIndication {
        let (current_account, user_name, specified_explicitly) = {
            let default_account_name = Self::default_account_name(tenancy_config);

            if let Some(account) = account {
                (
                    account.clone(),
                    if *account == default_account_name {
                        default_account_name
                    } else {
                        // Use account as username, when there is no data
                        account
                    },
                    true,
                )
            } else {
                let default_user_name = Self::default_user_name(tenancy_config);

                (default_account_name, default_user_name, false)
            }
        };

        let is_admin = if tenancy_config == TenancyConfig::MultiTenant {
            predefined_accounts_config
                .predefined
                .iter()
                .find(|a| a.account_name.as_str().eq(&current_account))
                .is_some_and(|a| a.is_admin)
        } else {
            true
        };

        CurrentAccountIndication::new(current_account, user_name, specified_explicitly, is_admin)
    }

    pub fn related_account_indication(
        target_account: Option<String>,
        all_accounts: bool,
    ) -> RelatedAccountIndication {
        let target_account = if let Some(account_name) = target_account {
            TargetAccountSelection::Specific { account_name }
        } else if all_accounts {
            TargetAccountSelection::AllUsers
        } else {
            TargetAccountSelection::Current
        };

        RelatedAccountIndication::new(target_account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
