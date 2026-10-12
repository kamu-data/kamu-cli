// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::TenancyConfig;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME_STR};

use crate::accounts::models::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountService {}

impl AccountService {
    pub fn default_account_name(
        tenancy_config: TenancyConfig,
    ) -> Result<odf::AccountName, OsUserAccountNameError> {
        match tenancy_config {
            TenancyConfig::MultiTenant => {
                Self::account_name_from_os_user(whoami::username().ok().as_deref())
            }
            TenancyConfig::SingleTenant => Ok(DEFAULT_ACCOUNT_NAME.clone()),
        }
    }

    /// Maps an OS user name onto the account name grammar, which is stricter
    /// than OS user names: `john_doe` (Linux), `_www` (macOS) and `John Doe`
    /// (Windows) are all real ones. Valid names are returned unchanged.
    ///
    /// Each run of ASCII characters other than letters and digits becomes a
    /// single `-` (or `.` when the run has only dots), runs at the ends are
    /// dropped, and so is any non-ASCII character. Fails when the name is unavailable or
    /// nothing valid remains, rather than guessing an identity.
    pub fn account_name_from_os_user(
        os_user_name: Option<&str>,
    ) -> Result<odf::AccountName, OsUserAccountNameError> {
        let Some(os_user_name) = os_user_name else {
            return Err(OsUserAccountNameError::Unavailable);
        };

        let mut normalized = String::new();
        let mut pending_separator = None;

        // Non-ASCII characters are dropped, so `Müller` becomes `Mller` and
        // `Ольга` fails. Transliterating first (the `any_ascii` crate gives
        // `Muller`, `Ol'ga`, `ZhangWei`), with `'` dropped rather than treated
        // as a separator, would keep such names.
        for c in os_user_name.chars() {
            if c.is_ascii_alphanumeric() {
                if let Some(separator) = pending_separator.take()
                    && !normalized.is_empty()
                {
                    normalized.push(separator);
                }
                normalized.push(c);
            } else if c.is_ascii() {
                pending_separator = Some(match pending_separator {
                    None | Some('.') if c == '.' => '.',
                    _ => '-',
                });
            }
        }

        if normalized.is_empty() {
            return Err(OsUserAccountNameError::NoValidCharacters {
                os_user_name: os_user_name.to_string(),
            });
        }

        Ok(odf::AccountName::try_from(normalized).unwrap_or_else(|e| {
            panic!("OS user name {os_user_name:?} normalized to an invalid account name: {e}")
        }))
    }

    pub fn default_user_name(tenancy_config: TenancyConfig) -> String {
        match tenancy_config {
            TenancyConfig::MultiTenant => whoami::realname()
                .or_else(|_| whoami::username())
                .unwrap_or_else(|_| String::from("Unknown")),
            TenancyConfig::SingleTenant => String::from(DEFAULT_ACCOUNT_NAME_STR),
        }
    }

    /// An explicitly specified account name is taken as typed: it is never
    /// normalized like the implicit OS user name, so an invalid one is an error.
    pub fn current_account_indication(
        maybe_specified_account: Option<String>,
        tenancy_config: TenancyConfig,
    ) -> Result<CurrentAccountIndication, CurrentAccountIndicationError> {
        match maybe_specified_account {
            // Use account as username, when there is no data
            Some(account) => Ok(CurrentAccountIndication::new(
                account.clone(),
                account,
                true,
            )?),
            None => Ok(CurrentAccountIndication {
                account_name: Self::default_account_name(tenancy_config)?,
                user_name: Self::default_user_name(tenancy_config),
                specified_explicitly: false,
            }),
        }
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
