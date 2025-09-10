// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use kamu_accounts::*;
use kamu_auth_rebac::{AccountPropertyName, RebacService, boolean_property_value};
use kamu_auth_rebac_services::DefaultAccountProperties;
use odf::AccountID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A service that aims to register accounts on a one-time basis
pub struct PredefinedAccountsRegistrator {
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    account_service: Arc<dyn AccountService>,
    rebac_service: Arc<dyn RebacService>,
    default_account_properties: Arc<DefaultAccountProperties>,
    update_account_use_case: Arc<dyn UpdateAccountUseCase>,
    create_account_use_case: Arc<dyn CreateAccountUseCase>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    depends_on: &[],
    requires_transaction: true,
})]
impl PredefinedAccountsRegistrator {
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        account_service: Arc<dyn AccountService>,
        rebac_service: Arc<dyn RebacService>,
        default_account_properties: Arc<DefaultAccountProperties>,
        update_account_use_case: Arc<dyn UpdateAccountUseCase>,
        create_account_use_case: Arc<dyn CreateAccountUseCase>,
    ) -> Self {
        Self {
            predefined_accounts_config,
            account_service,
            rebac_service,
            default_account_properties,
            update_account_use_case,
            create_account_use_case,
        }
    }

    async fn set_rebac_properties(
        &self,
        account_id: &AccountID,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        // TODO: Revisit if batch property setting will be implemented
        for name in [
            AccountPropertyName::IsAdmin,
            AccountPropertyName::CanProvisionAccounts,
        ] {
            let value = if let Some(predefined_properties) = &account_config.properties
                && predefined_properties.contains(&name)
            {
                boolean_property_value(true)
            } else {
                self.default_account_properties.as_property_value(name)
            };

            self.rebac_service
                .set_account_property(account_id, name, &value)
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn register_unknown_account(
        &self,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let account: Account = account_config.into();

        self.create_account_use_case
            .execute(&account, &account_config.password)
            .await
            .int_err()?;

        Ok(())
    }

    async fn compare_and_update_account(
        &self,
        account: Account,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let updated_account = Account {
            registered_at: account.registered_at,
            ..account_config.into()
        };

        if account_config.provider == <&'static str>::from(AccountProvider::Password) {
            use VerifyPasswordError as E;

            let has_password_changed = match self
                .account_service
                .verify_account_password(&updated_account.account_name, &account_config.password)
                .await
            {
                Ok(_) => Ok(false),
                Err(E::IncorrectPassword(_)) => Ok(true),
                Err(e @ (E::AccountNotFound(_) | E::Internal(_))) => Err(e.int_err()),
            }?;

            if has_password_changed {
                self.account_service
                    .modify_account_password(&updated_account.id, &account_config.password)
                    .await
                    .int_err()?;
            }
        }

        if account != updated_account {
            tracing::info!(
                "Updating modified predefined account: old: {account:?}, new: {updated_account:?}",
            );

            self.update_account_use_case
                .execute_internal(&updated_account, &account)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for PredefinedAccountsRegistrator {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "PredefinedAccountsRegistrator::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // If there are duplicates by account ID, skip them.
        // This could happen i.e., when a predefined user gets renamed,
        // but the implicit CLI config for current user still points to same ID
        let mut account_config_by_id = HashMap::new();
        for account_config in &self.predefined_accounts_config.predefined {
            let account_id = account_config.get_id();
            match account_config_by_id.entry(account_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(account_config);
                }
                Entry::Occupied(_) => {
                    tracing::warn!(
                        "Duplicate account configuration found for account ID: {}. Skipping.",
                        account_id
                    );
                }
            }
        }

        for account_config in account_config_by_id.values() {
            let account_id = account_config.get_id();
            match self.account_service.get_account_by_id(&account_id).await {
                Ok(account) => {
                    self.compare_and_update_account(account, account_config)
                        .await?;
                }
                Err(GetAccountByIdError::NotFound(_)) => {
                    self.register_unknown_account(account_config).await?;
                }
                Err(GetAccountByIdError::Internal(e)) => return Err(e),
            }
            self.set_rebac_properties(&account_id, account_config)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
