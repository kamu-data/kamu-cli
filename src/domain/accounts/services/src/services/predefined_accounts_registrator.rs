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

use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use kamu_accounts::*;
use kamu_auth_rebac::{
    AccountProperties,
    AccountPropertyName,
    RebacService,
    boolean_property_value,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A service that aims to register accounts on a one-time basis
#[derive(Clone)]
pub struct PredefinedAccountsRegistrator {
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    account_service: Arc<dyn AccountService>,
    rebac_service: Arc<dyn RebacService>,
    default_account_properties: Arc<AccountProperties>,
    update_account_use_case: Arc<dyn UpdateAccountUseCase>,
    create_account_use_case: Arc<dyn CreateAccountUseCase>,
}

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    depends_on: &[],
    requires_transaction: true,
})]
impl PredefinedAccountsRegistrator {
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        account_service: Arc<dyn AccountService>,
        rebac_service: Arc<dyn RebacService>,
        default_account_properties: Arc<AccountProperties>,
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

    async fn process_account(&self, account_config: &AccountConfig) -> Result<(), InternalError> {
        let account_id = account_config.get_id();

        match self.account_service.get_account_by_id(&account_id).await {
            Ok(account) => {
                self.compare_and_maybe_update_account(account, account_config)
                    .await?;
            }
            Err(GetAccountByIdError::NotFound(_)) => {
                self.register_unknown_account(account_config).await?;
            }
            Err(GetAccountByIdError::Internal(e)) => return Err(e),
        }

        self.set_rebac_properties(&account_id, account_config)
            .await?;

        Ok(())
    }

    async fn set_rebac_properties(
        &self,
        account_id: &odf::AccountID,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        // TODO: Revisit if batch property setting will be implemented
        for name in [
            AccountPropertyName::IsAdmin,
            AccountPropertyName::CanProvisionAccounts,
        ] {
            let value = if account_config.properties.contains(&name.into()) {
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
        self.create_account_use_case
            .execute(account_config, true /* quiet */)
            .await
            .int_err()?;

        Ok(())
    }

    async fn compare_and_maybe_update_account(
        &self,
        original_account: Account,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let updated_account = Account {
            id: account_config.get_id(),
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: original_account.registered_at,
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.account_name.to_string(),
        };

        if AccountProvider::is_password(&account_config.provider) {
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

        if original_account != updated_account {
            tracing::info!(
                "Updating modified predefined account: old: {original_account:?}, new: \
                 {updated_account:?}",
            );

            self.update_account_use_case
                .execute_internal(&updated_account, &original_account)
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
                    entry.insert(account_config.clone());
                }
                Entry::Occupied(_) => {
                    tracing::warn!(
                        "Duplicate account configuration found for account ID: {account_id} ({}). \
                         Skipping.",
                        account_config.account_name
                    );
                }
            }
        }

        // Process accounts in parallel using tasks
        // Note: these are heavy operations, because of password hashing, ReBAC activity
        let mut join_set = tokio::task::JoinSet::new();
        for account_config in account_config_by_id.into_values() {
            let registrator = self.clone();
            join_set.spawn(async move { registrator.process_account(&account_config).await });
        }

        // Execute jobs in parallel
        let results = join_set.join_all().await;

        // Report errors, if any
        let mut had_errors = false;
        for result in results {
            if let Err(err) = result {
                had_errors = true;
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Failed to process predefined account",
                );
            }
        }

        // Interrupt initialization if there were errors
        if had_errors {
            Err(InternalError::new(
                "One or more predefined accounts failed to register/update.",
            ))
        } else {
            Ok(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
