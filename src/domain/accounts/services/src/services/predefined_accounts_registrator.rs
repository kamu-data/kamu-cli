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
use std::error::Error;
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
use odf::metadata::AsStackString;
use secrecy::{ExposeSecret, SecretString};

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
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
    did_secret_encryption_key: Option<SecretString>,
}

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    depends_on: &[],
    requires_transaction: true,
})]
impl PredefinedAccountsRegistrator {
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        account_service: Arc<dyn AccountService>,
        rebac_service: Arc<dyn RebacService>,
        default_account_properties: Arc<AccountProperties>,
        update_account_use_case: Arc<dyn UpdateAccountUseCase>,
        create_account_use_case: Arc<dyn CreateAccountUseCase>,
        did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
        did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
    ) -> Self {
        Self {
            predefined_accounts_config,
            account_service,
            rebac_service,
            default_account_properties,
            update_account_use_case,
            create_account_use_case,
            did_secret_key_repo,
            did_secret_encryption_key: did_secret_encryption_config
                .get_encryption_key()
                .map(SecretString::from),
        }
    }

    async fn process_account(&self, account_config: &AccountConfig) -> Result<(), InternalError> {
        let maybe_account_id = account_config.resolve_account_id().int_err()?;

        /*
        - accountName: kamu
          password: 1
          avatarUrl: https://avatars.githubusercontent.com/u/50896974?s=200&v=4
          properties:
            - Admin
          treatDatasetsAsPublic: true
          email: support+kamu@kamu.dev

        - accountName: sh101-bowen
          password: sh101-bowen
          avatarUrl: https://cdn-icons-png.flaticon.com/512/3118/3118054.png
          treatDatasetsAsPublic: true
          email: support+sh101-bowen@kamu.dev
        */

        // todo информация: какие у нас есть кодовые пути
        //                  1) попытаться создать неизвестный
        //                  - если есть id/ключи
        //                  - если не найден по уникальным полям
        //                  - если найден: ???
        //                  2) обновить известный если есть id и/или ключ

        let account_id = if let Some(account_id) = maybe_account_id {
            match self.account_service.get_account_by_id(&account_id).await {
                Ok(original_account) => {
                    self.compare_and_maybe_update_account(
                        &account_id,
                        original_account,
                        account_config,
                    )
                    .await?;
                    account_id
                }
                Err(GetAccountByIdError::NotFound(_)) => {
                    self.preflight_unique_fields(account_config).await?;
                    self.register_unknown_account(account_config).await?
                }
                Err(e @ GetAccountByIdError::Internal(_)) => return Err(e.int_err()),
            }
        } else {
            match self
                .account_service
                .account_by_name(&account_config.account_name)
                .await?
            {
                Some(_existing) => {
                    tracing::warn!(
                        account_name = %account_config.account_name,
                        "Predefined account already exists; skipping update without id",
                    );
                    return Ok(());
                }
                None => {
                    self.preflight_unique_fields(account_config).await?;
                    self.register_unknown_account(account_config).await?
                }
            }
        };

        self.set_rebac_properties(&account_id, account_config)
            .await?;

        Ok(())
    }

    async fn preflight_unique_fields(
        &self,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        // todo проверка за один запрос?

        if let Some(existing_id) = self
            .account_service
            .find_account_id_by_email(&account_config.email)
            .await
            .int_err()?
        {
            let existing = self
                .account_service
                .get_account_by_id(&existing_id)
                .await
                .int_err()?;
            return Err(PredefinedAccountUniqueConflictError {
                field: AccountDuplicateField::Email,
                configured_account_name: account_config.account_name.clone(),
                existing_account_name: existing.account_name,
                existing_account_id: existing.id,
            }
            .int_err());
        }

        let provider_identity_key = account_config.provider_identity_key();
        if let Some(existing_id) = self
            .account_service
            .find_account_id_by_provider_identity_key(&provider_identity_key)
            .await
            .int_err()?
        {
            let existing = self
                .account_service
                .get_account_by_id(&existing_id)
                .await
                .int_err()?;
            return Err(PredefinedAccountUniqueConflictError {
                field: AccountDuplicateField::ProviderIdentityKey,
                configured_account_name: account_config.account_name.clone(),
                existing_account_name: existing.account_name,
                existing_account_id: existing.id,
            }
            .int_err());
        }

        Ok(())
    }

    async fn set_rebac_properties(
        &self,
        account_id: &odf::AccountID,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        // todo: ref
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
    ) -> Result<odf::AccountID, InternalError> {
        println!("!!!2.1\n{account_config:?}");

        let created = self
            .create_account_use_case
            .execute(account_config, true /* quiet */)
            .await
            .int_err()?;

        println!("!!!2.2\n{account_config:?}");

        Ok(created.id)
    }

    async fn compare_and_maybe_update_account(
        &self,
        resolved_account_id: &odf::AccountID,
        original_account: Account,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let updated_account = Account {
            id: resolved_account_id.clone(),
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: original_account.registered_at,
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.provider_identity_key(),
        };

        if AccountProvider::is_password(&account_config.provider) {
            use VerifyPasswordError as E;

            let has_password_changed = match self
                .account_service
                .verify_account_password_by_id(resolved_account_id, &account_config.password)
                .await
            {
                Ok(_) => Ok(false),
                Err(E::IncorrectPassword(_)) => Ok(true),
                Err(
                    e @ (E::AccountNotFoundByName(_) | E::AccountNotFoundById(_) | E::Internal(_)),
                ) => Err(e.int_err()),
            }?;

            if has_password_changed {
                self.account_service
                    .modify_account_password(&updated_account.id, &account_config.password)
                    .await
                    .int_err()?;
            }
        }

        println!("!!!1.2 {}", original_account != updated_account);

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

        self.maybe_update_private_key(resolved_account_id, account_config)
            .await?;

        Ok(())
    }

    // todo action этот метод нужно пересмотреть
    async fn maybe_update_private_key(
        &self,
        account_id: &odf::AccountID,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let Some(private_key) = &account_config.private_key else {
            return Ok(());
        };

        let Some(encryption_key) = &self.did_secret_encryption_key else {
            return Ok(());
        };

        let account_id_str = account_id.as_stack_string();
        let account_entity = DidEntity::new_account(account_id_str.as_str());

        let needs_update = match self
            .did_secret_key_repo
            .get_did_secret_key(&account_entity)
            .await
        {
            Ok(stored) => {
                let decrypted = stored
                    .get_decrypted_private_key(encryption_key.expose_secret())
                    .int_err()?;
                decrypted != *private_key
            }
            Err(GetDidSecretKeyError::NotFound(_)) => true,
            Err(GetDidSecretKeyError::Internal(e)) => return Err(e),
        };

        if !needs_update {
            return Ok(());
        }

        // Repository save is insert-only; replace existing key if present.
        match self
            .did_secret_key_repo
            .delete_did_secret_key(&account_entity)
            .await
        {
            Ok(()) | Err(DeleteDidSecretKeyError::NotFound(_)) => {}
            Err(DeleteDidSecretKeyError::Internal(e)) => return Err(e),
        }

        let did_secret_key =
            DidSecretKey::try_new(private_key, encryption_key.expose_secret()).int_err()?;

        self.did_secret_key_repo
            .save_did_secret_key(&account_entity, &did_secret_key)
            .await
            .int_err()?;

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
                        "Duplicate account configuration found for account ID: {}. Skipping.",
                        account_id
                    );
                }
            }
        }

        // Process accounts in parallel using tasks
        // Note: these are heavy operations, because of password hashing, ReBAC activity
        let mut join_set = tokio::task::JoinSet::new();
        // for account_config in unique_configs.into_values() {
        for account_config in &self.predefined_accounts_config.predefined {
            let registrator = self.clone();
            let account_config_clone = account_config.clone();

            println!("!!!6: {account_config:?}");
            join_set.spawn(async move { registrator.process_account(&account_config_clone).await });
        }

        // Execute jobs in parallel
        let results = join_set.join_all().await;

        // Report errors, if any
        let mut had_errors = false;
        for result in results {
            if let Err(err) = result {
                println!("!!!7: {:#?}", err.source());

                had_errors = true;
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Failed to process predefined account",
                );
            }
        }

        // Err(InternalError::new(
        //     "One or more predefined accounts failed to register/update.",
        // ))

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

#[derive(thiserror::Error, Debug)]
#[error(
    "Cannot create predefined account '{configured_account_name}': field '{field}' already used \
     by account '{existing_account_name}' ({existing_account_id})"
)]
struct PredefinedAccountUniqueConflictError {
    field: AccountDuplicateField,
    configured_account_name: odf::AccountName,
    existing_account_name: odf::AccountName,
    existing_account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
