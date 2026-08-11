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

    async fn resolve_account_ids(&self) -> Result<AccountIdsResolution, InternalError> {
        use futures::future::try_join_all;

        // NOTE: PERF: io-bound futures so `tokio::Task`s are unneeded
        let resolutions: Vec<_> =
            try_join_all(self.predefined_accounts_config.predefined.iter().map(
                |account_config| async move {
                    let maybe_resolved_account_id =
                        account_config.get_account_id_from_config_or_private_key();

                    if let Some(resolved_account_id) = maybe_resolved_account_id {
                        // We have explicit ID -- just use it
                        return Ok(AccountIdResolution::Resolved((
                            resolved_account_id,
                            account_config.clone(),
                        )));
                    }

                    let mut account_ids = self
                        .account_service
                        .find_account_ids_by_one_of_unique_fields(
                            &account_config.account_name,
                            &account_config.email,
                            &account_config.provider_identity_key(),
                        )
                        .await
                        .int_err()?;
                    let result = match account_ids.len() {
                        0 => {
                            // Vacant space for insertion
                            AccountIdResolution::Unresolved(account_config.clone())
                        }
                        1 => {
                            // Exact match: an account has most likely already been created.
                            let account_id = account_ids.swap_remove(0);
                            AccountIdResolution::Resolved((account_id, account_config.clone()))
                        }
                        _ => {
                            let account_name = account_config.account_name.clone();
                            AccountIdResolution::Conflicted(AccountUniqueFieldsConflictError {
                                account_name,
                                account_ids,
                            })
                        }
                    };

                    Ok::<_, InternalError>(result)
                },
            ))
            .await?;

        // If there are duplicates by account ID, skip them.
        // This could happen i.e., when a predefined user gets renamed,
        // but the implicit CLI config for the current user still points to the same ID
        let mut account_config_by_id = HashMap::new();
        let mut unresolved = Vec::new();
        let mut conflicted = Vec::new();

        for result in resolutions {
            match result {
                AccountIdResolution::Resolved((account_id, account_config)) => {
                    match account_config_by_id.entry(account_id.clone()) {
                        Entry::Vacant(entry) => {
                            entry.insert(account_config.clone());
                        }
                        Entry::Occupied(entry) => {
                            let previously_stored_config = entry.get();
                            let stored_account_name = &previously_stored_config.account_name;
                            let duplicate_account_name = account_config.account_name;

                            tracing::warn!(
                                %stored_account_name,
                                %duplicate_account_name,
                                %account_id,
                                "Duplicate account configuration found. Skipping",
                            );
                        }
                    }
                }
                AccountIdResolution::Unresolved(ac) => unresolved.push(ac),
                AccountIdResolution::Conflicted(e) => conflicted.push(e),
            }
        }

        Ok(AccountIdsResolution {
            resolved: account_config_by_id,
            unresolved,
            conflicted,
        })
    }

    async fn process_account(
        &self,
        maybe_account_id: Option<odf::AccountID>,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let account_id = if let Some(account_id) = maybe_account_id {
            match self.account_service.get_account_by_id(&account_id).await {
                Ok(original_account) => {
                    // 1) An account has an ID set to it and was found -- update it
                    self.compare_and_maybe_update_account(
                        &account_id,
                        original_account,
                        account_config,
                    )
                    .await?;
                    account_id
                }
                Err(GetAccountByIdError::NotFound(_)) => {
                    // 2) An account has an ID set to it but not found -- register it
                    self.register_unknown_account(account_config).await?
                }
                Err(e) => return Err(e.int_err()),
            }
        } else {
            // 3) We were previously unable to find the ID based on the fields, which means
            //    an account does not exist. Register it
            self.register_unknown_account(account_config).await?
        };

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
    ) -> Result<odf::AccountID, InternalError> {
        let created = self
            .create_account_use_case
            .execute(account_config)
            .await
            .int_err()?;

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

        use odf::metadata::AsStackString;

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
        // Pre-flight checks
        self.predefined_accounts_config.validate().int_err()?;

        // Resolve account IDs for predefined accounts w/o IDs
        let account_ids_resolution = self.resolve_account_ids().await?;

        // Log configs with conflicting fields
        for e in account_ids_resolution.conflicted {
            tracing::warn!(
                error = ?e,
                error_msg = %e,
                "Skip a predefined account w/ potentially conflicting fields. Skipping",
            );
        }

        // Process accounts in parallel using tasks
        // Note: these are heavy operations, because of password hashing, ReBAC activity
        let resolved_iter = account_ids_resolution
            .resolved
            .into_iter()
            .map(|(account_id, account_config)| (Some(account_id), account_config));
        let unresolved_iter = account_ids_resolution
            .unresolved
            .into_iter()
            .map(|account_config| (None, account_config));

        let account_configs_iter = resolved_iter.chain(unresolved_iter);

        let mut join_set = tokio::task::JoinSet::new();
        for (maybe_account_id, account_config) in account_configs_iter {
            let registrator = self.clone();

            join_set.spawn(async move {
                registrator
                    .process_account(maybe_account_id, &account_config)
                    .await
            });
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

enum AccountIdResolution {
    Resolved((odf::AccountID, AccountConfig)),
    Unresolved(AccountConfig),
    Conflicted(AccountUniqueFieldsConflictError),
}

struct AccountIdsResolution {
    pub resolved: HashMap<odf::AccountID, AccountConfig>,
    pub unresolved: Vec<AccountConfig>,
    pub conflicted: Vec<AccountUniqueFieldsConflictError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error(
    "Account '{account_name}': found more than one account with the same unique fields: {}",
    format_utils::format_collection(account_ids)
)]
pub struct AccountUniqueFieldsConflictError {
    pub account_name: odf::AccountName,
    pub account_ids: Vec<odf::AccountID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
