// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::BatchLookup;
use email_utils::Email;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountConfig,
    AccountIdentityGenerator,
    AccountLifecycleMessage,
    AccountProvider,
    AccountService,
    AccountType,
    CreateAccountError,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    CreateMultiWalletAccountsError,
    DidEntity,
    DidSecretEncryptionConfig,
    DidSecretKey,
    DidSecretKeyRepository,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    Password,
};
use odf::metadata::DidPkh;
use secrecy::{ExposeSecret, SecretString};
use time_source::SystemTimeSource;
use tokio::try_join;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
    account_identity_generator: Arc<dyn AccountIdentityGenerator>,
    // todo refactor: extract to service -->
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
    did_secret_encryption_key: Option<SecretString>,
    // <--
}

#[dill::component(pub)]
#[dill::interface(dyn CreateAccountUseCase)]
impl CreateAccountUseCaseImpl {
    #[expect(clippy::needless_pass_by_value)]
    fn new(
        account_service: Arc<dyn AccountService>,
        outbox: Arc<dyn messaging_outbox::Outbox>,
        time_source: Arc<dyn SystemTimeSource>,
        account_identity_generator: Arc<dyn AccountIdentityGenerator>,
        did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
        did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
    ) -> Self {
        Self {
            account_service,
            outbox,
            time_source,
            account_identity_generator,
            did_secret_encryption_key: did_secret_encryption_config
                .encryption_key
                .as_ref()
                .map(|encryption_key| SecretString::from(encryption_key.clone())),
            did_secret_key_repo,
        }
    }

    fn generate_email(
        creator_account: &Account,
        account_name: &odf::AccountName,
    ) -> Result<Email, InternalError> {
        let parent_host = creator_account.email.host();
        let email_str = format!(
            "{}+{}@{}",
            creator_account.account_name, account_name, parent_host
        );

        Email::parse(&email_str).int_err()
    }

    fn generate_password() -> Result<Password, InternalError> {
        const RANDOM_PASSWORD_LENGTH: usize = 16;

        let random_password = random_strings::get_random_string(
            None,
            RANDOM_PASSWORD_LENGTH,
            &random_strings::AllowedSymbols::AsciiSymbols,
        );

        Password::try_new(random_password).int_err()
    }

    async fn maybe_save_private_key(
        &self,
        account_id: &odf::AccountID,
        maybe_account_key: Option<odf::metadata::SigningKey>,
    ) -> Result<(), InternalError> {
        // No key, nothing to do
        let Some(account_key) = maybe_account_key else {
            return Ok(());
        };

        let Some(did_secret_encryption_key) = &self.did_secret_encryption_key else {
            return Ok(());
        };

        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();
        let did_secret_key = DidSecretKey::try_new(
            &account_key.into(),
            did_secret_encryption_key.expose_secret(),
        )
        .int_err()?;
        let account_entity = DidEntity::new_account(account_id.as_str());

        self.did_secret_key_repo
            .save_did_secret_key(&account_entity, &did_secret_key)
            .await
            .int_err()
    }

    fn resolve_account_key_and_id(
        &self,
        account_config: &AccountConfig,
    ) -> (Option<odf::metadata::SigningKey>, odf::AccountID) {
        if let Some(id) = account_config.resolve_account_id() {
            // if there is an ID, we use it and the private key, if specified. ...
            let maybe_account_key = account_config.private_key.clone().map(Into::into);
            (maybe_account_key, id)
        } else {
            // ... Otherwise, create a new pair
            let (account_key, account_id) = self
                .account_identity_generator
                .generate_ed25519(&account_config.account_name);

            (Some(account_key), account_id)
        }
    }

    async fn notify_account_created(&self, new_account: &Account) -> Result<(), InternalError> {
        use messaging_outbox::OutboxExt;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::created(
                    new_account.registered_at,
                    new_account.id.clone(),
                    new_account.email.clone(),
                    new_account.account_name.clone(),
                    new_account.display_name.clone(),
                ),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CreateAccountUseCase for CreateAccountUseCaseImpl {
    async fn execute(
        &self,
        account_config: &AccountConfig,
        quiet: bool,
    ) -> Result<Account, CreateAccountError> {
        let (maybe_account_key, account_id) = self.resolve_account_key_and_id(account_config);

        let new_account = Account {
            id: account_id,
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: account_config
                .registered_at
                .unwrap_or_else(|| self.time_source.now()),
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.provider_identity_key(),
        };

        self.account_service.save_account(&new_account).await?;

        try_join!(
            async {
                if AccountProvider::is_password(&new_account.provider) {
                    self.account_service
                        .save_account_password(&new_account.id, &account_config.password)
                        .await
                } else {
                    Ok(())
                }
            },
            self.maybe_save_private_key(&new_account.id, maybe_account_key)
        )?;

        if !quiet {
            self.notify_account_created(&new_account).await?;
        }

        Ok(new_account)
    }

    async fn execute_derived(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        options: CreateAccountUseCaseOptions,
    ) -> Result<Account, CreateAccountError> {
        let email = if let Some(email) = options.email {
            email
        } else {
            Self::generate_email(creator_account, account_name)?
        };

        let password = if let Some(password) = options.password {
            password
        } else {
            Self::generate_password()?
        };

        let (account_key, account_id) = self
            .account_identity_generator
            .generate_ed25519(account_name);

        let new_account = Account {
            id: account_id,
            account_name: account_name.clone(),
            email,
            display_name: account_name.to_string(),
            account_type: AccountType::User,
            avatar_url: options.avatar_url,
            registered_at: self.time_source.now(),
            provider: AccountProvider::Password.to_string(),
            provider_identity_key: account_name.to_string(),
        };

        self.account_service.save_account(&new_account).await?;

        try_join!(
            self.account_service
                .save_account_password(&new_account.id, &password),
            self.maybe_save_private_key(&new_account.id, Some(account_key))
        )?;

        self.notify_account_created(&new_account).await?;

        Ok(new_account)
    }

    async fn execute_multi_wallet_accounts(
        &self,
        wallet_addresses: HashSet<DidPkh>,
    ) -> Result<Vec<Account>, CreateMultiWalletAccountsError> {
        let account_ids = wallet_addresses
            .into_iter()
            .map(Into::into)
            .collect::<Vec<odf::AccountID>>();
        let account_ids_refs = account_ids.iter().collect::<Vec<_>>();

        let BatchLookup { found, not_found } = self
            .account_service
            .get_accounts_by_ids(&account_ids_refs)
            .await?;

        if not_found.is_empty() {
            return Ok(found);
        }

        let not_found_wallet_addresses = not_found
            .into_iter()
            .map(|(account_id, _e)| {
                // SAFETY: accounts IDs are originally did:pkh
                let odf::AccountID::Pkh(did_pkh) = account_id else {
                    unreachable!();
                };
                did_pkh
            })
            .collect::<Vec<_>>();

        let mut created_accounts = Vec::with_capacity(not_found_wallet_addresses.len());
        for wallet_address in not_found_wallet_addresses {
            let created_account = self
                .account_service
                .create_wallet_account(&wallet_address)
                .await
                .int_err()?;

            created_accounts.push(created_account);
        }

        for created_account in &created_accounts {
            // TODO: PEFF: batch message
            self.notify_account_created(created_account).await?;
        }

        created_accounts.extend(found);

        Ok(created_accounts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
