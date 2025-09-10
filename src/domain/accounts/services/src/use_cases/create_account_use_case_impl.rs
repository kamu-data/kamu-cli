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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
    did_secret_encryption_key: Option<SecretString>,
}

#[dill::component(pub)]
#[dill::interface(dyn CreateAccountUseCase)]
impl CreateAccountUseCaseImpl {
    #[expect(clippy::needless_pass_by_value)]
    fn new(
        account_service: Arc<dyn AccountService>,
        outbox: Arc<dyn messaging_outbox::Outbox>,
        time_source: Arc<dyn SystemTimeSource>,
        did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
        did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
    ) -> Self {
        Self {
            account_service,
            outbox,
            time_source,
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

    async fn save_account(
        &self,
        account: &Account,
        password: &Password,
    ) -> Result<(), CreateAccountError> {
        self.account_service.save_account(account).await?;

        if account.provider == AccountProvider::Password.to_string() {
            self.account_service
                .save_account_password(account, password)
                .await?;
        }

        Ok(())
    }

    async fn notify_account_created(&self, new_account: &Account) -> Result<(), InternalError> {
        use messaging_outbox::OutboxExt;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
                AccountLifecycleMessage::created(
                    new_account.id.clone(),
                    new_account.email.clone(),
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
        account: &Account,
        password: &Password,
    ) -> Result<Account, CreateAccountError> {
        self.save_account(account, password).await?;

        Ok(account.clone())
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

        let (account_key, account_id) = odf::AccountID::new_generated_ed25519();

        let account = Account {
            id: account_id,
            account_name: account_name.clone(),
            email,
            display_name: account_name.to_string(),
            account_type: AccountType::User,
            avatar_url: None,
            registered_at: self.time_source.now(),
            provider: AccountProvider::Password.to_string(),
            provider_identity_key: String::from(account_name.as_str()),
        };

        self.save_account(&account, &password).await?;

        if let Some(did_secret_encryption_key) = &self.did_secret_encryption_key {
            use odf::metadata::AsStackString;

            let account_id = account.id.as_stack_string();
            let did_secret_key = DidSecretKey::try_new(
                &account_key.into(),
                did_secret_encryption_key.expose_secret(),
            )
            .int_err()?;

            self.did_secret_key_repo
                .save_did_secret_key(
                    &DidEntity::new_account(account_id.as_str()),
                    &did_secret_key,
                )
                .await
                .int_err()?;
        }

        self.notify_account_created(&account).await?;

        Ok(account)
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
            self.notify_account_created(created_account).await?;
        }

        created_accounts.extend(found);

        Ok(created_accounts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
