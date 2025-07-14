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

use email_utils::Email;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountLifecycleMessage,
    AccountService,
    CreateAccountError,
    CreateAccountUseCase,
    CreateAccountUseCaseOptions,
    CreateMultiWalletAccountsError,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    Password,
};
use odf::metadata::DidPkh;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn CreateAccountUseCase)]
pub struct CreateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    outbox: Arc<dyn messaging_outbox::Outbox>,
}

impl CreateAccountUseCaseImpl {
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

        let created_account = self
            .account_service
            .create_password_account(account_name, password, email)
            .await?;

        self.notify_account_created(&created_account).await?;

        Ok(created_account)
    }

    async fn execute_multi_wallet_accounts(
        &self,
        mut wallet_addresses: HashSet<DidPkh>,
    ) -> Result<Vec<Account>, CreateMultiWalletAccountsError> {
        let account_ids = wallet_addresses
            .iter()
            .map(|wa| wa.clone().into())
            .collect::<Vec<odf::AccountID>>();
        let existing_accounts = self
            .account_service
            .get_accounts_by_ids(&account_ids)
            .await?;

        if existing_accounts.len() == wallet_addresses.len() {
            return Ok(existing_accounts);
        }

        for existing_account in &existing_accounts {
            // SAFETY: input IDs are originally did:pkh
            let did_pkh = existing_account.id.as_did_pkh().unwrap();
            wallet_addresses.remove(did_pkh);
        }

        let mut created_accounts = Vec::new();
        for wallet_address in wallet_addresses {
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

        created_accounts.extend(existing_accounts);

        Ok(created_accounts)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
