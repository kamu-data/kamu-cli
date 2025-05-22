// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crypto_utils::{Argon2Hasher, PasswordHashingMode};
use email_utils::Email;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountService,
    CreateAccountError,
    CreateAccountUseCase,
    PasswordHashRepository,
};
use random_strings::AllowedSymbols;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

#[dill::component(pub)]
#[dill::interface(dyn CreateAccountUseCase)]
impl CreateAccountUseCaseImpl {
    #[allow(clippy::needless_pass_by_value)]
    fn new(
        account_service: Arc<dyn AccountService>,
        password_hash_repository: Arc<dyn PasswordHashRepository>,
        password_hashing_mode: Option<Arc<PasswordHashingMode>>,
    ) -> Self {
        Self {
            account_service,
            password_hash_repository,
            password_hashing_mode: password_hashing_mode
                .map_or(PasswordHashingMode::Default, |mode| *mode),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CreateAccountUseCase for CreateAccountUseCaseImpl {
    async fn execute(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        maybe_email: Option<Email>,
    ) -> Result<Account, CreateAccountError> {
        let email = if let Some(email) = maybe_email {
            email
        } else {
            Self::generate_email(creator_account, account_name)?
        };

        let random_password =
            random_strings::get_random_string(None, 10, &AllowedSymbols::AsciiSymbols);

        let created_account = self
            .account_service
            .create_account(account_name, email)
            .await?;

        // Save account password
        let password_hash =
            Argon2Hasher::hash_async(random_password.as_bytes(), self.password_hashing_mode)
                .await
                .int_err()?;

        self.password_hash_repository
            .save_password_hash(&created_account.id, account_name, password_hash)
            .await
            .int_err()?;

        Ok(created_account)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
