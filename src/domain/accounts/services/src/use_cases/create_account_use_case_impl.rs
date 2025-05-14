// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use email_utils::Email;
use internal_error::ResultIntoInternal;
use kamu_accounts::{Account, AccountService, CreateAccountError, CreateAccountUseCase, Password};
use random_strings::AllowedSymbols;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn CreateAccountUseCase)]
pub struct CreateAccountUseCaseImpl {
    account_service: Arc<dyn AccountService>,
}

#[async_trait::async_trait]
impl CreateAccountUseCase for CreateAccountUseCaseImpl {
    async fn execute(
        &self,
        creator_account: &Account,
        account_name: &odf::AccountName,
        email_maybe: Option<Email>,
    ) -> Result<Account, CreateAccountError> {
        let email = email_maybe.unwrap_or_else(|| {
            let parent_host = creator_account.email.host();
            let email_str = format!(
                "{}+{}@{}",
                creator_account.account_name, account_name, parent_host
            );

            Email::parse(&email_str).int_err()?
        });

        let random_password =
            random_strings::get_random_string(None, 10, &AllowedSymbols::AsciiSymbols);

        self.account_service
            .create_account(
                account_name,
                email,
                Password::try_new(&random_password).unwrap(),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
