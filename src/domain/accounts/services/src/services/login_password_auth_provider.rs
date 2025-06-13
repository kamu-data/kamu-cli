// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use serde::{Deserialize, Serialize};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
pub struct LoginPasswordAuthProvider {
    account_service: Arc<dyn AccountService>,
}

impl LoginPasswordAuthProvider {
    // TODO: Remove this method during refactoring:
    //       https://github.com/kamu-data/kamu-cli/issues/1270
    pub async fn save_password(
        &self,
        account: &Account,
        password: Password,
    ) -> Result<(), InternalError> {
        self.account_service
            .save_account_password(account, &password)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for LoginPasswordAuthProvider {
    fn provider_name(&self) -> &'static str {
        AccountProvider::Password.into()
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        // Decode credentials
        let password_login_credentials =
            serde_json::from_str::<PasswordLoginCredentials>(login_credentials_json.as_str())
                .map_err(ProviderLoginError::invalid_credentials)?;

        // Extract account name and password
        let account_name =
            odf::AccountName::from_str(&password_login_credentials.login).int_err()?;
        let password = Password::try_new(password_login_credentials.password).int_err()?;

        self.account_service
            .verify_account_password(&account_name, &password)
            .await
            .map_err(|e| {
                use VerifyPasswordError as E;
                match e {
                    E::AccountNotFound(_) | E::IncorrectPassword(_) => {
                        ProviderLoginError::RejectedCredentials(RejectedCredentialsError {})
                    }
                    e @ E::Internal(_) => ProviderLoginError::Internal(e.int_err()),
                }
            })?;

        // Extract known account data
        let account = self
            .account_service
            .get_account_by_name(&account_name)
            .await
            .int_err()?;

        Ok(ProviderLoginResponse {
            // For password-based accounts
            account_id: odf::AccountID::new_seeded_ed25519(account_name.as_bytes()),
            account_name,
            email: account.email.clone(),
            display_name: password_login_credentials.login.clone(),
            account_type: account.account_type,
            avatar_url: account.avatar_url.clone(),
            provider_identity_key: password_login_credentials.login.to_ascii_lowercase(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordLoginCredentials {
    pub login: String,
    pub password: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
