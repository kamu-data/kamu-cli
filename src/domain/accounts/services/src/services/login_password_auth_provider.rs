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

use crypto_utils::{Argon2Hasher, PasswordHashingMode};
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use serde::{Deserialize, Serialize};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginPasswordAuthProvider {
    account_repository: Arc<dyn AccountRepository>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

#[component(pub)]
#[interface(dyn AuthenticationProvider)]
impl LoginPasswordAuthProvider {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        account_repository: Arc<dyn AccountRepository>,
        password_hash_repository: Arc<dyn PasswordHashRepository>,
        password_hashing_mode: Option<Arc<PasswordHashingMode>>,
    ) -> Self {
        Self {
            account_repository,
            password_hash_repository,
            // When hashing mode is unspecified, safely assume default mode.
            // Higher security by default is better than forgetting to configure
            password_hashing_mode: password_hashing_mode
                .map_or(PasswordHashingMode::Default, |mode| *mode),
        }
    }

    pub async fn save_password(
        &self,
        account: &Account,
        password: String,
    ) -> Result<(), InternalError> {
        let password_hash =
            Argon2Hasher::hash_async(password.as_bytes(), self.password_hashing_mode)
                .await
                .int_err()?;

        // Save hash in the repository
        self.password_hash_repository
            .save_password_hash(&account.id, &account.account_name, password_hash)
            .await
            .int_err()?;

        Ok(())
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

        // Extract account name
        let account_name =
            odf::AccountName::from_str(&password_login_credentials.login).int_err()?;

        // Locate password hash associated with this account name
        let password_hash = match self
            .password_hash_repository
            .find_password_hash_by_account_name(&account_name)
            .await
        {
            // Found
            Ok(Some(password_hash)) => password_hash,

            // Not found => error
            Ok(None) => {
                return Err(ProviderLoginError::RejectedCredentials(
                    RejectedCredentialsError {},
                ));
            }

            // Internal issue => error
            Err(e) => match e {
                FindPasswordHashError::Internal(e) => return Err(ProviderLoginError::Internal(e)),
            },
        };

        if !Argon2Hasher::verify_async(
            password_login_credentials.password.as_bytes(),
            password_hash.as_str(),
            self.password_hashing_mode,
        )
        .await
        .int_err()?
        {
            return Err(ProviderLoginError::RejectedCredentials(
                RejectedCredentialsError {},
            ));
        }

        // Extract known account data
        let account = self
            .account_repository
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
