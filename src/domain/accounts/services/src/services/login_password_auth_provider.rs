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

use crypto_utils::{Argon2Hasher, Hasher, PasswordHashingMode};
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
        account_name: &odf::AccountName,
        password: String,
    ) -> Result<(), InternalError> {
        // Copy hashing mod
        let hashing_mode = self.password_hashing_mode;

        // Generate password hash: this is a compute-intensive operation,
        // so spawn a blocking task
        let password_hash = tokio::task::spawn_blocking(move || {
            tracing::info_span!("Generate password hash").in_scope(|| {
                let argon2_hasher = Argon2Hasher::new(hashing_mode);
                argon2_hasher.hash(password.as_bytes()).int_err()
            })
        })
        .await
        .int_err()??;

        // Save hash in the repository
        self.password_hash_repository
            .save_password_hash(account_name, password_hash)
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for LoginPasswordAuthProvider {
    fn provider_name(&self) -> &'static str {
        PROVIDER_PASSWORD
    }

    fn generate_id(&self, account_name: &odf::AccountName) -> odf::AccountID {
        // For passwords, use an ID based on name
        odf::AccountID::new_seeded_ed25519(account_name.as_bytes())
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        // Decode credentials
        let password_login_credentials =
            serde_json::from_str::<PasswordLoginCredentials>(login_credentials_json.as_str())
                .map_err(|e| {
                    ProviderLoginError::InvalidCredentials(InvalidCredentialsError::new(Box::new(
                        e,
                    )))
                })?;

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

        // Copy hashing mode
        let hashing_mode = self.password_hashing_mode;

        // Verify password hash: this is a compute-intensive operation,
        // so spawn a blocking task
        tokio::task::spawn_blocking(move || {
            tracing::info_span!("Verify password hash").in_scope(|| {
                let argon2_hasher = Argon2Hasher::new(hashing_mode);
                argon2_hasher
                    .verify(
                        password_login_credentials.password.as_bytes(),
                        password_hash.as_str(),
                    )
                    .map_err(|_| {
                        ProviderLoginError::RejectedCredentials(RejectedCredentialsError {})
                    })
            })
        })
        .await
        .int_err()??;

        // Extract known account data
        let account = self
            .account_repository
            .get_account_by_name(&account_name)
            .await
            .int_err()?;

        Ok(ProviderLoginResponse {
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
