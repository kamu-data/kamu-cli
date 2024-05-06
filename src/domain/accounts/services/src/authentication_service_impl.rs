// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;

use chrono::Utc;
use dill::*;
use internal_error::*;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{
    decode,
    encode,
    Algorithm,
    DecodingKey,
    EncodingKey,
    Header,
    TokenData,
    Validation,
};
use kamu_accounts::*;
use kamu_core::SystemTimeSource;
use opendatafabric::{AccountID, AccountName};
use serde::{Deserialize, Serialize};

use crate::LoginPasswordAuthProvider;

///////////////////////////////////////////////////////////////////////////////

const KAMU_JWT_ISSUER: &str = "dev.kamu";
const KAMU_JWT_ALGORITHM: Algorithm = Algorithm::HS384;
const EXPIRATION_TIME_SEC: usize = 24 * 60 * 60; // 1 day in seconds

///////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceImpl {
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    time_source: Arc<dyn SystemTimeSource>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    authentication_providers_by_method: HashMap<&'static str, Arc<dyn AuthenticationProvider>>,
    login_password_auth_provider: Option<Arc<LoginPasswordAuthProvider>>,
    account_repository: Arc<dyn AccountRepository>,
    state: tokio::sync::Mutex<State>,
}

#[derive(Default)]
struct State {
    predefined_accounts_registered: bool,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AuthenticationService)]
impl AuthenticationServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        authentication_providers: Vec<Arc<dyn AuthenticationProvider>>,
        login_password_auth_provider: Option<Arc<LoginPasswordAuthProvider>>,
        account_repository: Arc<dyn AccountRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        config: Arc<JwtAuthenticationConfig>,
    ) -> Self {
        let mut authentication_providers_by_method = HashMap::new();

        for authentication_provider in authentication_providers {
            let login_method = authentication_provider.provider_name();

            let insert_result = authentication_providers_by_method
                .insert(login_method, authentication_provider.clone());

            assert!(
                insert_result.is_none(),
                "Duplicate authentication provider for method {login_method}"
            );
        }

        let jwt_secret = config.jwt_secret();

        Self {
            predefined_accounts_config,
            time_source,
            encoding_key: EncodingKey::from_secret(jwt_secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(jwt_secret.as_bytes()),
            authentication_providers_by_method,
            login_password_auth_provider,
            account_repository,
            state: tokio::sync::Mutex::new(State::default()),
        }
    }

    fn resolve_authentication_provider(
        &self,
        login_method: &str,
    ) -> Result<Arc<dyn AuthenticationProvider>, UnsupportedLoginMethodError> {
        match self.authentication_providers_by_method.get(login_method) {
            Some(provider) => Ok(provider.clone()),
            None => Err(UnsupportedLoginMethodError {
                method: login_method.into(),
            }),
        }
    }

    pub fn make_access_token(
        &self,
        subject: String,
        expiration_time_sec: usize,
    ) -> Result<String, InternalError> {
        let current_time = self.time_source.now();
        let iat = usize::try_from(current_time.timestamp()).unwrap();
        let exp = iat + expiration_time_sec;
        let claims = KamuAccessTokenClaims {
            iat,
            exp,
            iss: String::from(KAMU_JWT_ISSUER),
            sub: subject,
        };

        encode(
            &Header::new(KAMU_JWT_ALGORITHM),
            &claims,
            &self.encoding_key,
        )
        .int_err()
    }

    pub fn decode_access_token(
        &self,
        access_token: &str,
    ) -> Result<TokenData<KamuAccessTokenClaims>, AccessTokenError> {
        let mut validation = Validation::new(KAMU_JWT_ALGORITHM);
        validation.set_issuer(&[KAMU_JWT_ISSUER]);

        decode::<KamuAccessTokenClaims>(access_token, &self.decoding_key, &validation).map_err(
            |e| match *e.kind() {
                ErrorKind::ExpiredSignature => AccessTokenError::Expired,
                _ => AccessTokenError::Invalid(Box::new(e)),
            },
        )
    }

    async fn ensure_predefined_accounts_registration(&self) -> Result<(), InternalError> {
        let mut guard = self.state.lock().await;
        if !guard.predefined_accounts_registered {
            // Register predefined accounts on first request
            for account_config in &self.predefined_accounts_config.predefined {
                let account_id = account_config.get_id();
                let is_unknown = match self.account_repository.get_account_by_id(&account_id).await
                {
                    Ok(_) => Ok(false),
                    Err(GetAccountByIdError::NotFound(_)) => Ok(true),
                    Err(GetAccountByIdError::Internal(e)) => Err(e),
                }?;

                if is_unknown {
                    let account = Account {
                        id: account_config.get_id(),
                        account_name: account_config.account_name.clone(),
                        email: account_config.email.clone(),
                        display_name: account_config.get_display_name(),
                        account_type: account_config.account_type,
                        avatar_url: account_config.avatar_url.clone(),
                        registered_at: account_config.registered_at,
                        is_admin: account_config.is_admin,
                        provider: account_config.provider.clone(),
                        provider_identity_key: account_config.account_name.to_string(),
                    };

                    self.account_repository
                        .create_account(&account)
                        .await
                        .map_err(|e| match e {
                            CreateAccountError::Duplicate(e) => e.int_err(),
                            CreateAccountError::Internal(e) => e,
                        })?;

                    if let Some(login_password_auth_provider) =
                        self.login_password_auth_provider.as_ref()
                        && account_config.provider == PROVIDER_PASSWORD
                    {
                        login_password_auth_provider
                            .save_password(&account.account_name, account_config.get_password())
                            .await?;
                    }
                }
            }

            guard.predefined_accounts_registered = true;
        }
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationService for AuthenticationServiceImpl {
    fn supported_login_methods(&self) -> Vec<&'static str> {
        self.authentication_providers_by_method
            .keys()
            .copied()
            .collect()
    }

    async fn login(
        &self,
        login_method: &str,
        login_credentials_json: String,
    ) -> Result<LoginResponse, LoginError> {
        self.ensure_predefined_accounts_registration().await?;

        // Resolve provider via specified login method
        let provider = self.resolve_authentication_provider(login_method)?;

        // Attempt to login via provider
        let provider_response = provider.login(login_credentials_json).await.map_err(
            |e: ProviderLoginError| match e {
                ProviderLoginError::RejectedCredentials(e) => LoginError::RejectedCredentials(e),
                ProviderLoginError::InvalidCredentials(e) => LoginError::InvalidCredentials(e),
                ProviderLoginError::Internal(e) => LoginError::Internal(e),
            },
        )?;

        // Try to resolve existing account via provider's identity key
        let maybe_account_id = self
            .account_repository
            .find_account_id_by_provider_identity_key(&provider_response.provider_identity_key)
            .await
            .map_err(|e| match e {
                FindAccountIdByProviderIdentityKeyError::Internal(e) => LoginError::Internal(e),
            })?;

        let account_id = match maybe_account_id {
            // Account already exists
            Some(account_id) => account_id,

            // Account does not exist and needs to be created
            None => {
                // Generate identity: temporarily we do this via provider, but revise in future
                let account_id = provider.generate_id(&provider_response.account_name);

                // Create a new account
                let new_account = Account {
                    id: account_id,
                    account_name: provider_response.account_name.clone(),
                    email: provider_response.email,
                    display_name: provider_response.display_name,
                    account_type: provider_response.account_type,
                    avatar_url: provider_response.avatar_url,
                    registered_at: Utc::now(),
                    is_admin: false,
                    provider: String::from(login_method),
                    provider_identity_key: provider_response.provider_identity_key,
                };

                // Register account in the repository
                self.account_repository
                    .create_account(&new_account)
                    .await
                    .map_err(|e| match e {
                        CreateAccountError::Duplicate(_) => LoginError::DuplicateCredentials,
                        CreateAccountError::Internal(e) => LoginError::Internal(e),
                    })?;
                new_account.id
            }
        };

        // Create access token and attach basic identity properties
        Ok(LoginResponse {
            access_token: self.make_access_token(account_id.to_string(), EXPIRATION_TIME_SEC)?,
            account_id,
            account_name: provider_response.account_name,
        })
    }

    async fn account_by_token(&self, access_token: String) -> Result<Account, GetAccountInfoError> {
        self.ensure_predefined_accounts_registration()
            .await
            .map_err(GetAccountInfoError::Internal)?;

        let decoded_access_token = self
            .decode_access_token(&access_token)
            .map_err(GetAccountInfoError::AccessToken)?;

        let account_id = AccountID::from_did_str(&decoded_access_token.claims.sub)
            .map_err(|e| GetAccountInfoError::Internal(e.int_err()))?;

        match self.account_by_id(&account_id).await {
            Ok(Some(account)) => Ok(account),
            Ok(None) => Err(GetAccountInfoError::AccountUnresolved),
            Err(e) => Err(GetAccountInfoError::Internal(e)),
        }
    }

    async fn account_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Option<Account>, InternalError> {
        self.ensure_predefined_accounts_registration().await?;

        match self.account_repository.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account.clone())),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }

    async fn account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<Account>, InternalError> {
        self.ensure_predefined_accounts_registration().await?;

        match self
            .account_repository
            .get_account_by_name(account_name)
            .await
        {
            Ok(account) => Ok(Some(account.clone())),
            Err(GetAccountByNameError::NotFound(_)) => Ok(None),
            Err(GetAccountByNameError::Internal(e)) => Err(e),
        }
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, InternalError> {
        self.ensure_predefined_accounts_registration().await?;

        match self
            .account_repository
            .find_account_id_by_name(account_name)
            .await
        {
            Ok(maybe_account_id) => Ok(maybe_account_id),
            Err(FindAccountIdByNameError::Internal(e)) => Err(e),
        }
    }

    async fn find_account_name_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Option<AccountName>, InternalError> {
        self.ensure_predefined_accounts_registration().await?;

        match self.account_repository.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account.account_name.clone())),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub struct KamuAccessTokenClaims {
    exp: usize,
    iat: usize,
    iss: String,
    sub: String,
}

///////////////////////////////////////////////////////////////////////////////
