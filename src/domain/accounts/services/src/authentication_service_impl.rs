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
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use kamu_accounts::*;
use kamu_core::SystemTimeSource;
use opendatafabric::{AccountID, AccountName};

////////////////////////////////////////////////////////////////////////////////

const KAMU_JWT_ISSUER: &str = "dev.kamu";
const KAMU_JWT_ALGORITHM: Algorithm = Algorithm::HS384;
const EXPIRATION_TIME_SEC: usize = 24 * 60 * 60; // 1 day in seconds

////////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    authentication_providers_by_method: HashMap<&'static str, Arc<dyn AuthenticationProvider>>,
    account_repository: Arc<dyn AccountRepository>,
    access_token_svc: Arc<dyn AccessTokenService>,
}

////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AuthenticationService)]
impl AuthenticationServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        authentication_providers: Vec<Arc<dyn AuthenticationProvider>>,
        account_repository: Arc<dyn AccountRepository>,
        access_token_svc: Arc<dyn AccessTokenService>,
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

        Self {
            time_source,
            encoding_key: EncodingKey::from_secret(config.jwt_secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(config.jwt_secret.as_bytes()),
            authentication_providers_by_method,
            account_repository,
            access_token_svc,
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
        account_id: &AccountID,
        expiration_time_sec: usize,
    ) -> Result<String, InternalError> {
        let current_time = self.time_source.now();
        let iat = usize::try_from(current_time.timestamp()).unwrap();
        let exp = iat + expiration_time_sec;

        let claims = JWTClaims {
            iat,
            exp,
            iss: String::from(KAMU_JWT_ISSUER),
            sub: account_id.to_string(),
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
    ) -> Result<AccessTokenType, AccessTokenError> {
        if &access_token[..2] == ACCESS_TOKEN_PREFIX {
            return KamuAccessToken::decode(access_token)
                .map_err(|err| AccessTokenError::Invalid(Box::new(err)))
                .map(AccessTokenType::KamuAccessToken);
        }
        let mut validation = Validation::new(KAMU_JWT_ALGORITHM);
        validation.set_issuer(&[KAMU_JWT_ISSUER]);

        decode::<JWTClaims>(access_token, &self.decoding_key, &validation)
            .map_err(|e| match *e.kind() {
                ErrorKind::ExpiredSignature => AccessTokenError::Expired,
                _ => AccessTokenError::Invalid(Box::new(e)),
            })
            .map(AccessTokenType::JWTToken)
    }

    pub async fn account_by_token_impl(
        &self,
        access_token: &str,
    ) -> Result<Account, GetAccountInfoError> {
        let decoded_access_token = self
            .decode_access_token(access_token)
            .map_err(GetAccountInfoError::AccessToken)?;

        match decoded_access_token {
            AccessTokenType::JWTToken(token_data) => {
                let account_id = AccountID::from_did_str(&token_data.claims.sub)
                    .map_err(|e| GetAccountInfoError::Internal(e.int_err()))?;

                match self.account_by_id(&account_id).await {
                    Ok(Some(account)) => Ok(account),
                    Ok(None) => Err(GetAccountInfoError::AccountUnresolved),
                    Err(e) => Err(GetAccountInfoError::Internal(e)),
                }
            }
            AccessTokenType::KamuAccessToken(kamu_access_token) => self
                .access_token_svc
                .find_account_by_active_token_id(
                    &kamu_access_token.id,
                    kamu_access_token.random_bytes_hash,
                )
                .await
                .map_err(|err| GetAccountInfoError::Internal(err.int_err())),
        }
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
        // Resolve provider via specified login method
        let provider = self.resolve_authentication_provider(login_method)?;

        // Attempt to login via provider
        let provider_response = provider.login(login_credentials_json).await?;

        // Try to resolve existing account via provider's identity key
        let maybe_account_id = self
            .account_repository
            .find_account_id_by_provider_identity_key(&provider_response.provider_identity_key)
            .await?;

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
            access_token: self.make_access_token(&account_id, EXPIRATION_TIME_SEC)?,
            account_id,
            account_name: provider_response.account_name,
        })
    }

    async fn account_by_token(&self, access_token: String) -> Result<Account, GetAccountInfoError> {
        self.account_by_token_impl(&access_token).await
    }

    async fn account_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Option<Account>, InternalError> {
        match self.account_repository.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account.clone())),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }

    async fn accounts_by_ids(
        &self,
        account_ids: Vec<AccountID>,
    ) -> Result<Vec<Account>, InternalError> {
        self.account_repository
            .get_accounts_by_ids(account_ids)
            .await
            .int_err()
    }

    async fn account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<Account>, InternalError> {
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
        match self.account_repository.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account.account_name.clone())),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }
}
