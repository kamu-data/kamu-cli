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
use messaging_outbox::{Outbox, OutboxExt};
use odf::dataset::DUMMY_ODF_ACCESS_TOKEN;
use thiserror::Error;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const KAMU_JWT_ISSUER: &str = "dev.kamu";
const KAMU_JWT_ALGORITHM: Algorithm = Algorithm::HS384;
const EXPIRATION_TIME_SEC: usize = 24 * 60 * 60; // 1 day in seconds

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    authentication_providers_by_method: HashMap<&'static str, Arc<dyn AuthenticationProvider>>,
    account_repository: Arc<dyn AccountRepository>,
    access_token_svc: Arc<dyn AccessTokenService>,
    outbox: Arc<dyn Outbox>,
    maybe_dummy_token_account: Option<Account>,
    oauth_device_code_service: Arc<dyn OAuthDeviceCodeService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AuthenticationService)]
#[interface(dyn JwtTokenIssuer)]
impl AuthenticationServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        authentication_providers: Vec<Arc<dyn AuthenticationProvider>>,
        account_repository: Arc<dyn AccountRepository>,
        access_token_svc: Arc<dyn AccessTokenService>,
        time_source: Arc<dyn SystemTimeSource>,
        config: Arc<JwtAuthenticationConfig>,
        outbox: Arc<dyn Outbox>,
        oauth_device_code_service: Arc<dyn OAuthDeviceCodeService>,
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
            outbox,
            oauth_device_code_service,
            maybe_dummy_token_account: config.maybe_dummy_token_account.clone(),
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

    fn generate_jwt_claims(
        &self,
        account_id: &odf::AccountID,
        expiration_time_sec: usize,
    ) -> JWTClaims {
        let current_time = self.time_source.now();
        let iat = usize::try_from(current_time.timestamp()).unwrap();
        let exp = iat + expiration_time_sec;

        JWTClaims {
            iat,
            exp,
            iss: KAMU_JWT_ISSUER.to_string(),
            sub: account_id.to_string(),
        }
    }

    fn make_access_token_from_jwt_claims(
        &self,
        claims: &JWTClaims,
    ) -> Result<JwtAccessToken, InternalError> {
        let token =
            encode(&Header::new(KAMU_JWT_ALGORITHM), claims, &self.encoding_key).int_err()?;

        JwtAccessToken::try_new(token).map(Ok).unwrap()
    }

    fn make_access_token_from_account_id_impl(
        &self,
        account_id: &odf::AccountID,
        expiration_time_sec: usize,
    ) -> Result<JwtAccessToken, InternalError> {
        let claims = self.generate_jwt_claims(account_id, expiration_time_sec);

        self.make_access_token_from_jwt_claims(&claims)
    }

    pub fn decode_access_token(
        &self,
        access_token: &str,
    ) -> Result<AccessTokenType, AccessTokenError> {
        if access_token == DUMMY_ODF_ACCESS_TOKEN {
            if let Some(dummy_account) = self.maybe_dummy_token_account.as_ref() {
                return Ok(AccessTokenType::DummyToken(dummy_account.clone()));
            }

            tracing::error!("Obtained dummy ODF access token unexpectedly");
            #[derive(Debug, Error)]
            #[error("Obtained dummy ODF access token unexpectedly")]
            struct DummyOdfAccessTokenError {}

            return Err(AccessTokenError::Invalid(Box::new(
                DummyOdfAccessTokenError {},
            )));
        }
        if access_token.len() > 2 && &access_token[..2] == ACCESS_TOKEN_PREFIX {
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
                let account_id = odf::AccountID::from_did_str(&token_data.claims.sub).int_err()?;

                match self.account_repository.get_account_by_id(&account_id).await {
                    Ok(account) => Ok(account),
                    Err(GetAccountByIdError::NotFound(_)) => {
                        Err(GetAccountInfoError::AccountUnresolved)
                    }
                    Err(GetAccountByIdError::Internal(e)) => Err(GetAccountInfoError::Internal(e)),
                }
            }
            AccessTokenType::KamuAccessToken(kamu_access_token) => self
                .access_token_svc
                .find_account_by_active_token_id(
                    &kamu_access_token.id,
                    kamu_access_token.random_bytes_hash,
                )
                .await
                .map_err(|err| match err {
                    FindAccountByTokenError::NotFound(_) => GetAccountInfoError::AccountUnresolved,
                    FindAccountByTokenError::InvalidTokenHash => {
                        GetAccountInfoError::AccessToken(AccessTokenError::Invalid(Box::new(err)))
                    }
                    FindAccountByTokenError::Internal(err) => GetAccountInfoError::Internal(err),
                }),
            AccessTokenType::DummyToken(dummy_account) => Ok(dummy_account),
        }
    }

    async fn notify_account_created(&self, new_account: &Account) -> Result<(), InternalError> {
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
impl AuthenticationService for AuthenticationServiceImpl {
    fn supported_login_methods(&self) -> Vec<&'static str> {
        let mut methods = self
            .authentication_providers_by_method
            .keys()
            .copied()
            .collect::<Vec<_>>();
        methods.sort_unstable();
        methods
    }

    async fn login(
        &self,
        login_method: &str,
        login_credentials_json: String,
        device_code: Option<DeviceCode>,
    ) -> Result<LoginResponse, LoginError> {
        // Resolve provider via a specified login method
        let provider = self.resolve_authentication_provider(login_method)?;

        // Attempt to login via provider
        let provider_response = provider.login(login_credentials_json).await?;

        // Try to resolve an existing account via the provider's identity key
        let maybe_account_id = self
            .account_repository
            .find_account_id_by_provider_identity_key(&provider_response.provider_identity_key)
            .await?;

        let account_id = match maybe_account_id {
            // Account already exists
            Some(account_id) => account_id,

            // Account does not exist and needs to be created
            None => {
                // Create a new account
                let new_account = Account {
                    id: provider_response.account_id,
                    account_name: provider_response.account_name.clone(),
                    email: provider_response.email,
                    display_name: provider_response.display_name,
                    account_type: provider_response.account_type,
                    avatar_url: provider_response.avatar_url,
                    registered_at: Utc::now(),
                    provider: String::from(login_method),
                    provider_identity_key: provider_response.provider_identity_key,
                };

                // Register an account in the repository
                self.account_repository
                    .save_account(&new_account)
                    .await
                    .map_err(|e| match e {
                        CreateAccountError::Duplicate(_) => LoginError::DuplicateCredentials,
                        CreateAccountError::Internal(e) => LoginError::Internal(e),
                    })?;

                // Notify interested parties
                self.notify_account_created(&new_account).await?;

                new_account.id
            }
        };

        // Create access. If there is a device_code, save token parameters for it
        let access_token = if let Some(device_code) = device_code {
            let claims = self.generate_jwt_claims(&account_id, EXPIRATION_TIME_SEC);
            let token = self.make_access_token_from_jwt_claims(&claims)?;
            let device_token_params_part = DeviceTokenParamsPart {
                iat: claims.iat,
                exp: claims.exp,
                account_id: account_id.clone(),
            };

            self.oauth_device_code_service
                .update_device_token_with_token_params_part(&device_code, &device_token_params_part)
                .await
                .int_err()?;

            token
        } else {
            self.make_access_token_from_account_id(&account_id, EXPIRATION_TIME_SEC)?
        };

        // Attach basic identity properties
        Ok(LoginResponse {
            access_token: access_token.into_inner(),
            account_id,
            account_name: provider_response.account_name,
        })
    }

    async fn account_by_token(&self, access_token: String) -> Result<Account, GetAccountInfoError> {
        self.account_by_token_impl(&access_token).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl JwtTokenIssuer for AuthenticationServiceImpl {
    fn make_access_token_from_account_id(
        &self,
        account_id: &odf::AccountID,
        expiration_time_sec: usize,
    ) -> Result<JwtAccessToken, InternalError> {
        self.make_access_token_from_account_id_impl(account_id, expiration_time_sec)
    }

    fn make_access_token_from_device_token_params_part(
        &self,
        DeviceTokenParamsPart {
            iat,
            exp,
            account_id,
        }: DeviceTokenParamsPart,
    ) -> Result<JwtAccessToken, InternalError> {
        let claims = JWTClaims {
            iat,
            exp,
            iss: KAMU_JWT_ISSUER.to_string(),
            sub: account_id.to_string(),
        };

        self.make_access_token_from_jwt_claims(&claims)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
