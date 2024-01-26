// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::panic;
use std::collections::HashMap;
use std::sync::Arc;

use conv::ConvUtil;
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use kamu_core::auth::*;
use kamu_core::SystemTimeSource;
use opendatafabric::AccountName;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

const KAMU_JWT_ISSUER: &str = "dev.kamu";
const KAMU_JWT_ALGORITHM: Algorithm = Algorithm::HS384;

pub const ENV_VAR_KAMU_JWT_SECRET: &str = "KAMU_JWT_SECRET";

///////////////////////////////////////////////////////////////////////////////

pub fn set_random_jwt_secret() {
    let random_jwt_secret: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();

    std::env::set_var(ENV_VAR_KAMU_JWT_SECRET, random_jwt_secret);
}

///////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceImpl {
    time_source: Arc<dyn SystemTimeSource>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    authentication_providers_by_method: HashMap<&'static str, Arc<dyn AuthenticationProvider>>,
    authentication_providers: Vec<Arc<dyn AuthenticationProvider>>,
}

#[component(pub)]
#[interface(dyn AuthenticationService)]
impl AuthenticationServiceImpl {
    pub fn new(
        authentication_providers: Vec<Arc<dyn AuthenticationProvider>>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        let kamu_jwt_secret = std::env::var(ENV_VAR_KAMU_JWT_SECRET)
            .unwrap_or_else(|_| panic!("{} env var is not set", ENV_VAR_KAMU_JWT_SECRET));

        let mut authentication_providers_by_method = HashMap::new();

        for authentication_provider in &authentication_providers {
            let login_method = authentication_provider.login_method();

            let insert_result = authentication_providers_by_method
                .insert(login_method, authentication_provider.clone());

            assert!(
                insert_result.is_none(),
                "Duplicate authentication provider for method {login_method}"
            );
        }

        Self {
            time_source,
            encoding_key: EncodingKey::from_secret(kamu_jwt_secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(kamu_jwt_secret.as_bytes()),
            authentication_providers_by_method,
            authentication_providers,
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

    fn make_access_token(
        &self,
        subject: String,
        login_method: &str,
        provider_credentials_json: String,
    ) -> Result<String, InternalError> {
        const EXPIRE_AFTER_SEC: usize = 24 * 60 * 60; // 1 day in seconds

        let current_time = self.time_source.now();
        let iat = current_time.timestamp().value_as::<usize>().unwrap();
        let exp = iat + EXPIRE_AFTER_SEC;
        let claims = KamuAccessTokenClaims {
            iat,
            exp,
            iss: String::from(KAMU_JWT_ISSUER),
            sub: subject,
            access_credentials: KamuAccessCredentials {
                login_method: String::from(login_method),
                provider_credentials_json,
            },
        };

        encode(
            &Header::new(KAMU_JWT_ALGORITHM),
            &claims,
            &self.encoding_key,
        )
        .map_err(ErrorIntoInternal::int_err)
    }

    fn decode_access_token(
        &self,
        access_token: &str,
    ) -> Result<KamuAccessCredentials, AccessTokenError> {
        let mut validation = Validation::new(KAMU_JWT_ALGORITHM);
        validation.set_issuer(vec![KAMU_JWT_ISSUER].as_slice());

        let token_data =
            decode::<KamuAccessTokenClaims>(access_token, &self.decoding_key, &validation)
                .map_err(|e| match *e.kind() {
                    ErrorKind::ExpiredSignature => AccessTokenError::Expired,
                    _ => AccessTokenError::Invalid(Box::new(e)),
                })?;

        Ok(token_data.claims.access_credentials)
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
        let provider = self.resolve_authentication_provider(login_method)?;

        let provider_response = provider.login(login_credentials_json).await.map_err(
            |e: ProviderLoginError| match e {
                ProviderLoginError::RejectedCredentials(e) => LoginError::RejectedCredentials(e),
                ProviderLoginError::InvalidCredentials(e) => LoginError::InvalidCredentials(e),
                ProviderLoginError::Internal(e) => LoginError::Internal(e),
            },
        )?;

        Ok(LoginResponse {
            access_token: self.make_access_token(
                provider_response.account_info.account_name.to_string(),
                login_method,
                provider_response.provider_credentials_json,
            )?,
            account_info: provider_response.account_info,
        })
    }

    async fn account_info_by_token(
        &self,
        access_token: String,
    ) -> Result<AccountInfo, GetAccountInfoError> {
        let decoded_access_token = self
            .decode_access_token(&access_token)
            .map_err(GetAccountInfoError::AccessToken)?;

        let provider = self
            .resolve_authentication_provider(decoded_access_token.login_method.as_str())
            .map_err(|e| {
                GetAccountInfoError::AccessToken(AccessTokenError::Invalid(Box::new(e)))
            })?;

        provider
            .account_info_by_token(decoded_access_token.provider_credentials_json)
            .await
            .map_err(|e| GetAccountInfoError::Internal(e.int_err()))
    }

    async fn find_account_info_by_name<'a>(
        &'a self,
        account_name: &'a AccountName,
    ) -> Result<Option<AccountInfo>, InternalError> {
        for provider in &self.authentication_providers {
            let maybe_account_info = provider.find_account_info_by_name(account_name).await?;
            if maybe_account_info.is_some() {
                return Ok(maybe_account_info);
            }
        }

        Ok(None)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
struct KamuAccessCredentials {
    login_method: String,
    provider_credentials_json: String,
}

impl KamuAccessCredentials {
    #[allow(dead_code)]
    pub fn new<S>(login_method: S, provider_credentials_json: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            login_method: login_method.into(),
            provider_credentials_json: provider_credentials_json.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
struct KamuAccessTokenClaims {
    exp: usize,
    iat: usize,
    iss: String,
    sub: String,
    access_credentials: KamuAccessCredentials,
}

///////////////////////////////////////////////////////////////////////////////
