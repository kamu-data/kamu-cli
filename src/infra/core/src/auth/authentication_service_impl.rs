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

use chrono::{Duration, Utc};
use dill::component;
use internal_error::{ErrorIntoInternal, InternalError};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use kamu_core::auth::*;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

const KAMU_JWT_ISSUER: &str = "dev.kamu";
const KAMU_JWT_ALGORITHM: Algorithm = Algorithm::HS384;

///////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceImpl {
    kamu_jwt_secret: String,
    authentication_providers_by_method: HashMap<&'static str, Arc<dyn AuthenticationProvider>>,
}

#[component(pub)]
impl AuthenticationServiceImpl {
    pub fn new(authentication_providers: Vec<Arc<dyn AuthenticationProvider>>) -> Self {
        let kamu_jwt_secret = match std::env::var("KAMU_JWT_SECRET") {
            Ok(jwt_secret) => jwt_secret,
            Err(_) => Self::random_jwt_secret(),
        };

        let mut service = Self {
            authentication_providers_by_method: HashMap::new(),
            kamu_jwt_secret,
        };

        for authentication_provider in authentication_providers {
            let login_method = authentication_provider.login_method();

            let insert_result = service
                .authentication_providers_by_method
                .insert(login_method, authentication_provider);

            if let Some(_) = insert_result {
                panic!(
                    "Duplicate authentication provider for method {}",
                    login_method
                );
            }
        }

        service
    }

    fn random_jwt_secret() -> String {
        thread_rng()
            .sample_iter(&Alphanumeric)
            .take(64)
            .map(char::from)
            .collect()
    }

    fn resolve_authentication_provider(
        &self,
        login_method: &str,
    ) -> Result<Arc<dyn AuthenticationProvider>, UnknownLoginMethodError> {
        match self.authentication_providers_by_method.get(login_method) {
            Some(provider) => Ok(provider.clone()),
            None => Err(UnknownLoginMethodError {
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
        let claims = KamuAccessTokenClaims {
            iat: Utc::now().timestamp() as usize,
            exp: (Utc::now() + Duration::days(1)).timestamp() as usize,
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
            &EncodingKey::from_secret(self.kamu_jwt_secret.as_bytes()),
        )
        .map_err(|e| e.int_err())
    }

    fn decode_access_token(
        &self,
        access_token: String,
    ) -> Result<KamuAccessCredentials, AccessTokenError> {
        let mut validation = Validation::new(KAMU_JWT_ALGORITHM);
        validation.set_issuer(vec![KAMU_JWT_ISSUER].as_slice());

        let token_data = decode::<KamuAccessTokenClaims>(
            &access_token,
            &DecodingKey::from_secret(self.kamu_jwt_secret.as_bytes()),
            &validation,
        )
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
                provider_response.account_info.login.to_string(),
                login_method,
                provider_response.provider_credentials_json,
            )?,
            account_info: provider_response.account_info,
        })
    }

    async fn get_account_info(
        &self,
        access_token: String,
    ) -> Result<AccountInfo, GetAccountInfoError> {
        let decoded_access_token = self
            .decode_access_token(access_token)
            .map_err(|e| GetAccountInfoError::AccessToken(e))?;

        let provider = self
            .resolve_authentication_provider(decoded_access_token.login_method.as_str())
            .map_err(|e| GetAccountInfoError::Internal(e.int_err()))?;

        provider
            .get_account_info(decoded_access_token.provider_credentials_json)
            .await
            .map_err(|e| GetAccountInfoError::Internal(e.int_err()))
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
