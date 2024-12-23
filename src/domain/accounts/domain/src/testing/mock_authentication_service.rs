// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use internal_error::InternalError;
use mockall::predicate::{always, eq};
use thiserror::Error;

use crate::{
    AccessTokenError,
    Account,
    AuthenticationService,
    GetAccountInfoError,
    LoginError,
    LoginResponse,
    UnsupportedLoginMethodError,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
    DUMMY_ACCESS_TOKEN,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DUMMY_LOGIN_METHOD: &str = "test";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub AuthenticationService {}
    #[async_trait::async_trait]
    impl AuthenticationService for AuthenticationService {
        fn supported_login_methods(&self) -> Vec<&'static str>;

        async fn login(
            &self,
            login_method: &str,
            login_credentials_json: String,
        ) -> Result<LoginResponse, LoginError>;

        async fn account_by_token(
            &self,
            access_token: String,
        ) -> Result<Account, GetAccountInfoError>;

        async fn account_by_id(
            &self,
            account_id: &odf::AccountID,
        ) -> Result<Option<Account>, InternalError>;

        async fn accounts_by_ids(
            &self,
            account_ids: Vec<odf::AccountID>,
        ) -> Result<Vec<Account>, InternalError>;

        async fn account_by_name(
            &self,
            account_name: &odf::AccountName,
        ) -> Result<Option<Account>, InternalError>;

        async fn find_account_id_by_name(
            &self,
            account_name: &odf::AccountName,
        ) -> Result<Option<odf::AccountID>, InternalError>;

        async fn find_account_name_by_id(
            &self,
            account_id: &odf::AccountID,
        ) -> Result<Option<odf::AccountName>, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockAuthenticationService {
    pub fn with_custom_account(account_id: odf::AccountID, account_name: odf::AccountName) -> Self {
        let account_name_clone = account_name.clone();
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_find_account_name_by_id()
            .returning(move |_| Ok(Some(account_name.clone())));
        mock_authentication_service
            .expect_account_by_name()
            .with(eq(account_name_clone.clone()))
            .returning(move |_| {
                Ok(Some(Account {
                    id: account_id.clone(),
                    account_name: account_name_clone.clone(),
                    ..Account::dummy()
                }))
            });
        mock_authentication_service
    }

    pub fn built_in() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_login()
            .with(eq(DUMMY_LOGIN_METHOD), always())
            .returning(|_, _| {
                Ok(LoginResponse {
                    access_token: DUMMY_ACCESS_TOKEN.to_string(),
                    account_id: DEFAULT_ACCOUNT_ID.clone(),
                    account_name: DEFAULT_ACCOUNT_NAME.clone(),
                })
            });
        mock_authentication_service
            .expect_account_by_token()
            .with(eq(DUMMY_ACCESS_TOKEN.to_string()))
            .returning(|_| Ok(Account::dummy()));
        mock_authentication_service
    }

    pub fn unsupported_login_method() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_login()
            .with(eq(DUMMY_LOGIN_METHOD), always())
            .returning(|_, _| {
                Err(LoginError::UnsupportedMethod(UnsupportedLoginMethodError {
                    method: DUMMY_LOGIN_METHOD.to_string(),
                }))
            });
        mock_authentication_service
    }

    pub fn expired_token() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_account_by_token()
            .with(eq(DUMMY_ACCESS_TOKEN.to_string()))
            .returning(|_| Err(GetAccountInfoError::AccessToken(AccessTokenError::Expired)));
        mock_authentication_service
    }

    pub fn invalid_token() -> Self {
        #[derive(Debug, Error)]
        struct InvalidTokenError {}
        impl Display for InvalidTokenError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "InvalidTokenError")
            }
        }

        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_account_by_token()
            .with(eq(DUMMY_ACCESS_TOKEN.to_string()))
            .returning(|_| {
                Err(GetAccountInfoError::AccessToken(AccessTokenError::Invalid(
                    Box::new(InvalidTokenError {}),
                )))
            });
        mock_authentication_service
    }

    pub fn resolving_token(access_token: &str, expected_account_info: Account) -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        let account_cloned = expected_account_info.clone();
        mock_authentication_service
            .expect_account_by_id()
            .with(eq(account_cloned.id.clone()))
            .returning(move |_| Ok(Some(account_cloned.clone())));
        mock_authentication_service
            .expect_account_by_token()
            .with(eq(access_token.to_string()))
            .returning(move |_| Ok(expected_account_info.clone()));
        mock_authentication_service
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
