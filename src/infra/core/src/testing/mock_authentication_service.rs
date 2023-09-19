// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::auth::{
    AccessTokenError,
    AccountInfo,
    AuthenticationService,
    GetAccountInfoError,
    LoginError,
    LoginResponse,
    UnsupportedLoginMethodError,
};
use mockall::predicate::{always, eq};
use opendatafabric::AccountName;

/////////////////////////////////////////////////////////////////////////////////////////

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

        async fn account_info_by_token(
            &self,
            access_token: String,
        ) -> Result<AccountInfo, GetAccountInfoError>;

        async fn find_account_info_by_name<'a>(&'a self, account_name: &'a AccountName) -> Result<Option<AccountInfo>, InternalError>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub const DUMMY_TOKEN: &str = "test-dummy-token";
pub const DUMMY_LOGIN_METHOD: &str = "test";

impl MockAuthenticationService {
    pub fn built_in() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_login()
            .with(eq(DUMMY_LOGIN_METHOD), always())
            .returning(|_, _| {
                Ok(LoginResponse {
                    access_token: DUMMY_TOKEN.to_string(),
                    account_info: AccountInfo::dummy(),
                })
            });
        mock_authentication_service
            .expect_account_info_by_token()
            .with(eq(DUMMY_TOKEN.to_string()))
            .returning(|_| Ok(AccountInfo::dummy()));
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
            .expect_account_info_by_token()
            .with(eq(DUMMY_TOKEN.to_string()))
            .returning(|_| Err(GetAccountInfoError::AccessToken(AccessTokenError::Expired)));
        mock_authentication_service
    }

    pub fn resolving_token(access_token: &str, expected_account_info: AccountInfo) -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_account_info_by_token()
            .with(eq(access_token.to_string()))
            .returning(move |_| Ok(expected_account_info.clone()));
        mock_authentication_service
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
