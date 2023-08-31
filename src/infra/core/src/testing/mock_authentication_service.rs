// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth::{
    AccountInfo,
    AuthenticationService,
    GetAccountInfoError,
    LoginError,
    LoginResponse,
};
use kamu_core::{DEFAULT_ACCOUNT_NAME, DEFAULT_AVATAR_URL};
use mockall::predicate::{always, eq};
use opendatafabric::AccountName;

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub AuthenticationService {}
    #[async_trait::async_trait]
    impl AuthenticationService for AuthenticationService {
        async fn login(
            &self,
            login_method: &str,
            login_credentials_json: String,
        ) -> Result<LoginResponse, LoginError>;

        async fn get_account_info(
            &self,
            access_token: String,
        ) -> Result<AccountInfo, GetAccountInfoError>;
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const DUMMY_TOKEN: &str = "test-dummy-token";

impl MockAuthenticationService {
    pub fn built_in() -> Self {
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_login()
            .with(eq("test"), always())
            .returning(|_, _| {
                Ok(LoginResponse {
                    access_token: DUMMY_TOKEN.to_string(),
                    account_info: Self::make_dummy_account_info(),
                })
            });
        mock_authentication_service
            .expect_get_account_info()
            .with(eq(DUMMY_TOKEN.to_string()))
            .returning(|_| Ok(Self::make_dummy_account_info()));
        mock_authentication_service
    }

    fn make_dummy_account_info() -> AccountInfo {
        AccountInfo {
            account_name: AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME),
            display_name: DEFAULT_ACCOUNT_NAME.to_string(),
            avatar_url: Some(DEFAULT_AVATAR_URL.to_string()),
        }
    }
}
