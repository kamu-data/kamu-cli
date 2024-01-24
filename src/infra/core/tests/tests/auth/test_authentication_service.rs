// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use internal_error::InternalError;
use kamu::{set_random_jwt_secret, AuthenticationServiceImpl};
use kamu_core::auth::{
    AccessTokenError,
    AccountInfo,
    AuthenticationProvider,
    AuthenticationService,
    GetAccountInfoError,
    LoginError,
    ProviderLoginError,
    ProviderLoginResponse,
};
use kamu_core::{SystemTimeSource, SystemTimeSourceStub};
use opendatafabric::AccountName;

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_enabled_login_methods() {
    let catalog = make_catalog();
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let mut supported_login_methods = authentication_service.supported_login_methods();
    supported_login_methods.sort_unstable();
    assert_eq!(supported_login_methods, vec!["method-A", "method-B"]);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_login() {
    let catalog = make_catalog();
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let response_a = authentication_service
        .login("method-A", "dummy".to_string())
        .await;

    let response_b = authentication_service
        .login("method-B", "dummy".to_string())
        .await;

    let response_bad = authentication_service
        .login("method-bad", "dummy".to_string())
        .await;

    assert_matches!(response_a, Ok(_));
    assert_matches!(response_b, Ok(_));
    assert_matches!(response_bad, Err(LoginError::UnsupportedMethod(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_use_good_access_token() {
    let catalog = make_catalog();
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let login_response = authentication_service
        .login("method-A", "dummy".to_string())
        .await
        .unwrap();

    let resolved_account_info = authentication_service
        .account_info_by_token(login_response.access_token)
        .await
        .unwrap();
    assert_eq!(login_response.account_info, resolved_account_info);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_use_bad_access_token() {
    let catalog = make_catalog();
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    assert_matches!(
        authentication_service
            .account_info_by_token("bad-token".to_string())
            .await,
        Err(GetAccountInfoError::AccessToken(AccessTokenError::Invalid(
            _
        )))
    );
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_find_account_info() {
    let catalog = make_catalog();
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    assert_matches!(
        authentication_service
            .find_account_info_by_name(&AccountName::new_unchecked("user-a"))
            .await,
        Ok(Some(_))
    );
    assert_matches!(
        authentication_service
            .find_account_info_by_name(&AccountName::new_unchecked("user-b"))
            .await,
        Ok(Some(_))
    );
    assert_matches!(
        authentication_service
            .find_account_info_by_name(&AccountName::new_unchecked("user-c"))
            .await,
        Ok(None)
    );
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_catalog() -> dill::Catalog {
    set_random_jwt_secret();

    dill::CatalogBuilder::new()
        .add::<DummyAuthenticationProviderA>()
        .add::<DummyAuthenticationProviderB>()
        .add::<AuthenticationServiceImpl>()
        .add_value(SystemTimeSourceStub::new())
        .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

struct DummyAuthenticationProviderA {}
struct DummyAuthenticationProviderB {}

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
impl DummyAuthenticationProviderA {
    fn new() -> Self {
        Self {}
    }
}

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
impl DummyAuthenticationProviderB {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl AuthenticationProvider for DummyAuthenticationProviderA {
    fn login_method(&self) -> &'static str {
        "method-A"
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        Ok(ProviderLoginResponse {
            provider_credentials_json: "credentials-method-A".to_string(),
            account_info: AccountInfo::dummy(),
        })
    }

    async fn account_info_by_token(
        &self,
        provider_credentials_json: String,
    ) -> Result<AccountInfo, InternalError> {
        if provider_credentials_json == "credentials-method-A" {
            Ok(AccountInfo::dummy())
        } else {
            panic!("Bad credentials");
        }
    }

    async fn find_account_info_by_name<'a>(
        &'a self,
        account_name: &'a AccountName,
    ) -> Result<Option<AccountInfo>, InternalError> {
        if account_name == "user-a" {
            Ok(Some(AccountInfo::dummy()))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl AuthenticationProvider for DummyAuthenticationProviderB {
    fn login_method(&self) -> &'static str {
        "method-B"
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        Ok(ProviderLoginResponse {
            provider_credentials_json: "credentials-method-B".to_string(),
            account_info: AccountInfo::dummy(),
        })
    }

    async fn account_info_by_token(
        &self,
        provider_credentials_json: String,
    ) -> Result<AccountInfo, InternalError> {
        if provider_credentials_json == "credentials-method-B" {
            Ok(AccountInfo::dummy())
        } else {
            panic!("Bad credentials");
        }
    }

    async fn find_account_info_by_name<'a>(
        &'a self,
        account_name: &'a AccountName,
    ) -> Result<Option<AccountInfo>, InternalError> {
        if account_name == "user-b" {
            Ok(Some(AccountInfo::dummy()))
        } else {
            Ok(None)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
