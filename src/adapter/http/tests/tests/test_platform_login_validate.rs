// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use chrono::{Duration, Utc};
use kamu::domain::{InternalError, ResultIntoInternal, SystemTimeSource, SystemTimeSourceStub};
use kamu_accounts::{
    set_random_jwt_secret,
    AccountConfig,
    PredefinedAccountsConfig,
    PROVIDER_PASSWORD,
};
use kamu_accounts_inmem::AccountRepositoryInMemory;
use kamu_accounts_services::{AuthenticationServiceImpl, LoginPasswordAuthProvider};
use kamu_adapter_http::{LoginRequestBody, LoginResponseBody};
use opendatafabric::AccountName;
use serde_json::json;

use crate::harness::{await_client_server_flow, TestAPIServer};

/////////////////////////////////////////////////////////////////////////////////////////

const USER_WASYA: &str = "wasya";
const PASSWORD_WASYA: &str = "pwd_wasya";

const USER_PETYA: &str = "petya";

/////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    #[allow(dead_code)]
    run_info_dir: tempfile::TempDir,
    api_server: TestAPIServer,
    system_time_source_stub: Arc<SystemTimeSourceStub>,
}

impl Harness {
    fn new() -> Self {
        let run_info_dir = tempfile::tempdir().unwrap();

        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        predefined_accounts_config.predefined.push(
            AccountConfig::from_name(AccountName::new_unchecked(USER_WASYA))
                .set_password(String::from(PASSWORD_WASYA)),
        );
        predefined_accounts_config
            .predefined
            .push(AccountConfig::from_name(AccountName::new_unchecked(
                USER_PETYA,
            )));

        set_random_jwt_secret();

        let catalog = dill::CatalogBuilder::new()
            .add::<AuthenticationServiceImpl>()
            .add_value(predefined_accounts_config)
            .add::<AccountRepositoryInMemory>()
            .add_value(SystemTimeSourceStub::new())
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
            .add::<LoginPasswordAuthProvider>()
            .build();

        let system_time_source_stub = catalog.get_one::<SystemTimeSourceStub>().unwrap();

        let api_server = TestAPIServer::new(
            catalog,
            Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            None,
            true,
        );

        Self {
            run_info_dir,
            api_server,
            system_time_source_stub,
        }
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn login_url(&self) -> String {
        format!("http://{}/platform/login", self.api_server_addr())
    }

    fn validate_url(&self) -> String {
        format!("http://{}/platform/token/validate", self.api_server_addr())
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_with_password_method_success() {
    let harness = Harness::new();

    let login_url = harness.login_url();
    let validate_url = harness.validate_url();

    let client = async move {
        let client = reqwest::Client::new();

        let login_credentials_json = json!({
            "login": String::from(USER_WASYA),
            "password": String::from(PASSWORD_WASYA)
        })
        .to_string();

        let login_response = client
            .post(login_url)
            .json(&LoginRequestBody {
                login_method: String::from(PROVIDER_PASSWORD),
                login_credentials_json,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(200, login_response.status());

        let login_response_body = login_response.json::<LoginResponseBody>().await.unwrap();

        let validate_response = client
            .get(validate_url)
            .bearer_auth(login_response_body.access_token)
            .send()
            .await
            .unwrap();
        assert_eq!(200, validate_response.status());
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_with_password_method_invalid_credentials() {
    let harness = Harness::new();

    let login_url = harness.login_url();

    let client = async move {
        let client = reqwest::Client::new();

        let login_credentials_json = json!({
            "login": String::from(USER_WASYA),
            "password": String::from("wrong-password")
        })
        .to_string();

        let login_response = client
            .post(login_url)
            .json(&LoginRequestBody {
                login_method: String::from(PROVIDER_PASSWORD),
                login_credentials_json,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(401, login_response.status());

        let login_response_error = login_response.text().await.unwrap();
        assert_eq!(
            login_response_error,
            "Rejected credentials: invalid login or password"
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_with_password_method_expired_credentials() {
    let harness = Harness::new();
    let time_source_stub = harness.system_time_source_stub.clone();

    let login_url = harness.login_url();
    let validate_url = harness.validate_url();

    let client = async move {
        let client = reqwest::Client::new();

        // Roll back time on 2 days to get expired token
        time_source_stub.set(Utc::now() - Duration::days(2));

        let login_credentials_json = json!({
            "login": String::from(USER_WASYA),
            "password": String::from(PASSWORD_WASYA)
        })
        .to_string();

        let login_response = client
            .post(login_url)
            .json(&LoginRequestBody {
                login_method: String::from(PROVIDER_PASSWORD),
                login_credentials_json,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(200, login_response.status());

        let login_response_body = login_response.json::<LoginResponseBody>().await.unwrap();

        let validate_response = client
            .get(validate_url)
            .bearer_auth(login_response_body.access_token)
            .send()
            .await
            .unwrap();
        assert_eq!(401, validate_response.status());

        let validate_response_error = validate_response.text().await.unwrap();
        assert_eq!(validate_response_error, "Authentication token expired");
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_validate_invalid_token_fails() {
    let harness = Harness::new();
    let validate_url = harness.validate_url();

    let client = async move {
        let client = reqwest::Client::new();

        let validate_response = client
            .get(validate_url)
            .bearer_auth("bad-access-token")
            .send()
            .await
            .unwrap();
        assert_eq!(400, validate_response.status());

        let validate_response_error = validate_response.text().await.unwrap();
        assert_eq!(validate_response_error, "Authentication token invalid");
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_validate_without_token_fails() {
    let harness = Harness::new();
    let validate_url = harness.validate_url();

    let client = async move {
        let client = reqwest::Client::new();

        let validate_response = client.get(validate_url).send().await.unwrap();
        assert_eq!(400, validate_response.status());

        let validate_response_error = validate_response.text().await.unwrap();
        assert_eq!(validate_response_error, "No authentication token provided");
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////
