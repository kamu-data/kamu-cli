// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountHandle,
    AnonymousAccountReason,
    AuthenticationService,
    CurrentAccountSubject,
    CurrentAccountSubjectCatalogExt as _,
    DEFAULT_ACCOUNT_NAME_STR,
    DUMMY_ACCESS_TOKEN,
    MockAuthenticationService,
    Principal,
};
use url::Url;

use crate::harness::await_client_server_flow;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_api_access() {
    let server_harness = ServerHarness::new(MockAuthenticationService::new(), None).await;
    let test_url = server_harness.test_url();
    let principal_url = server_harness.principal_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        assert_eq!(
            format!("{:?}", AnonymousAccountReason::NoAuthenticationProvided),
            ServerHarness::get_text(&test_url, None).await
        );
        assert_eq!(
            format!("{:?}", Principal::Anonymous),
            ServerHarness::get_text(&principal_url, None).await
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_valid_access() {
    let server_harness = ServerHarness::new(
        MockAuthenticationService::resolving_token(DUMMY_ACCESS_TOKEN, Account::dummy()),
        None,
    )
    .await;
    let test_url = server_harness.test_url();
    let principal_url = server_harness.principal_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        assert_eq!(
            DEFAULT_ACCOUNT_NAME_STR,
            ServerHarness::get_text(&test_url, Some(DUMMY_ACCESS_TOKEN)).await
        );
        assert_eq!(
            format!(
                "{:?}",
                Principal::User(AccountHandle::from(&Account::dummy()))
            ),
            ServerHarness::get_text(&principal_url, Some(DUMMY_ACCESS_TOKEN)).await
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_access_expired_token() {
    let server_harness = ServerHarness::new(MockAuthenticationService::expired_token(), None).await;
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client
            .get(test_url)
            .bearer_auth(kamu_accounts::DUMMY_ACCESS_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(
            format!("{:?}", AnonymousAccountReason::AuthenticationExpired),
            response.text().await.unwrap()
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_correct_handle_empty_bearer_token() {
    let server_harness = ServerHarness::new(MockAuthenticationService::expired_token(), None).await;
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client.get(test_url).bearer_auth("").send().await.unwrap();

        assert_eq!(http::StatusCode::BAD_REQUEST, response.status());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_access_invalid_token() {
    let server_harness = ServerHarness::new(MockAuthenticationService::invalid_token(), None).await;
    let test_url = server_harness.test_url();
    let principal_url = server_harness.principal_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        assert_eq!(
            format!("{:?}", AnonymousAccountReason::AuthenticationInvalid),
            ServerHarness::get_text(&test_url, Some(DUMMY_ACCESS_TOKEN)).await
        );
        assert_eq!(
            format!("{:?}", Principal::Anonymous),
            ServerHarness::get_text(&principal_url, Some(DUMMY_ACCESS_TOKEN)).await
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_ENDPOINT: &str = "/foo";
const PRINCIPAL_ENDPOINT: &str = "/principal";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ServerHarness {
    server_future: Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Unpin>,
    local_addr: SocketAddr,
}

impl ServerHarness {
    pub async fn new(
        authentication_service: MockAuthenticationService,
        initial_current_account_subject: Option<CurrentAccountSubject>,
    ) -> Self {
        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(authentication_service)
                .bind::<dyn AuthenticationService, MockAuthenticationService>();

            if let Some(current_account_subject) = initial_current_account_subject {
                b.add_current_account_subject(current_account_subject);
            }

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let app = axum::Router::new()
            .route(
                TEST_ENDPOINT,
                axum::routing::get(ServerHarness::foo_handler),
            )
            .route(
                PRINCIPAL_ENDPOINT,
                axum::routing::get(ServerHarness::principal_handler),
            )
            .layer(
                tower::ServiceBuilder::new()
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    )
                    .layer(axum::Extension(catalog))
                    .layer(kamu_adapter_http::AuthenticationLayer::new()),
            );

        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let server_future = Box::new(axum::serve(listener, app.into_make_service()).into_future());

        Self {
            server_future,
            local_addr,
        }
    }

    pub async fn api_server_run(self) -> Result<(), InternalError> {
        self.server_future.await.int_err()
    }

    pub fn test_url(&self) -> Url {
        Url::from_str(format!("http://{}{}", self.local_addr, TEST_ENDPOINT).as_str()).unwrap()
    }

    /// An endpoint that answers with the `Debug` form of the request's
    /// [`Principal`]
    pub fn principal_url(&self) -> Url {
        Url::from_str(format!("http://{}{}", self.local_addr, PRINCIPAL_ENDPOINT).as_str()).unwrap()
    }

    pub async fn get_text(url: &Url, bearer_token: Option<&str>) -> String {
        let client = reqwest::Client::new();
        let request = client.get(url.clone());
        let request = match bearer_token {
            Some(token) => request.bearer_auth(token),
            None => request,
        };
        request.send().await.unwrap().text().await.unwrap()
    }

    #[expect(clippy::unused_async, reason = "axum handlers must be async")]
    async fn foo_handler(
        axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    ) -> Result<String, http::StatusCode> {
        let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
        match current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(r) => Ok(format!("{r:?}")),
            CurrentAccountSubject::Logged(l) => Ok(l.name.to_string()),
        }
    }

    #[expect(clippy::unused_async, reason = "axum handlers must be async")]
    async fn principal_handler(
        axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    ) -> String {
        let principal = catalog.get_one::<Principal>().unwrap();
        format!("{principal:?}")
    }
}
