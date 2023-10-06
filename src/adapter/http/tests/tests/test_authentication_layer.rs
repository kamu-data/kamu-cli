// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use kamu::domain::{
    AnonymousAccountReason,
    CurrentAccountSubject,
    InternalError,
    ResultIntoInternal,
};
use kamu::testing::DUMMY_TOKEN;
use url::Url;

use crate::harness::await_client_server_flow;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_api_access() {
    let server_harness = ServerHarness::new(kamu::testing::MockAuthenticationService::new(), None);
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client.get(test_url).send().await.unwrap();
        assert_eq!(
            format!("{:?}", AnonymousAccountReason::NoAuthenticationProvided),
            response.text().await.unwrap()
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_valid_access() {
    let server_harness = ServerHarness::new(
        kamu::testing::MockAuthenticationService::resolving_token(
            DUMMY_TOKEN,
            kamu::domain::auth::AccountInfo::dummy(),
        ),
        None,
    );
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client
            .get(test_url)
            .bearer_auth(DUMMY_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(TEST_ACOUNT_NAME, response.text().await.unwrap());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_access_expired_token() {
    let server_harness = ServerHarness::new(
        kamu::testing::MockAuthenticationService::expired_token(),
        None,
    );
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client
            .get(test_url)
            .bearer_auth(DUMMY_TOKEN)
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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_access_invalid_token() {
    let server_harness = ServerHarness::new(
        kamu::testing::MockAuthenticationService::invalid_token(),
        None,
    );
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client
            .get(test_url)
            .bearer_auth(DUMMY_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(
            format!("{:?}", AnonymousAccountReason::AuthenticationInvalid),
            response.text().await.unwrap()
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

const TEST_ACOUNT_NAME: &str = "some-account";
const TEST_ENDPOINT: &str = "/foo";

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct ServerHarness {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
}

impl ServerHarness {
    pub fn new(
        authentication_service: kamu::testing::MockAuthenticationService,
        initial_current_account_subject: Option<CurrentAccountSubject>,
    ) -> Self {
        let mut catalog_builder = dill::CatalogBuilder::new();
        catalog_builder.add_value(authentication_service).bind::<dyn kamu::domain::auth::AuthenticationService, kamu::testing::MockAuthenticationService>();
        if let Some(current_account_subject) = initial_current_account_subject {
            catalog_builder.add_value(current_account_subject);
        }

        let catalog = catalog_builder.build();

        let app = axum::Router::new()
            .route(
                TEST_ENDPOINT,
                axum::routing::get(ServerHarness::foo_handler),
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

        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self { server }
    }

    pub async fn api_server_run(self) -> Result<(), InternalError> {
        self.server.await.int_err()
    }

    pub fn test_url(&self) -> Url {
        Url::from_str(
            format!(
                "http://{}{}",
                self.server.local_addr().to_string(),
                TEST_ENDPOINT
            )
            .as_str(),
        )
        .unwrap()
    }

    async fn foo_handler(
        axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    ) -> Result<String, http::StatusCode> {
        let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
        match current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(r) => Ok(format!("{:?}", r)),
            CurrentAccountSubject::Logged(l) => Ok(l.account_name.to_string()),
        }
    }
}
