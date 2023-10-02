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

use kamu::domain::{CurrentAccountSubject, InternalError, ResultIntoInternal};
use url::Url;

use crate::harness::await_client_server_flow;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_api_access() {
    let server_harness = ServerHarness::new(None);
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client.get(test_url).send().await.unwrap();
        assert_eq!(http::StatusCode::OK, response.status());
        assert_eq!(ANONYMOUS_RETURN_TEXT, response.text().await.unwrap());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_anonymous_api_access() {
    let server_harness = ServerHarness::new(None);
    let test_url = server_harness.test_url();

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client
            .get(test_url)
            .bearer_auth(TEST_ACCESS_TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(http::StatusCode::OK, response.status());
        assert_eq!(TEST_ACOUNT_NAME, response.text().await.unwrap());
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

const ANONYMOUS_RETURN_TEXT: &str = "Anonymous access detected";
const TEST_ACCESS_TOKEN: &str = "some-token";
const TEST_ACOUNT_NAME: &str = "some-account";
const TEST_ENDPOINT: &str = "/foo";

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct ServerHarness {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
}

impl ServerHarness {
    pub fn new(initial_current_account_subject: Option<CurrentAccountSubject>) -> Self {
        let mut catalog_builder = dill::CatalogBuilder::new();
        catalog_builder.add_value(kamu::testing::MockAuthenticationService::resolving_token(TEST_ACCESS_TOKEN, kamu::domain::auth::AccountInfo {
            account_id: opendatafabric::FAKE_ACCOUNT_ID.to_string(),
            account_name: opendatafabric::AccountName::new_unchecked(TEST_ACOUNT_NAME),
            account_type: kamu::domain::auth::AccountType::User,
            display_name: TEST_ACOUNT_NAME.to_string(),
            avatar_url: None,
        })).bind::<dyn kamu::domain::auth::AuthenticationService, kamu::testing::MockAuthenticationService>();
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
        catalog: axum::extract::Extension<dill::Catalog>,
    ) -> Result<String, http::StatusCode> {
        let current_account_subject = catalog.0.get_one::<CurrentAccountSubject>().unwrap();
        match current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => Ok(ANONYMOUS_RETURN_TEXT.to_string()),
            CurrentAccountSubject::Logged(l) => Ok(l.account_name.to_string()),
        }
    }
}
