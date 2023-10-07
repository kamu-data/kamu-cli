// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use kamu::domain::auth::DatasetAction;
use kamu::domain::{
    AnonymousAccountReason,
    CurrentAccountSubject,
    DatasetRepository,
    InternalError,
    ResultIntoInternal,
};
use kamu::testing::{MetadataFactory, MockDatasetActionAuthorizer};
use kamu::DatasetRepositoryLocalFs;
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetHandle, DatasetKind, DatasetName, DatasetRef};
use url::Url;

use crate::harness::await_client_server_flow;

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_http_methods(
    methods: &[http::Method],
    test_url_path: &str,
    logged_in: bool,
    expected_action: DatasetAction,
    should_authorize: bool,
    expected_status: http::StatusCode,
) {
    let server_harness = ServerHarness::new(
        if logged_in {
            CurrentAccountSubject::new_test()
        } else {
            CurrentAccountSubject::Anonymous(AnonymousAccountReason::NoAuthenticationProvided)
        },
        ServerHarness::mock_dataset_action_authorizer(expected_action, should_authorize),
    )
    .await;

    let test_url = server_harness.test_url(test_url_path);

    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        for method in methods {
            ServerHarness::check_access(&test_url, method.clone(), expected_status).await;
        }
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_anonymous() {
    test_http_methods(
        &UNSAFE_METHODS,
        "foo",
        false,
        DatasetAction::Write,
        false,
        http::StatusCode::FORBIDDEN,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_logged_but_unauthorized() {
    test_http_methods(
        &UNSAFE_METHODS,
        "foo",
        true,
        DatasetAction::Write,
        false,
        http::StatusCode::FORBIDDEN,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_logged_authorized() {
    test_http_methods(
        &UNSAFE_METHODS,
        "foo",
        true,
        DatasetAction::Write,
        true,
        http::StatusCode::OK,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_anonymous() {
    test_http_methods(
        &SAFE_METHODS,
        "foo",
        false,
        DatasetAction::Read,
        true,
        http::StatusCode::OK,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_logged_in() {
    test_http_methods(
        &SAFE_METHODS,
        "foo",
        true,
        DatasetAction::Read,
        true,
        http::StatusCode::OK,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_anonymous() {
    test_http_methods(
        &SAFE_METHODS,
        "bar", // potential write
        false,
        DatasetAction::Write,
        false,
        http::StatusCode::FORBIDDEN,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_logged_but_unauthorized() {
    test_http_methods(
        &SAFE_METHODS,
        "bar", // potential write
        true,
        DatasetAction::Write,
        false,
        http::StatusCode::FORBIDDEN,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_logged_authorized() {
    test_http_methods(
        &SAFE_METHODS,
        "bar", // potential write
        true,
        DatasetAction::Write,
        true,
        http::StatusCode::OK,
    )
    .await;
}

/////////////////////////////////////////////////////////////////////////////////////////

const SAFE_METHODS: [http::Method; 2] = [http::Method::GET, http::Method::HEAD];

const UNSAFE_METHODS: [http::Method; 4] = [
    http::Method::PUT,
    http::Method::POST,
    http::Method::PATCH,
    http::Method::DELETE,
];

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
struct ServerHarness {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
    _temp_dir: tempfile::TempDir,
}

impl ServerHarness {
    pub async fn new(
        current_account_subject: CurrentAccountSubject,
        dataset_action_authorizer: MockDatasetActionAuthorizer,
    ) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();

        let mut catalog_builder = dill::CatalogBuilder::new();
        catalog_builder.add_value(
            kamu::testing::MockAuthenticationService::resolving_token(kamu::domain::auth::DUMMY_ACCESS_TOKEN, kamu::domain::auth::AccountInfo::dummy())
        )
            .bind::<dyn kamu::domain::auth::AuthenticationService, kamu::testing::MockAuthenticationService>();
        catalog_builder
            .add_value(dataset_action_authorizer)
            .bind::<dyn kamu::domain::auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>();
        catalog_builder.add_value(current_account_subject);
        catalog_builder
            .add_builder(
                dill::builder_for::<DatasetRepositoryLocalFs>()
                    .with_multi_tenant(false)
                    .with_root(temp_dir.path().join("datasets")),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();

        let catalog = catalog_builder.build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .create_dataset(
                &Self::dataset_alias(),
                MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                    .build_typed(),
            )
            .await
            .unwrap();

        let app = axum::Router::new()
            .route(
                "/foo",
                axum::routing::get(ServerHarness::foo_handler)
                    .put(ServerHarness::foo_handler)
                    .delete(ServerHarness::foo_handler)
                    .patch(ServerHarness::foo_handler)
                    .post(ServerHarness::foo_handler),
            )
            .route("/bar", axum::routing::get(ServerHarness::bar_handler))
            .layer(
                tower::ServiceBuilder::new()
                    .layer(axum::Extension(catalog))
                    .layer(axum::Extension(DatasetRef::from_str("mydataset").unwrap()))
                    .layer(kamu_adapter_http::DatasetAuthorizationLayer::new(vec![
                        "/bar",
                    ])),
            );

        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self {
            server,
            _temp_dir: temp_dir,
        }
    }

    pub fn mock_dataset_action_authorizer(
        action: DatasetAction,
        should_authorize: bool,
    ) -> MockDatasetActionAuthorizer {
        let mut mock_dataset_action_authorizer = MockDatasetActionAuthorizer::new();
        mock_dataset_action_authorizer
            .expect_check_action_allowed()
            .with(
                function(|dh: &DatasetHandle| dh.alias == ServerHarness::dataset_alias()),
                eq(action),
            )
            .returning(move |dh, action| {
                if should_authorize {
                    Ok(())
                } else {
                    Err(MockDatasetActionAuthorizer::denying_error(dh, action))
                }
            });

        mock_dataset_action_authorizer
    }

    pub async fn api_server_run(self) -> Result<(), InternalError> {
        self.server.await.int_err()
    }

    pub async fn check_access(
        test_url: &Url,
        method: http::Method,
        expected_status: http::StatusCode,
    ) {
        let client = reqwest::Client::new();

        let test_url = test_url.clone();

        let response = match method {
            http::Method::DELETE => client.delete(test_url),
            http::Method::GET => client.get(test_url),
            http::Method::HEAD => client.head(test_url),
            http::Method::PATCH => client.patch(test_url),
            http::Method::POST => client.post(test_url),
            http::Method::PUT => client.put(test_url),
            _ => panic!("unsupported method"),
        }
        .send()
        .await
        .unwrap();

        assert_eq!(response.status(), expected_status);
    }

    pub fn test_url(&self, path: &str) -> Url {
        Url::from_str(format!("http://{}/{}", self.server.local_addr().to_string(), path).as_str())
            .unwrap()
    }

    async fn foo_handler() -> http::StatusCode {
        http::StatusCode::OK
    }

    async fn bar_handler() -> http::StatusCode {
        http::StatusCode::OK
    }

    fn dataset_alias() -> DatasetAlias {
        DatasetAlias::new(None, DatasetName::new_unchecked("mydataset"))
    }
}
