// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::str::FromStr;

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::AccountServiceImpl;
use kamu_core::{DidGenerator, MockDidGenerator, TenancyConfig};
use kamu_datasets::{CreateDatasetUseCase, DatasetPermission, MockDatasetPermissionChecker};
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::DummyOutboxImpl;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;
use url::Url;

use crate::harness::await_client_server_flow;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestHttpMethodsOptions<'a> {
    methods: &'a [http::Method],
    test_url_path: &'a str,
    logged_in: bool,
    granted_permissions: &'a [DatasetPermission],
    expected_status: http::StatusCode,
}

async fn test_http_methods(
    TestHttpMethodsOptions {
        methods,
        test_url_path,
        logged_in,
        granted_permissions,
        expected_status,
    }: TestHttpMethodsOptions<'_>,
) {
    let server_harness = ServerHarness::new(
        if logged_in {
            CurrentAccountSubject::new_test()
        } else {
            CurrentAccountSubject::Anonymous(AnonymousAccountReason::NoAuthenticationProvided)
        },
        ServerHarness::mock_dataset_permission_checker(
            granted_permissions.iter().copied().collect(),
        ),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_anonymous_and_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &UNSAFE_METHODS,
        test_url_path: "foo",
        logged_in: false,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_anonymous_and_authorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &UNSAFE_METHODS,
        test_url_path: "foo",
        logged_in: false,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::FORBIDDEN,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_logged_but_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &UNSAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_logged_and_authorized_for_reading() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &UNSAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::FORBIDDEN,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_unsafe_methods_logged_and_authorized_for_writing() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &UNSAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read, DatasetPermission::Write],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_anonymous_and_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: false,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_anonymous_and_authorized_for_reading() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: false,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_anonymous_and_authorized_for_writing() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: false,
        granted_permissions: &[DatasetPermission::Read, DatasetPermission::Write],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_logged_but_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_logged_and_authorized_for_reading() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_methods_logged_and_authorized_for_writing() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "foo",
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read, DatasetPermission::Write],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_anonymous_and_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: false,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_anonymous_and_authorized_for_reading()
{
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: false,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::FORBIDDEN,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_anonymous_and_authorized_for_writing()
{
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: false,
        granted_permissions: &[DatasetPermission::Read, DatasetPermission::Write],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_logged_but_unauthorized() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: true,
        granted_permissions: &[],
        expected_status: http::StatusCode::NOT_FOUND,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_logged_authorized_for_reading() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read],
        expected_status: http::StatusCode::FORBIDDEN,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_safe_method_but_potential_write_logged_authorized_for_writing() {
    test_http_methods(TestHttpMethodsOptions {
        methods: &SAFE_METHODS,
        test_url_path: "bar", // potential write
        logged_in: true,
        granted_permissions: &[DatasetPermission::Read, DatasetPermission::Write],
        expected_status: http::StatusCode::OK,
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_access_invalid_token_returns_unauthorized() {
    let server_harness = ServerHarness::new(
        CurrentAccountSubject::Anonymous(AnonymousAccountReason::AuthenticationInvalid),
        ServerHarness::mock_dataset_permission_checker(HashSet::new()),
    )
    .await;

    let test_url = server_harness.test_url("foo");
    let api_server_handle = server_harness.api_server_run();

    let client_handle = async {
        let client = reqwest::Client::new();
        let response = client.get(test_url).send().await.unwrap();

        assert_eq!(http::StatusCode::UNAUTHORIZED, response.status());
        assert_eq!(
            "Authentication token invalid",
            response.text().await.unwrap()
        );
    };

    await_client_server_flow!(api_server_handle, client_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SAFE_METHODS: [http::Method; 2] = [http::Method::GET, http::Method::HEAD];

const UNSAFE_METHODS: [http::Method; 4] = [
    http::Method::PUT,
    http::Method::POST,
    http::Method::PATCH,
    http::Method::DELETE,
];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ServerHarness {
    server_future: Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Unpin>,
    local_addr: SocketAddr,
    _temp_dir: tempfile::TempDir,
}

impl ServerHarness {
    pub async fn new(
        current_account_subject: CurrentAccountSubject,
        dataset_permission_checker: MockDatasetPermissionChecker,
    ) -> Self {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let dataset_id = Self::dataset_id();
        let base_catalog = {
            let mut b = dill::CatalogBuilder::new();

            kamu_datasets_inmem::register_dependencies(&mut b);
            b.add::<SystemTimeSourceDefault>()
                .add_value(MockDidGenerator::predefined_dataset_ids(vec![
                    dataset_id.clone(),
                ]))
                .bind::<dyn DidGenerator, MockDidGenerator>()
                .add::<DummyOutboxImpl>()
                .add::<DatabaseTransactionRunner>()
                .add::<DependencyGraphServiceImpl>()
                .add_value(MockAuthenticationService::resolving_token(
                    DUMMY_ACCESS_TOKEN,
                    Account::dummy(),
                ))
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add_value(dataset_permission_checker)
                .bind::<dyn kamu_datasets::DatasetPermissionChecker, MockDatasetPermissionChecker>()
                .add_value(TenancyConfig::SingleTenant)
                .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                    datasets_dir,
                ))
                .add::<kamu_datasets_services::DatasetLfsBuilderDatabaseBackedImpl>()
                .add_value(kamu_datasets_services::MetadataChainDbBackedConfig::default())
                .add::<CreateDatasetUseCaseImpl>()
                .add::<CreateDatasetUseCaseHelper>()
                .add::<DatasetHeadCommitterImpl>()
                .add::<DatasetReferenceServiceImpl>()
                .add::<DatasetEntryServiceImpl>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add::<InMemoryAccountRepository>()
                .add_value(DidSecretEncryptionConfig::sample());

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        base_catalog
            .get_one::<dyn AccountRepository>()
            .unwrap()
            .save_account(&Account::dummy())
            .await
            .unwrap();

        let catalog_system = dill::CatalogBuilder::new_chained(&base_catalog)
            .add_value(CurrentAccountSubject::new_test())
            .build();

        let create_dataset = catalog_system
            .get_one::<dyn CreateDatasetUseCase>()
            .unwrap();
        create_dataset
            .execute(
                &Self::dataset_alias(),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root)
                        .id(dataset_id.clone())
                        .build(),
                )
                .build_typed(),
                Default::default(),
            )
            .await
            .unwrap();

        let catalog_test = dill::CatalogBuilder::new_chained(&base_catalog)
            .add_value(current_account_subject)
            .build();

        let app = axum::Router::new()
            .route(
                "/foo",
                axum::routing::get(Self::foo_handler)
                    .put(Self::foo_handler)
                    .delete(Self::foo_handler)
                    .patch(Self::foo_handler)
                    .post(Self::foo_handler),
            )
            .route("/bar", axum::routing::get(Self::bar_handler))
            .layer(
                tower::ServiceBuilder::new()
                    .layer(axum::Extension(catalog_test))
                    .layer(axum::Extension(dataset_id.as_local_ref()))
                    .layer(kamu_adapter_http::DatasetAuthorizationLayer::new(
                        |request| {
                            if !request.method().is_safe() || request.uri().path() == "/bar" {
                                DatasetPermission::Write
                            } else {
                                DatasetPermission::Read
                            }
                        },
                    )),
            );

        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();

        let server_future = Box::new(axum::serve(listener, app.into_make_service()).into_future());

        Self {
            server_future,
            local_addr,
            _temp_dir: temp_dir,
        }
    }

    pub fn mock_dataset_permission_checker(
        granted_permissions: HashSet<DatasetPermission>,
    ) -> MockDatasetPermissionChecker {
        use mockall::predicate::function;

        let mut mock_dataset_permission_checker = MockDatasetPermissionChecker::new();
        mock_dataset_permission_checker
            .expect_get_granted_permissions()
            .with(function(|dataset_id: &odf::DatasetID| {
                *dataset_id == Self::dataset_id()
            }))
            .returning(move |_dataset_id| Ok(granted_permissions.clone()));

        mock_dataset_permission_checker
    }

    pub async fn api_server_run(self) -> Result<(), InternalError> {
        self.server_future.await.int_err()
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

        assert_eq!(expected_status, response.status());
    }

    pub fn test_url(&self, path: &str) -> Url {
        Url::from_str(format!("http://{}/{}", self.local_addr, path).as_str()).unwrap()
    }

    #[expect(clippy::unused_async, reason = "axum handlers must be async")]
    async fn foo_handler() -> http::StatusCode {
        http::StatusCode::OK
    }

    #[expect(clippy::unused_async, reason = "axum handlers must be async")]
    async fn bar_handler() -> http::StatusCode {
        http::StatusCode::OK
    }

    fn dataset_alias() -> odf::DatasetAlias {
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("mydataset"))
    }

    fn dataset_id() -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(Self::dataset_alias().dataset_name.as_bytes())
    }
}
