// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{CacheDir, ServerUrlConfig};
use kamu_accounts::{
    AccountConfig,
    JwtAuthenticationConfig,
    JwtTokenIssuer,
    PredefinedAccountsConfig,
    DEFAULT_ACCOUNT_ID,
};
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryAccountRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
    PredefinedAccountsRegistrator,
};
use kamu_adapter_http::platform::{
    FileUploadLimitConfig,
    UploadContext,
    UploadServiceLocal,
    UploadToken,
    UploadTokenBase64Json,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
    RebacServiceImplCacheState,
};
use kamu_core::{MediaType, TenancyConfig};
use messaging_outbox::DummyOutboxImpl;
use serde_json::json;
use time_source::SystemTimeSourceDefault;

use crate::harness::{await_client_server_flow, TestAPIServer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    _tempdir: tempfile::TempDir,
    cache_dir: PathBuf,
    api_server: TestAPIServer,
    authentication_service: Arc<AuthenticationServiceImpl>,
    another_account_id: odf::AccountID,
}

impl Harness {
    async fn new() -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let api_server_address =
            format!("http://localhost:{}", listener.local_addr().unwrap().port());

        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");

        const ANOTHER_ACCOUNT_NAME: &str = "another-account";

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            let mut predefined_account_configs = PredefinedAccountsConfig::single_tenant();
            predefined_account_configs
                .predefined
                .push(AccountConfig::test_config_from_name(
                    odf::AccountName::new_unchecked(ANOTHER_ACCOUNT_NAME),
                ));

            b.add_value(CacheDir::new(cache_dir.clone()))
                .add_value(predefined_account_configs)
                .add::<AuthenticationServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add::<SystemTimeSourceDefault>()
                .add::<LoginPasswordAuthProvider>()
                .add_value(JwtAuthenticationConfig::default())
                .add_value(ServerUrlConfig::new_test(Some(&api_server_address)))
                .add_value(FileUploadLimitConfig::new_in_bytes(100))
                .add::<UploadServiceLocal>()
                .add::<PredefinedAccountsRegistrator>()
                .add::<RebacServiceImpl>()
                .add::<RebacServiceImplCacheState>()
                .add::<InMemoryRebacRepository>()
                .add_value(DefaultAccountProperties::default())
                .add_value(DefaultDatasetProperties::default())
                .add::<DummyOutboxImpl>()
                .add::<OAuthDeviceCodeServiceImpl>()
                .add::<OAuthDeviceCodeGeneratorDefault>()
                .add::<InMemoryOAuthDeviceCodeRepository>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        let authentication_service = catalog.get_one::<AuthenticationServiceImpl>().unwrap();

        let api_server = TestAPIServer::new(catalog, listener, TenancyConfig::MultiTenant);

        Self {
            _tempdir: tempdir,
            cache_dir,
            api_server,
            authentication_service,
            another_account_id: odf::AccountID::new_seeded_ed25519(ANOTHER_ACCOUNT_NAME.as_bytes()),
        }
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn make_access_token(&self, account_id: &odf::AccountID) -> String {
        self.authentication_service
            .make_access_token_from_account_id(account_id, 60)
            .unwrap()
            .into_inner()
    }

    fn mock_upload_token(&self) -> UploadTokenBase64Json {
        UploadTokenBase64Json(UploadToken {
            upload_id: "123".to_string(),
            file_name: "someFile.json".to_string(),
            owner_account_id: DEFAULT_ACCOUNT_ID.as_multibase().to_string(),
            content_length: 123,
            content_type: Some(MediaType::JSON.to_owned()),
        })
    }

    fn upload_prepare_url(&self, file_name: &str, content_type: &str, file_size: usize) -> String {
        format!(
            "http://{}/platform/file/upload/prepare?fileName={file_name}&contentType={content_type}&contentLength={file_size}",
            self.api_server_addr(),
        )
    }

    fn upload_main_url(&self) -> String {
        format!("http://{}/platform/file/upload", self.api_server_addr(),)
    }

    fn target_path_from_upload_url(cache_dir: &Path, upload_url: &str) -> PathBuf {
        const PATTERN: &str = "platform/file/upload/";
        let pos = upload_url
            .find(PATTERN)
            .expect("pattern not found in upload url")
            .add(PATTERN.len());

        let url_suffix = &upload_url[pos..];
        let upload_token: UploadTokenBase64Json = url_suffix.parse().unwrap();
        let upload_token = upload_token.0;

        cache_dir
            .join("uploads")
            .join(DEFAULT_ACCOUNT_ID.as_multibase().to_string())
            .join(upload_token.upload_id)
            .join(upload_token.file_name)
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_unauthorized() {
    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", 100);
    let upload_main_url = harness.upload_main_url();
    let upload_token = harness.mock_upload_token();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url.clone())
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            http::StatusCode::UNAUTHORIZED,
            upload_prepare_response.status()
        );
        pretty_assertions::assert_eq!(
            json!({
                "message": "No authentication token provided"
            }),
            upload_prepare_response
                .json::<serde_json::Value>()
                .await
                .unwrap()
        );

        let upload_main_response = client
            .post(format!("{upload_main_url}/{upload_token}"))
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text("some file")
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            http::StatusCode::UNAUTHORIZED,
            upload_main_response.status()
        );
        pretty_assertions::assert_eq!(
            json!({
                "message": "No authentication token provided"
            }),
            upload_main_response
                .json::<serde_json::Value>()
                .await
                .unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_authorized() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let cache_dir = harness.cache_dir.clone();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_prepare_response.status());
        let upload_context = upload_prepare_response
            .json::<UploadContext>()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::method::Method::POST.as_str(), upload_context.method);
        assert!(upload_context.use_multipart);
        assert!(upload_context.fields.is_empty());

        let upload_main_url = upload_context.upload_url;

        let upload_main_response = client
            .post(upload_main_url.clone())
            .bearer_auth(access_token)
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text(FILE_BODY)
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_main_response.status());

        let expected_upload_path =
            Harness::target_path_from_upload_url(&cache_dir, &upload_main_url);
        let file_body = std::fs::read_to_string(expected_upload_path).unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_by_different_user() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let different_access_token = harness.make_access_token(&harness.another_account_id);

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token)
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_prepare_response.status());
        let upload_context = upload_prepare_response
            .json::<UploadContext>()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::method::Method::POST.as_str(), upload_context.method);
        assert!(upload_context.use_multipart);
        assert!(upload_context.fields.is_empty());

        let upload_main_url = upload_context.upload_url;

        let upload_main_response = client
            .post(upload_main_url.clone())
            .bearer_auth(different_access_token)
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text(FILE_BODY)
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::FORBIDDEN, upload_main_response.status());
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_that_is_too_large() {
    const FILE_BODY: &str =
        "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum \
         has been the industry's standard dummy text ever since the 1500s, when an unknown \
         printer took a galley of type and scrambled it to make a type specimen book. It has \
         survived not only five centuries, but also the leap into electronic typesetting, \
         remaining essentially unchanged. It was popularised in the 1960s with the release of \
         Letraset sheets containing Lorem Ipsum passages, and more recently with desktop \
         publishing software like Aldus PageMaker including versions of Lorem Ipsum.";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let upload_main_url = harness.upload_main_url();
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let upload_token = harness.mock_upload_token();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            http::StatusCode::BAD_REQUEST,
            upload_prepare_response.status()
        );
        pretty_assertions::assert_eq!(
            json!({
                "message": "Content too large"
            }),
            upload_prepare_response
                .json::<serde_json::Value>()
                .await
                .unwrap()
        );

        let upload_main_response = client
            .post(format!("{upload_main_url}/{upload_token}"))
            .bearer_auth(access_token.clone())
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text(FILE_BODY)
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::BAD_REQUEST, upload_main_response.status());
        pretty_assertions::assert_eq!(
            json!({
                "message": "Content too large"
            }),
            upload_main_response
                .json::<serde_json::Value>()
                .await
                .unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_that_has_different_length_than_declared() {
    const FILE_BODY: &str = "Some text";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_prepare_response.status());
        let upload_context = upload_prepare_response
            .json::<UploadContext>()
            .await
            .unwrap();

        let upload_main_url = upload_context.upload_url;

        let upload_main_response = client
            .post(upload_main_url.clone())
            .bearer_auth(access_token)
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text("Some totally different text")
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::BAD_REQUEST, upload_main_response.status());
        pretty_assertions::assert_eq!(
            json!({
                "message": "Actual content length 27 does not match the initially declared length 9"
            }),
            upload_main_response
                .json::<serde_json::Value>()
                .await
                .unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_upload_then_read_file() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let different_access_token = harness.make_access_token(&harness.another_account_id);

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_prepare_response.status());
        let upload_context = upload_prepare_response
            .json::<UploadContext>()
            .await
            .unwrap();

        let upload_main_url = upload_context.upload_url;

        let upload_main_response = client
            .post(upload_main_url.clone())
            .bearer_auth(access_token.clone())
            .multipart(
                reqwest::multipart::Form::new().part(
                    "file",
                    reqwest::multipart::Part::text(FILE_BODY)
                        .file_name("test.txt")
                        .mime_str("text/plain")
                        .unwrap(),
                ),
            )
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_main_response.status());

        // Read file with the same authorization token
        let upload_retrieve_response = client
            .get(upload_main_url.clone())
            .bearer_auth(access_token)
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);

        // Read file with a different authorization token
        let upload_retrieve_response = client
            .get(upload_main_url.clone())
            .bearer_auth(different_access_token)
            .send()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);

        // Read file anonymously
        let upload_retrieve_response = client.get(upload_main_url).send().await.unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
