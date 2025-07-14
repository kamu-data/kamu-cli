// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::sync::Arc;

use database_common::NoOpDatabasePlugin;
use http::{HeaderMap, HeaderName, HeaderValue};
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::{FileUploadLimitConfig, ServerUrlConfig, TenancyConfig, UploadContext};
use kamu_accounts::{
    AuthConfig,
    DEFAULT_ACCOUNT_ID,
    DidSecretEncryptionConfig,
    JwtAuthenticationConfig,
    JwtTokenIssuer,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AccountServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
    PredefinedAccountsRegistrator,
};
use kamu_adapter_http::platform::UploadServiceS3;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use s3_utils::S3Context;
use serde_json::json;
use test_utils::LocalS3Server;
use time_source::SystemTimeSourceDefault;
use tokio::io::AsyncReadExt;

use crate::harness::{TestAPIServer, await_client_server_flow};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    _s3: LocalS3Server,
    s3_upload_context: S3Context,
    api_server: TestAPIServer,
    authentication_service: Arc<AuthenticationServiceImpl>,
}

impl Harness {
    async fn new() -> Self {
        let s3 = LocalS3Server::new().await;
        let s3_upload_context = S3Context::from_url(&s3.url).await;

        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let api_server_address =
            format!("http://localhost:{}", listener.local_addr().unwrap().port());

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(PredefinedAccountsConfig::single_tenant())
                .add::<AuthenticationServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add::<SystemTimeSourceDefault>()
                .add::<LoginPasswordAuthProvider>()
                .add::<AccountServiceImpl>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add_value(JwtAuthenticationConfig::default())
                .add_value(AuthConfig::sample())
                .add_value(ServerUrlConfig::new_test(Some(&api_server_address)))
                .add_value(FileUploadLimitConfig::new_in_bytes(100))
                .add_builder(UploadServiceS3::builder(s3_upload_context.clone()))
                .add::<PredefinedAccountsRegistrator>()
                .add::<RebacServiceImpl>()
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
            _s3: s3,
            s3_upload_context,
            api_server,
            authentication_service,
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

    fn upload_prepare_url(&self, file_name: &str, content_type: &str, file_size: usize) -> String {
        format!(
            "http://{}/platform/file/upload/prepare?fileName={file_name}&contentType={content_type}&contentLength={file_size}",
            self.api_server_addr(),
        )
    }

    fn upload_retrieve_url(&self) -> String {
        format!("http://{}/platform/file/upload", self.api_server_addr(),)
    }

    async fn read_bucket_file_as_string(bucket_contest: &S3Context, file_key: String) -> String {
        let get_object_output = bucket_contest.get_object(file_key).await.unwrap();
        let mut stream = get_object_output.body.into_async_read();
        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.unwrap();
        String::from_utf8(data).expect("Our bytes should be valid utf8")
    }

    fn make_header_map(upload_context: &UploadContext) -> HeaderMap {
        let mut header_map = HeaderMap::new();
        for header in &upload_context.headers {
            header_map.insert(
                HeaderName::from_bytes(header.0.as_bytes()).unwrap(),
                HeaderValue::from_str(&header.1).unwrap(),
            );
        }
        header_map
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_unauthorized() {
    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", 100);

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
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_authorized() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new().await;

    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let upload_bucket_context = harness.s3_upload_context.clone();

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
        pretty_assertions::assert_eq!(http::method::Method::PUT.as_str(), upload_context.method);
        assert!(!upload_context.use_multipart);
        assert!(upload_context.fields.is_empty());

        pretty_assertions::assert_eq!(
            vec![(String::from("x-amz-acl"), String::from("private"))],
            upload_context.headers,
        );

        let header_map = Harness::make_header_map(&upload_context);
        let s3_upload_url = upload_context.upload_url;

        let s3_upload_response = client
            .put(s3_upload_url)
            .headers(header_map)
            .body(FILE_BODY)
            .send()
            .await
            .unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, s3_upload_response.status());

        let upload_token = upload_context.upload_token.0;

        let expected_key = format!(
            "{}/{}/test.txt",
            DEFAULT_ACCOUNT_ID.as_id_without_did_prefix(),
            upload_token.upload_id
        );
        let file_exists = upload_bucket_context
            .bucket_path_exists(&expected_key)
            .await
            .unwrap();
        assert!(file_exists);

        let actual_file_body =
            Harness::read_bucket_file_as_string(&upload_bucket_context, expected_key).await;
        pretty_assertions::assert_eq!(FILE_BODY, actual_file_body);
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
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
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);

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

        // Note: S3/Minio will of course accept up to their limit (5Gb?)
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_upload_then_read_file() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new().await;
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let retrieve_url = harness.upload_retrieve_url();
    let access_token = harness.make_access_token(&DEFAULT_ACCOUNT_ID);
    let different_access_token =
        harness.make_access_token(&(odf::AccountID::new_generated_ed25519().1));

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

        let header_map = Harness::make_header_map(&upload_context);
        let s3_upload_url = upload_context.upload_url;

        let s3_upload_response = client
            .put(s3_upload_url)
            .headers(header_map)
            .body(FILE_BODY)
            .send()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, s3_upload_response.status());

        // Read file with the same authorization token
        let get_url = format!("{retrieve_url}/{}", upload_context.upload_token);
        let upload_retrieve_response = client
            .get(get_url.clone())
            .bearer_auth(access_token)
            .send()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);

        // Read file with different authorization token
        let get_url = format!("{retrieve_url}/{}", upload_context.upload_token);
        let upload_retrieve_response = client
            .get(get_url.clone())
            .bearer_auth(different_access_token)
            .send()
            .await
            .unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);

        // Read file anonymously
        let upload_retrieve_response = client.get(get_url).send().await.unwrap();
        pretty_assertions::assert_eq!(http::StatusCode::OK, upload_retrieve_response.status());
        let file_body = upload_retrieve_response.text().await.unwrap();
        pretty_assertions::assert_eq!(FILE_BODY, file_body);
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
