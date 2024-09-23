// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use http::{HeaderMap, HeaderName, HeaderValue};
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::ServerUrlConfig;
use kamu::testing::LocalS3Server;
use kamu::utils::s3_context::S3Context;
use kamu_accounts::{JwtAuthenticationConfig, PredefinedAccountsConfig, DEFAULT_ACCOUNT_ID};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_adapter_http::{FileUploadLimitConfig, UploadContext, UploadService, UploadServiceS3};
use time_source::SystemTimeSourceDefault;
use tokio::io::AsyncReadExt;

use crate::harness::{await_client_server_flow, TestAPIServer};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    _s3: LocalS3Server,
    s3_upload_context: S3Context,
    access_token: String,
    api_server: TestAPIServer,
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
                .add_value(JwtAuthenticationConfig::default())
                .add_value(ServerUrlConfig::new_test(Some(&api_server_address)))
                .add_value(FileUploadLimitConfig::new_in_bytes(100))
                .add_builder(
                    UploadServiceS3::builder().with_s3_upload_context(s3_upload_context.clone()),
                )
                .bind::<dyn UploadService, UploadServiceS3>()
                .add::<PredefinedAccountsRegistrator>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        DatabaseTransactionRunner::new(catalog.clone())
            .transactional(|transactional_catalog| async move {
                let registrator = transactional_catalog
                    .get_one::<PredefinedAccountsRegistrator>()
                    .unwrap();

                registrator
                    .ensure_predefined_accounts_are_registered()
                    .await
            })
            .await
            .unwrap();

        let authentication_service = catalog.get_one::<AuthenticationServiceImpl>().unwrap();
        let access_token = authentication_service
            .make_access_token(&DEFAULT_ACCOUNT_ID, 60)
            .unwrap();

        let api_server = TestAPIServer::new(catalog, listener, true);

        Self {
            _s3: s3,
            s3_upload_context,
            access_token,
            api_server,
        }
    }

    fn api_server_addr(&self) -> String {
        self.api_server.local_addr().to_string()
    }

    fn upload_prepare_url(&self, file_name: &str, content_type: &str, file_size: usize) -> String {
        format!(
            "http://{}/platform/file/upload/prepare?fileName={file_name}&contentType={content_type}&contentLength={file_size}",
            self.api_server_addr(),
        )
    }

    async fn read_bucket_file_as_string(bucket_contest: &S3Context, file_key: String) -> String {
        let get_object_output = bucket_contest.get_object(file_key).await.unwrap();
        let mut stream = get_object_output.body.into_async_read();
        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.unwrap();
        String::from_utf8(data).expect("Our bytes should be valid utf8")
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
        assert_eq!(401, upload_prepare_response.status());
        assert_eq!(
            "No authentication token provided",
            upload_prepare_response.text().await.unwrap()
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
    let access_token = harness.access_token.clone();
    let upload_bucket_context = harness.s3_upload_context.clone();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(200, upload_prepare_response.status());
        let upload_context = upload_prepare_response
            .json::<UploadContext>()
            .await
            .unwrap();
        assert_eq!("PUT", upload_context.method);
        assert!(!upload_context.use_multipart);
        assert!(upload_context.fields.is_empty());

        assert_eq!(
            upload_context.headers,
            vec![(String::from("x-amz-acl"), String::from("private"))]
        );

        let upload_main_url = upload_context.upload_url;

        let mut header_map = HeaderMap::new();
        for header in upload_context.headers {
            header_map.insert(
                HeaderName::from_bytes(header.0.as_bytes()).unwrap(),
                HeaderValue::from_str(&header.1).unwrap(),
            );
        }

        let s3_upload_response = client
            .put(upload_main_url.clone())
            .headers(header_map)
            .body(FILE_BODY)
            .send()
            .await
            .unwrap();

        assert_eq!(200, s3_upload_response.status());

        let upload_token = upload_context.upload_token.0;

        let expected_key = format!(
            "{}/{}/{}",
            DEFAULT_ACCOUNT_ID.as_multibase(),
            upload_token.upload_id,
            "test.txt"
        );
        let file_exists = upload_bucket_context
            .bucket_path_exists(&expected_key)
            .await
            .unwrap();
        assert!(file_exists);

        let actual_file_body =
            Harness::read_bucket_file_as_string(&upload_bucket_context, expected_key).await;
        assert_eq!(actual_file_body, FILE_BODY);
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
    let access_token = harness.access_token.clone();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_prepare_response = client
            .post(upload_prepare_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(400, upload_prepare_response.status());
        assert_eq!(
            "Content too large",
            upload_prepare_response.text().await.unwrap()
        );

        // Note: S3/Minio will of course accept up to their limit (5Gb?)
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
