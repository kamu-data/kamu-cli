// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{SocketAddr, TcpListener};
use std::ops::Add;
use std::path::{Path, PathBuf};

use dill::Component;
use kamu::domain::{InternalError, ResultIntoInternal, ServerUrlConfig, SystemTimeSourceDefault};
use kamu_accounts::{JwtAuthenticationConfig, PredefinedAccountsConfig, DEFAULT_ACCOUNT_ID};
use kamu_accounts_inmem::{AccessTokenRepositoryInMemory, AccountRepositoryInMemory};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
};
use kamu_adapter_http::{
    decode_upload_token_payload,
    FileUploadLimitConfig,
    UploadContext,
    UploadService,
    UploadServiceLocal,
};

use crate::harness::{await_client_server_flow, TestAPIServer};

/////////////////////////////////////////////////////////////////////////////////////////

struct Harness {
    _tempdir: tempfile::TempDir,
    cache_dir: PathBuf,
    access_token: String,
    api_server: TestAPIServer,
}

impl Harness {
    fn new() -> Self {
        let addr = SocketAddr::from(([127, 0, 0, 1], 0));
        let bind_socket = TcpListener::bind(addr).unwrap();
        let api_server_address = format!(
            "http://localhost:{}",
            bind_socket.local_addr().unwrap().port()
        );

        let tempdir = tempfile::tempdir().unwrap();
        let cache_dir = tempdir.path().join("cache");

        let catalog = dill::CatalogBuilder::new()
            .add_value(PredefinedAccountsConfig::single_tenant())
            .add::<AuthenticationServiceImpl>()
            .add::<AccountRepositoryInMemory>()
            .add::<SystemTimeSourceDefault>()
            .add::<LoginPasswordAuthProvider>()
            .add::<AccessTokenServiceImpl>()
            .add::<AccessTokenRepositoryInMemory>()
            .add_value(JwtAuthenticationConfig::default())
            .add_value(ServerUrlConfig::new_test(Some(&api_server_address)))
            .add_value(FileUploadLimitConfig {
                max_file_size_in_bytes: 100,
            })
            .add_builder(UploadServiceLocal::builder().with_cache_dir(cache_dir.clone()))
            .bind::<dyn UploadService, UploadServiceLocal>()
            .build();

        let authentication_service = catalog.get_one::<AuthenticationServiceImpl>().unwrap();
        let access_token = authentication_service
            .make_access_token(&DEFAULT_ACCOUNT_ID, 60)
            .unwrap();

        let api_server = TestAPIServer::new(catalog, bind_socket, true);

        Self {
            _tempdir: tempdir,
            cache_dir,
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
        let upload_token_payload = decode_upload_token_payload(url_suffix).unwrap();

        cache_dir
            .join("uploads")
            .join(DEFAULT_ACCOUNT_ID.as_multibase().to_string())
            .join(upload_token_payload.upload_id)
            .join(upload_token_payload.file_name)
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_unauthorized() {
    let harness = Harness::new();
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", 100);
    let upload_main_url = harness.upload_main_url();

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

        let upload_main_reponse = client
            .post(format!("{upload_main_url}/someUploadToken"))
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
        assert_eq!(401, upload_main_reponse.status());
        assert_eq!(
            "No authentication token provided",
            upload_main_reponse.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_authorized() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new();
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let access_token = harness.access_token.clone();
    let cache_dir = harness.cache_dir.clone();

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
        assert_eq!("POST", upload_context.method);
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

        assert_eq!(200, upload_main_response.status());

        let expected_upload_path =
            Harness::target_path_from_upload_url(&cache_dir, &upload_main_url);
        let file_body = std::fs::read_to_string(expected_upload_path).unwrap();
        assert_eq!(FILE_BODY, file_body);
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

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

    let harness = Harness::new();
    let upload_prepare_url = harness.upload_prepare_url("test.txt", "text/plain", FILE_BODY.len());
    let upload_main_url = harness.upload_main_url();
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

        let upload_main_reponse = client
            .post(format!("{upload_main_url}/someUploadToken"))
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
        assert_eq!(400, upload_main_reponse.status());
        assert_eq!(
            "Content too large",
            upload_main_reponse.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_that_has_different_length_than_declared() {
    const FILE_BODY: &str = "Some text";

    let harness = Harness::new();
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

        assert_eq!(200, upload_prepare_response.status());
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
        assert_eq!(400, upload_main_response.status());
        assert_eq!(
            "Actual content length 27 does not match the initially declared length 9",
            upload_main_response.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////
