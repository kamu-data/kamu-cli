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
use kamu_accounts_inmem::AccountRepositoryInMemory;
use kamu_accounts_services::{AuthenticationServiceImpl, LoginPasswordAuthProvider};
use kamu_adapter_http::{FileUploadLimitConfig, UploadContext, UploadService, UploadServiceLocal};

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

    fn upload_get_url(&self, file_name: &str, file_size: usize) -> String {
        format!(
            "http://{}/platform/file/upload?fileName={file_name}&contentLength={file_size}",
            self.api_server_addr(),
        )
    }

    fn upload_post_url(&self, upload_id: &str, file_name: &str) -> String {
        format!(
            "http://{}/platform/file/upload/{}/{}",
            self.api_server_addr(),
            upload_id,
            file_name
        )
    }

    fn target_path_from_upload_url(cache_dir: &Path, upload_url: &str) -> PathBuf {
        const PATTERN: &str = "platform/file/upload/";
        let pos = upload_url
            .find(PATTERN)
            .expect("pattern not found in upload url")
            .add(PATTERN.len());

        let url_suffix = &upload_url[pos..];
        cache_dir
            .join("uploads")
            .join(DEFAULT_ACCOUNT_ID.as_multibase().to_string())
            .join(url_suffix)
    }

    async fn api_server_run(self) -> Result<(), InternalError> {
        self.api_server.run().await.int_err()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_unauthorized() {
    let harness = Harness::new();
    let upload_get_url = harness.upload_get_url("test.txt", 100);
    let upload_post_url = harness.upload_post_url("upload-1", "test.txt");

    let client = async move {
        let client = reqwest::Client::new();

        let upload_get_response = client.get(upload_get_url.clone()).send().await.unwrap();
        assert_eq!(401, upload_get_response.status());
        assert_eq!(
            "No authentication token provided",
            upload_get_response.text().await.unwrap()
        );

        let upload_post_reponse = client
            .post(upload_post_url)
            .body("some file")
            .send()
            .await
            .unwrap();
        assert_eq!(401, upload_post_reponse.status());
        assert_eq!(
            "No authentication token provided",
            upload_post_reponse.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_attempt_upload_file_authorized() {
    const FILE_BODY: &str = "a-test-file-body";

    let harness = Harness::new();
    let upload_url = harness.upload_get_url("test.txt", FILE_BODY.len());
    let access_token = harness.access_token.clone();
    let cache_dir = harness.cache_dir.clone();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_get_response = client
            .get(upload_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(200, upload_get_response.status());
        let upload_context = upload_get_response.json::<UploadContext>().await.unwrap();
        assert_eq!("POST", upload_context.method);
        assert!(upload_context.fields.is_empty());

        let upload_url = upload_context.upload_url;

        let upload_response = client
            .post(upload_url.clone())
            .bearer_auth(access_token)
            .body(FILE_BODY)
            .send()
            .await
            .unwrap();

        assert_eq!(200, upload_response.status());

        let expected_upload_path = Harness::target_path_from_upload_url(&cache_dir, &upload_url);
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
    let upload_url = harness.upload_get_url("test.txt", FILE_BODY.len());
    let upload_post_url = harness.upload_post_url("upload-1", "test.txt");
    let access_token = harness.access_token.clone();

    let client = async move {
        let client = reqwest::Client::new();

        let upload_get_response = client
            .get(upload_url)
            .bearer_auth(access_token.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(400, upload_get_response.status());
        assert_eq!(
            "Content too large",
            upload_get_response.text().await.unwrap()
        );

        let upload_post_reponse = client
            .post(upload_post_url)
            .bearer_auth(access_token.clone())
            .body(FILE_BODY)
            .send()
            .await
            .unwrap();
        assert_eq!(400, upload_post_reponse.status());
        assert_eq!(
            "Content too large",
            upload_post_reponse.text().await.unwrap()
        );
    };

    await_client_server_flow!(harness.api_server_run(), client);
}

/////////////////////////////////////////////////////////////////////////////////////////
