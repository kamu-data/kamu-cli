// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{Duration, Utc};
use file_utils::MediaType;
use kamu::domain::{FileDownload, FileDownloadService, TenancyConfig};
use kamu_accounts::JwtTokenIssuer;
use kamu_adapter_http::platform::FileDownloadServiceJwt;
use odf::metadata::testing::MetadataFactory;

use crate::harness::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FILE_BODY: &[u8] = b"a-test-file-body";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_file_by_token() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let download_url = harness.make_download_url(file);

    let client = async move {
        // No authorization header: the token in the URL is the credential
        let response = reqwest::get(download_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::OK, response.status());
        pretty_assertions::assert_eq!(
            Some("text/plain"),
            response
                .headers()
                .get(http::header::CONTENT_TYPE)
                .map(|v| v.to_str().unwrap()),
        );
        pretty_assertions::assert_eq!(
            Some(u64::try_from(FILE_BODY.len()).unwrap()),
            response.content_length(),
        );
        pretty_assertions::assert_eq!(FILE_BODY, response.bytes().await.unwrap().as_ref());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_isolates_active_content() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let html_url = harness.make_download_url(FileDownload {
        content_type: MediaType("text/html".to_string()),
        ..file.clone()
    });
    let pdf_url = harness.make_download_url(FileDownload {
        content_type: MediaType("application/pdf".to_string()),
        ..file
    });

    let client = async move {
        let security_headers = |response: &reqwest::Response| {
            [
                http::header::X_CONTENT_TYPE_OPTIONS,
                http::header::CONTENT_SECURITY_POLICY,
            ]
            .map(|name| {
                response
                    .headers()
                    .get(name)
                    .map(|v| v.to_str().unwrap().to_string())
            })
        };

        // Scripts of an HTML file must not run with the node's origin
        let response = reqwest::get(html_url).await.unwrap();
        pretty_assertions::assert_eq!(
            [Some("nosniff".to_string()), Some("sandbox".to_string())],
            security_headers(&response),
        );

        // Browsers do not render a PDF in a sandboxed document
        let response = reqwest::get(pdf_url).await.unwrap();
        pretty_assertions::assert_eq!(
            [Some("nosniff".to_string()), None],
            security_headers(&response),
        );
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_with_expired_token() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let download_url = harness.make_expired_download_url(file);

    let client = async move {
        let response = reqwest::get(download_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::UNAUTHORIZED, response.status());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_with_tampered_token() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let download_url = harness.make_download_url(file);

    // Flip the last character of the signature
    let tampered_url = {
        let mut url = download_url;
        let last = url.pop().unwrap();
        url.push(if last == 'A' { 'B' } else { 'A' });
        url
    };

    let client = async move {
        let response = reqwest::get(tampered_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::FORBIDDEN, response.status());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_with_access_token_instead_of_download_token() {
    let harness = DownloadLocalHarness::new().await;
    let access_token = harness.make_access_token();
    let download_url = harness.download_url_with_token(&access_token);

    let client = async move {
        let response = reqwest::get(download_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::FORBIDDEN, response.status());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_missing_file() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let download_url = harness.make_download_url(FileDownload {
        content_hash: odf::Multihash::from_digest_sha3_256(b"not-stored"),
        ..file
    });

    let client = async move {
        let response = reqwest::get(download_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::NOT_FOUND, response.status());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_download_from_missing_dataset() {
    let harness = DownloadLocalHarness::new().await;
    let file = harness.create_dataset_with_file(FILE_BODY).await;
    let download_url = harness.make_download_url(FileDownload {
        dataset_id: odf::DatasetID::new_seeded_ed25519(b"missing"),
        ..file
    });

    let client = async move {
        let response = reqwest::get(download_url).await.unwrap();

        pretty_assertions::assert_eq!(http::StatusCode::NOT_FOUND, response.status());
    };

    await_client_server_flow!(harness.server_harness.api_server_run(), client);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DownloadLocalHarness {
    server_harness: ServerSideLocalFsHarness,
}

impl DownloadLocalHarness {
    async fn new() -> Self {
        let catalog = dill::CatalogBuilder::new()
            .add::<FileDownloadServiceJwt>()
            .build();

        let server_harness = ServerSideLocalFsHarness::new(ServerSideHarnessOptions {
            tenancy_config: TenancyConfig::MultiTenant,
            authorized_writes: true,
            base_catalog: Some(catalog),
        })
        .await;

        Self { server_harness }
    }

    /// Creates an empty dataset and stores `content` in its data repository
    async fn create_dataset_with_file(&self, content: &[u8]) -> FileDownload {
        let create_result = self
            .server_harness
            .cli_create_dataset_use_case()
            .execute(
                &odf::DatasetAlias::new(
                    self.server_harness.operating_account_name(),
                    odf::DatasetName::new_unchecked("files"),
                ),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
                .system_time(Utc::now())
                .build_typed(),
                Default::default(),
            )
            .await
            .unwrap();

        let content_hash = create_result
            .dataset
            .as_data_repo()
            .insert_bytes(content, Default::default())
            .await
            .unwrap()
            .hash;

        FileDownload {
            dataset_id: create_result.dataset_handle.id,
            content_hash,
            content_type: MediaType("text/plain".to_string()),
            content_length: content.len(),
        }
    }

    fn make_download_url(&self, file: FileDownload) -> String {
        self.download_service()
            .make_download_url(file, Default::default())
            .unwrap()
            .url
            .to_string()
    }

    /// Issues a token whose lifetime ended an hour ago
    fn make_expired_download_url(&self, file: FileDownload) -> String {
        self.server_harness
            .system_time_source()
            .set(Utc::now() - Duration::hours(2));

        let url = self.make_download_url(file);

        self.server_harness.system_time_source().unset();
        url
    }

    fn make_access_token(&self) -> String {
        self.server_harness
            .base_catalog()
            .get_one::<dyn JwtTokenIssuer>()
            .unwrap()
            .make_access_token_from_account_id(&self.server_harness.server_account().id, 60)
            .unwrap()
            .into_inner()
    }

    fn download_url_with_token(&self, token: &str) -> String {
        format!(
            "http://{}/platform/file/download/{token}",
            self.server_harness.api_server_addr(),
        )
    }

    fn download_service(&self) -> Arc<dyn FileDownloadService> {
        self.server_harness.base_catalog().get_one().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
