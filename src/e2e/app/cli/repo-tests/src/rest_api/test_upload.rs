// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_adapter_http::PlatformFileUploadQuery;
use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt, UploadPrepareError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
pub async fn test_upload(mut kamu_api_server_client: KamuApiServerClient) {
    const DUMMY_FILE_NAME: &str = "dummy.txt";
    const DUMMY_FILE_CONTENT: &str = "It's certainly not a waste of space in the S3 bucket.";

    let options = PlatformFileUploadQuery {
        file_name: DUMMY_FILE_NAME.into(),
        content_length: DUMMY_FILE_CONTENT.len(),
        content_type: None,
    };

    assert_matches!(
        kamu_api_server_client
            .upload()
            .prepare(options.clone())
            .await,
        Err(UploadPrepareError::Unauthorized)
    );

    kamu_api_server_client.auth().login_as_kamu().await;

    let upload_context = kamu_api_server_client
        .upload()
        .prepare(options)
        .await
        .unwrap();

    assert_matches!(
        kamu_api_server_client
            .upload()
            .upload_file(&upload_context, DUMMY_FILE_NAME, DUMMY_FILE_CONTENT)
            .await,
        Ok(_)
    );

    assert_matches!(
        kamu_api_server_client
            .upload()
            .get_file_content(&upload_context)
            .await,
        Ok(actual_content)
            if actual_content == DUMMY_FILE_CONTENT
    );

    // Anonymous access
    kamu_api_server_client.auth().logout();

    assert_matches!(
        kamu_api_server_client
            .upload()
            .get_file_content(&upload_context)
            .await,
        Ok(actual_content)
            if actual_content == DUMMY_FILE_CONTENT
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
