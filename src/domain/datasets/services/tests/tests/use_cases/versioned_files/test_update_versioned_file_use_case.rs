// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_datasets::UpdateVersionFileUseCaseError;
use pretty_assertions::assert_eq;

use super::versioned_file_base_use_case_harness::VersionedFileBaseUseCaseHarness as Harness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_uploads_sequential_versions() {
    let harness = Harness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    for (expected_version, content) in [(1, b"foo".as_slice()), (2, b"foo_new"), (3, b"bar")] {
        let res = harness.upload(&file, content).await;

        assert_eq!(
            (res.new_version, res.content_hash),
            (expected_version, Harness::content_hash(content))
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_extra_data_only_keeps_content() {
    let harness = Harness::new();
    let file = harness
        .create_versioned_file(vec![odf::schema::DataField::string("tag").optional()])
        .await;

    harness.upload(&file, b"foo").await;
    let res = harness
        .update_extra_data(&file, serde_json::json!({"tag": "x"}))
        .await;

    assert_eq!(
        (res.new_version, res.content_hash),
        (2, Harness::content_hash(b"foo"))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_extra_data_only_without_versions_fails() {
    let harness = Harness::new();
    let file = harness
        .create_versioned_file(vec![odf::schema::DataField::string("tag").optional()])
        .await;
    let chain_len = harness.chain_len(&file).await;

    let res = harness
        .update_extra_data_res(&file, serde_json::json!({"tag": "x"}))
        .await;

    assert_matches!(res, Err(UpdateVersionFileUseCaseError::Internal(_)));
    assert_eq!(harness.chain_len(&file).await, chain_len);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_update_versioned_file_stale_expected_head_fails() {
    let harness = Harness::new();
    let file = harness.create_versioned_file(Vec::new()).await;

    let res = harness.upload(&file, b"foo").await;
    harness.upload(&file, b"bar").await;

    let res = harness.upload_res(&file, b"baz", Some(res.old_head)).await;

    assert_matches!(res, Err(UpdateVersionFileUseCaseError::RefCASFailed(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
