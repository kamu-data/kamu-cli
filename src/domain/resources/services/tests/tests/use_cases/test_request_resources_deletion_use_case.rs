// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_resources::RequestResourcesDeletionError;
use kamu_resources_services::testing::BaseResourceServiceHarness;
use messaging_outbox::MockOutbox;

use crate::tests::utils::{ServiceLayerTestHarness, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_deletion_of_multiple_resources_in_one_call() {
    let harness = ServiceLayerTestHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let ids = harness
        .apply_all(&account_handle, &["res-a", "res-b", "res-c"])
        .await;

    harness
        .request_resources_deletion(account_handle.did, ids.clone())
        .await;
    harness.settle().await;

    harness.assert_gone(&ids).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_deletion_of_resource_owned_by_other_account_fails() {
    let harness = ServiceLayerTestHarness::new();
    let account_handle_a = odf::ResourceOwnerHandle::new_test("test-owner-a");
    let account_handle_b = odf::ResourceOwnerHandle::new_test("test-owner-b");

    let id = harness.apply_and_get_id(&account_handle_a, "res-a").await;

    let result = harness
        .request_deletion_test_uc()
        .execute(account_handle_b.did, vec![id])
        .await;

    assert_matches!(
        result,
        Err(RequestResourcesDeletionError::Access(_)),
        "expected Access error, got: {result:?}"
    );

    harness.assert_exists(&id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_deletion_of_multiple_emits_single_message_with_all_resources() {
    let mut mock_outbox = MockOutbox::new();
    // Three apply calls → three Applied messages;
    // one delete call → one DeletionRequested message
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 3, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 3);

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let ids = harness
        .apply_all(&account_handle, &["res-a", "res-b", "res-c"])
        .await;

    harness
        .request_resources_deletion(account_handle.did, ids)
        .await;
    // MockOutbox verifies exactly one DeletionRequested message with 3 resources on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_repeated_deletion_request_posts_message_again() {
    // Requesting the deletion of a resource being deleted posts the message again
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 1);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 1);

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness
        .request_resources_deletion(account_handle.did.clone(), vec![id])
        .await;
    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_deletion_no_op_emits_no_message() {
    // A request that matches no resources posts no outbox message
    let mut mock_outbox = MockOutbox::new();
    mock_outbox.expect_post_message_as_json().times(0);

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    harness
        .request_resources_deletion(account_handle.did, vec![make_id()])
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deletion_requested_message_carries_correct_snapshot_data() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_deletion_requested_message_of(
        &mut mock_outbox,
        "res-a",
        serde_json::json!({ "value": "res-a" }),
    );

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let id = harness.apply_and_get_id(&account_handle, "res-a").await;

    harness
        .request_resources_deletion(account_handle.did, vec![id])
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
