// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources_services::testing::BaseResourceServiceHarness;
use messaging_outbox::MockOutbox;

use crate::tests::utils::ServiceLayerTestHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_account_deletion_leaves_other_accounts_intact() {
    let harness = ServiceLayerTestHarness::new();
    let account_handle_a = odf::ResourceOwnerHandle::new_test("test-owner-a");
    let account_handle_b = odf::ResourceOwnerHandle::new_test("test-owner-b");

    harness.apply_and_get_id(&account_handle_a, "res-a").await;

    let id_b = harness.apply_and_get_id(&account_handle_b, "res-b").await;

    harness
        .request_account_resources_deletion(account_handle_a.did)
        .await;

    harness.assert_exists(&id_b).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_account_deletion_handles_pagination() {
    // 101 resources span more than one page of the account listing (100 per page)
    let harness = ServiceLayerTestHarness::new();
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    let names = (0..=100).map(|i| format!("res-{i:03}")).collect::<Vec<_>>();
    let ids = harness.apply_all(&account_handle, &names).await;

    harness
        .request_account_resources_deletion(account_handle.did)
        .await;
    harness.settle().await;

    harness.assert_gone(&ids).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_account_deletion_multiple_resources_emits_single_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 3, None);
    BaseResourceServiceHarness::expect_deletion_requested_message(&mut mock_outbox, 3);

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    harness
        .apply_all(&account_handle, &["res-a", "res-b", "res-c"])
        .await;

    harness
        .request_account_resources_deletion(account_handle.did)
        .await;
    // MockOutbox verifies exactly one DeletionRequested message with 3 resources on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_account_deletion_no_resources_emits_no_message() {
    let mut mock_outbox = MockOutbox::new();
    mock_outbox.expect_post_message_as_json().times(0);

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    harness
        .request_account_resources_deletion(account_handle.did)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_request_account_deletion_message_carries_correct_snapshot_data() {
    let mut mock_outbox = MockOutbox::new();

    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);

    BaseResourceServiceHarness::expect_deletion_requested_message_of(
        &mut mock_outbox,
        "res-a",
        serde_json::json!({ "value": "res-a" }),
    );

    let harness = ServiceLayerTestHarness::new_with_mock_outbox(mock_outbox);
    let account_handle = odf::ResourceOwnerHandle::new_test("test-owner");

    harness.apply_and_get_id(&account_handle, "res-a").await;

    harness
        .request_account_resources_deletion(account_handle.did)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
