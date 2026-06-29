// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    DeleteResourcesError,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceLifecycleMessage,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;
use messaging_outbox::MockOutbox;

use crate::tests::use_cases::resource_use_case_base_harness::ResourceUseCaseBaseHarness;
use crate::tests::utils::{make_account_id, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_single_resource_success() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let (id, snapshot) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-a")
        .await;
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));

    harness.delete_resources(account_id, vec![id]).await;

    assert!(harness.get_snapshot_by_id(&id).await.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_multiple_resources_in_one_call() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let (id_a, snapshot_a) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-a")
        .await;
    let (id_b, snapshot_b) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-b")
        .await;
    let (id_c, snapshot_c) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-c")
        .await;

    pretty_assertions::assert_eq!(snapshot_a.spec, serde_json::json!({ "value": "res-a" }));
    pretty_assertions::assert_eq!(snapshot_b.spec, serde_json::json!({ "value": "res-b" }));
    pretty_assertions::assert_eq!(snapshot_c.spec, serde_json::json!({ "value": "res-c" }));

    harness
        .delete_resources(account_id, vec![id_a, id_b, id_c])
        .await;

    for id in [&id_a, &id_b, &id_c] {
        assert!(
            harness.get_snapshot_by_id(id).await.is_none(),
            "expected snapshot for {id} to be gone"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_nonexistent_id_is_idempotent() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let random_id = make_id();

    // Non-existent IDs land in `outcome.not_found` and are silently skipped
    harness.delete_resources(account_id, vec![random_id]).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_resource_owned_by_other_account_fails() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();

    let (id, snapshot) = harness
        .apply_and_assert_snapshot(account_a.clone(), "res-a")
        .await;
    pretty_assertions::assert_eq!(snapshot.spec, serde_json::json!({ "value": "res-a" }));

    let result = harness.delete_test_uc().execute(account_b, vec![id]).await;

    assert!(
        matches!(result, Err(DeleteResourcesError::Access(_))),
        "expected Access error, got: {result:?}",
    );

    // Resource must still exist after an unauthorized delete attempt
    assert!(
        harness.get_snapshot_by_id(&id).await.is_some(),
        "resource must still exist after failed delete"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_emits_deleted_lifecycle_message() {
    let mut mock_outbox = MockOutbox::new();
    // apply_and_get_id posts one Applied message; expect it before the Deleted one
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    let id = harness.apply_and_get_id(account_id.clone(), "res-a").await;

    harness.delete_resources(account_id, vec![id]).await;
    // MockOutbox verifies .times() expectations on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_multiple_emits_single_message_with_all_resources() {
    let mut mock_outbox = MockOutbox::new();
    // Three apply calls → three Applied messages;
    // one delete call → one Deleted message
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 3, None);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 3);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    let id_a = harness.apply_and_get_id(account_id.clone(), "res-a").await;
    let id_b = harness.apply_and_get_id(account_id.clone(), "res-b").await;
    let id_c = harness.apply_and_get_id(account_id.clone(), "res-c").await;

    harness
        .delete_resources(account_id, vec![id_a, id_b, id_c])
        .await;
    // MockOutbox verifies exactly one Deleted message with 3 resources on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_no_op_emits_no_message() {
    // Deleting non-existent UIDs must not post any outbox message.
    // Covers the `if !resources.is_empty()` guard in delete.rs.
    let mut mock_outbox = MockOutbox::new();
    mock_outbox.expect_post_message_as_json().times(0);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    harness.delete_resources(account_id, vec![make_id()]).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_message_carries_correct_snapshot_data() {
    // Verify the Deleted message payload contains the correct UID and spec.
    let mut mock_outbox = MockOutbox::new();

    // apply_and_get_id posts one Applied message; accept it first
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);

    // Use a shared cell to capture the UID inside the withf closure, since we
    // don't know it until after apply.
    let uid_cell = std::sync::Arc::new(std::sync::Mutex::new(None::<kamu_resources::ResourceID>));
    let uid_cell_clone = uid_cell.clone();

    mock_outbox
        .expect_post_message_as_json()
        .times(1)
        .withf(move |producer, message, _version| {
            let expected_id = uid_cell_clone.lock().unwrap();
            let Some(expected_id) = *expected_id else {
                return false;
            };
            producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                && match serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                    .unwrap()
                {
                    ResourceLifecycleMessage::Deleted(m) => {
                        m.resources.len() == 1
                            && m.resources[0].id == expected_id
                            && m.resources[0].spec == serde_json::json!({ "value": "res-a" })
                    }
                    _ => false,
                }
        })
        .returning(|_, _, _| Ok(()));

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    let id = harness.apply_and_get_id(account_id.clone(), "res-a").await;
    *uid_cell.lock().unwrap() = Some(id);

    harness.delete_resources(account_id, vec![id]).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
