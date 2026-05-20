// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, ResourceLifecycleMessage};
use kamu_resources_services::testing::BaseResourceServiceHarness;
use messaging_outbox::MockOutbox;

use crate::tests::use_cases::resource_use_case_base_harness::ResourceUseCaseBaseHarness;
use crate::tests::utils::make_account_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_no_resources_succeeds() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    harness.delete_account_resources(account_id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_deletes_all_owned_resources() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let (uid_a, snapshot_a) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-a")
        .await;
    let (uid_b, snapshot_b) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-b")
        .await;
    let (uid_c, snapshot_c) = harness
        .apply_and_assert_snapshot(account_id.clone(), "res-c")
        .await;

    pretty_assertions::assert_eq!(snapshot_a.spec, serde_json::json!({ "value": "res-a" }));
    pretty_assertions::assert_eq!(snapshot_b.spec, serde_json::json!({ "value": "res-b" }));
    pretty_assertions::assert_eq!(snapshot_c.spec, serde_json::json!({ "value": "res-c" }));

    harness.delete_account_resources(account_id).await;

    for uid in [&uid_a, &uid_b, &uid_c] {
        let snapshot = harness.get_snapshot_by_uid(uid).await;
        assert!(snapshot.is_none(), "expected snapshot for {uid} to be gone");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_leaves_other_accounts_intact() {
    let harness = ResourceUseCaseBaseHarness::new();
    let account_a = make_account_id();
    let account_b = make_account_id();

    harness.apply_and_get_uid(account_a.clone(), "res-a").await;

    let (uid_b, snapshot_b) = harness
        .apply_and_assert_snapshot(account_b.clone(), "res-b")
        .await;
    pretty_assertions::assert_eq!(snapshot_b.spec, serde_json::json!({ "value": "res-b" }));

    harness.delete_account_resources(account_a).await;

    let snapshot = harness.get_snapshot_by_uid(&uid_b).await;
    assert!(
        snapshot.is_some(),
        "account B's resource should still exist"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_handles_pagination() {
    // PAGE_SIZE in delete_account_resources.rs is 100; create 101 to exceed it
    let harness = ResourceUseCaseBaseHarness::new();
    let account_id = make_account_id();

    let mut uids = Vec::new();
    for i in 0..=100 {
        let uid = harness
            .apply_and_get_uid(account_id.clone(), &format!("res-{i:03}"))
            .await;
        uids.push(uid);
    }

    harness.delete_account_resources(account_id).await;

    for uid in &uids {
        let snapshot = harness.get_snapshot_by_uid(uid).await;
        assert!(snapshot.is_none(), "expected snapshot for {uid} to be gone");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_emits_deleted_lifecycle_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 1);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    harness.apply_and_get_uid(account_id.clone(), "res-a").await;

    harness.delete_account_resources(account_id).await;
    // MockOutbox verifies expectations on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_multiple_resources_emits_single_message() {
    let mut mock_outbox = MockOutbox::new();
    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 3, None);
    BaseResourceServiceHarness::expect_deleted_message(&mut mock_outbox, 3);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    harness.apply_and_get_uid(account_id.clone(), "res-a").await;
    harness.apply_and_get_uid(account_id.clone(), "res-b").await;
    harness.apply_and_get_uid(account_id.clone(), "res-c").await;

    harness.delete_account_resources(account_id).await;
    // MockOutbox verifies exactly one Deleted message with 3 resources on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_no_resources_emits_no_message() {
    let mut mock_outbox = MockOutbox::new();
    mock_outbox.expect_post_message_as_json().times(0);

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    harness.delete_account_resources(account_id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_account_message_carries_correct_snapshot_data() {
    let mut mock_outbox = MockOutbox::new();

    BaseResourceServiceHarness::expect_applied_messages(&mut mock_outbox, 1, None);

    let uid_cell = std::sync::Arc::new(std::sync::Mutex::new(None::<kamu_resources::ResourceUID>));
    let uid_cell_clone = uid_cell.clone();

    mock_outbox
        .expect_post_message_as_json()
        .times(1)
        .withf(move |producer, message, _version| {
            let expected_uid = uid_cell_clone.lock().unwrap();
            let Some(expected_uid) = *expected_uid else {
                return false;
            };
            producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                && match serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                    .unwrap()
                {
                    ResourceLifecycleMessage::Deleted(m) => {
                        m.resources.len() == 1
                            && m.resources[0].uid == expected_uid
                            && m.resources[0].spec == serde_json::json!({ "value": "res-a" })
                    }
                    _ => false,
                }
        })
        .returning(|_, _, _| Ok(()));

    let harness = ResourceUseCaseBaseHarness::new_with_mock_outbox(mock_outbox);
    let account_id = make_account_id();

    let uid = harness.apply_and_get_uid(account_id.clone(), "res-a").await;
    *uid_cell.lock().unwrap() = Some(uid);

    harness.delete_account_resources(account_id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
