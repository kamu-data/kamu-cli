// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use database_common::testing::{in_committed_transaction, in_rolled_back_transaction};
use dill::Catalog;
use internal_error::InternalError;
use kamu_resources::{
    ApplyResourceParams,
    DeclarativeResource,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ManagedResource,
    RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    ResourceAggregateLoader,
    ResourceBatchTransition,
    ResourceCleanupError,
    ResourceCleanupFailure,
    ResourceConditionPatchError,
    ResourceConditionUpdateError,
    ResourceConditionsPatch,
    ResourceDeletionCompletion,
    ResourceExpectedVersion,
    ResourceExtensionKind,
    ResourceExtensionResolutionError,
    ResourceExtensionValueError,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageConditionChange,
    ResourceLifecycleMessageStatusUpdated,
    ResourceLifecyclePrimitives,
    ResourceLifecycleTransitionError,
    ResourcePersistenceService,
    ResourcePhase,
    ResourceRawEventStore,
    ResourceStaleExpectationError,
    ResourceTransitionSkip,
    ResourceTransitionSkipReason,
    TypeRef,
    TypeUri,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::{OutboxMessageBoundary, OutboxMessageBridge, OutboxProvider};

use crate::tests::use_cases::{ResourceUseCaseBaseHarness, ResourceUseCaseBaseHarnessOpts};
use crate::tests::utils::{
    TEST_OTHER_TYPE_CONDITION_URI,
    TEST_PAYLOAD_CONDITION_URI,
    TEST_STAGE_CONDITION_URI,
    TestResource,
    TestResourceReconcileError,
    TestResourceSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_begin_reconciliation_marks_reconciling_and_posts_status() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let mark = harness.message_count().await;

    let version = harness.version(&id).await;
    let started = harness
        .commit(|p| async move {
            p.begin_reconciliation(&id, Some(version.status_generation))
                .await
        })
        .await
        .unwrap();

    assert!(started.is_some());
    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Reconciling);
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["StatusUpdated(Reconciling)"]
    );
}

#[test_log::test(tokio::test)]
async fn test_begin_reconciliation_of_reconciled_resource_changes_nothing() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_begin("res-a").await;
    let version = harness.version(&id).await;
    harness
        .commit(|p| async move { p.complete_reconciliation(&id, version).await })
        .await
        .unwrap();
    let ready = harness.version(&id).await;

    // A stale expectation does not matter when there is nothing to start
    let stale = ready.status_generation + 1;
    let started = harness
        .commit(|p| async move { p.begin_reconciliation(&id, Some(stale)).await })
        .await
        .unwrap();

    assert!(started.is_none());
    pretty_assertions::assert_eq!(harness.version(&id).await, ready);
    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Ready);
}

#[test_log::test(tokio::test)]
async fn test_begin_reconciliation_of_reconciling_resource_posts_nothing() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_begin("res-a").await;
    let reconciling = harness.version(&id).await;
    let mark = harness.message_count().await;

    let started = harness
        .commit(|p| async move {
            p.begin_reconciliation(&id, Some(reconciling.status_generation))
                .await
        })
        .await
        .unwrap();

    assert!(started.is_some());
    pretty_assertions::assert_eq!(harness.version(&id).await, reconciling);
    pretty_assertions::assert_eq!(harness.message_kinds_since(mark).await, [] as [&str; 0]);
}

#[test_log::test(tokio::test)]
async fn test_begin_reconciliation_rejects_stale_status_generation() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;

    let stale = harness.version(&id).await.status_generation + 1;
    let result = harness
        .commit(|p| async move { p.begin_reconciliation(&id, Some(stale)).await })
        .await;

    assert_matches!(
        result,
        Err(ResourceLifecycleTransitionError::Stale(
            ResourceStaleExpectationError::StatusGenerationChanged(_)
        ))
    );
    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Pending);
}

#[test_log::test(tokio::test)]
async fn test_complete_reconciliation_marks_ready_and_posts_status() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_begin("res-a").await;
    let mark = harness.message_count().await;

    let version = harness.version(&id).await;
    harness
        .commit(|p| async move { p.complete_reconciliation(&id, version).await })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Ready);
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["StatusUpdated(Ready)"]
    );
}

#[test_log::test(tokio::test)]
async fn test_fail_reconciliation_marks_failed_and_posts_status() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_begin("res-a").await;
    let mark = harness.message_count().await;

    let version = harness.version(&id).await;
    harness
        .commit(|p| async move {
            p.fail_reconciliation(&id, version, &reconcile_error())
                .await
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Failed);
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["StatusUpdated(Failed)"]
    );
    pretty_assertions::assert_eq!(
        harness.raw_messages_since(mark).await[0]["StatusUpdated"]["changed_conditions"],
        serde_json::json!({
            RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI: {
                "Set": { "value": { "code": "internal-error", "message": "Internal error: Internal error" } }
            },
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_reconciliation_outcome_rejects_stale_expectations() {
    for (outcome, staleness) in [
        (Outcome::Succeeded, Staleness::Generation),
        (Outcome::Succeeded, Staleness::StatusGeneration),
        (Outcome::Succeeded, Staleness::DeletionRequested),
        (Outcome::Failed, Staleness::Generation),
        (Outcome::Failed, Staleness::StatusGeneration),
        (Outcome::Failed, Staleness::DeletionRequested),
    ] {
        let harness = LifecyclePrimitivesHarness::new().await;
        let id = harness.apply_and_begin("res-a").await;
        let version = harness.version(&id).await;
        harness.make_stale(&id, staleness).await;
        let mark = harness.message_count().await;

        let result = harness
            .commit(|p| async move {
                match outcome {
                    Outcome::Succeeded => p.complete_reconciliation(&id, version).await,
                    Outcome::Failed => {
                        p.fail_reconciliation(&id, version, &reconcile_error())
                            .await
                    }
                }
            })
            .await;

        let stale = match result {
            Err(ResourceLifecycleTransitionError::Stale(stale)) => stale,
            other => panic!("{outcome:?}/{staleness:?}: expected a stale error, got {other:?}"),
        };
        match (staleness, stale) {
            (Staleness::Generation, ResourceStaleExpectationError::GenerationChanged(_))
            | (
                Staleness::StatusGeneration,
                ResourceStaleExpectationError::StatusGenerationChanged(_),
            )
            | (Staleness::DeletionRequested, ResourceStaleExpectationError::DeletionRequested(_)) =>
                {}
            (staleness, stale) => panic!("{outcome:?}/{staleness:?}: got {stale:?}"),
        }
        pretty_assertions::assert_eq!(
            harness.message_kinds_since(mark).await,
            [] as [&str; 0],
            "{outcome:?}/{staleness:?}"
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_request_deletion_posts_one_message_for_the_batch() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let ids = [harness.apply("res-a").await, harness.apply("res-b").await];
    let mark = harness.message_count().await;

    harness
        .commit(|p| async move { p.request_deletion(&ids).await })
        .await
        .unwrap();

    for id in &ids {
        pretty_assertions::assert_eq!(harness.phase(id).await, ResourcePhase::Deleting);
    }
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["DeletionRequested(2)"]
    );
}

#[test_log::test(tokio::test)]
async fn test_complete_deletion_skips_resources_not_awaiting_cleanup() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let deleting = harness.apply_and_request_deletion("res-a").await;
    let live = harness.apply("res-b").await;
    let mark = harness.message_count().await;

    let transition = harness
        .commit(|p| async move {
            p.complete_deletion(&[completion(deleting, None), completion(live, None)])
                .await
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(transitioned_ids(&transition), [deleting]);
    pretty_assertions::assert_eq!(
        transition.skipped,
        [ResourceTransitionSkip {
            id: live,
            reason: ResourceTransitionSkipReason::NotAwaitingCleanup,
        }]
    );
    pretty_assertions::assert_eq!(harness.message_kinds_since(mark).await, ["Deleted(1)"]);
}

#[test_log::test(tokio::test)]
async fn test_complete_deletion_with_stale_status_generation_is_skipped() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_request_deletion("res-a").await;
    let stale = harness.version(&id).await.status_generation + 1;
    let mark = harness.message_count().await;

    let transition = harness
        .commit(|p| async move { p.complete_deletion(&[completion(id, Some(stale))]).await })
        .await
        .unwrap();

    assert_matches!(
        transition.skipped.as_slice(),
        [ResourceTransitionSkip {
            reason: ResourceTransitionSkipReason::StatusConflict(_),
            ..
        }]
    );
    pretty_assertions::assert_eq!(harness.phase(&id).await, ResourcePhase::Deleting);
    pretty_assertions::assert_eq!(harness.message_kinds_since(mark).await, [] as [&str; 0]);
}

#[test_log::test(tokio::test)]
async fn test_record_cleanup_failure_keeps_deletion_in_progress() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_request_deletion("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    let mark = harness.message_count().await;

    let transition = harness
        .commit(|p| async move {
            p.record_cleanup_failure(&[cleanup_failure(id, status_generation)])
                .await
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(transitioned_ids(&transition), [id]);
    harness.assert_cleanup_failed(&id).await;
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["StatusUpdated(Deleting)"]
    );
}

#[test_log::test(tokio::test)]
async fn test_record_cleanup_failure_with_stale_status_generation_is_skipped() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_request_deletion("res-a").await;
    let stale = harness.version(&id).await.status_generation + 1;

    let transition = harness
        .commit(|p| async move {
            p.record_cleanup_failure(&[cleanup_failure(id, stale)])
                .await
        })
        .await
        .unwrap();

    assert_matches!(
        transition.skipped.as_slice(),
        [ResourceTransitionSkip {
            reason: ResourceTransitionSkipReason::StatusConflict(_),
            ..
        }]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_writers_patching_different_conditions_keep_both() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let read_by_both = harness.version(&id).await.status_generation;

    let first = harness
        .update_conditions(id, read_by_both, stage_patch("running"))
        .await
        .unwrap();
    let after_first = first.headers().status_generation;
    harness
        .update_conditions(
            id,
            after_first,
            payload_patch(serde_json::json!({ "step": 1 })),
        )
        .await
        .unwrap();

    pretty_assertions::assert_eq!(after_first, read_by_both + 1);
    pretty_assertions::assert_eq!(
        harness.conditions(&id).await,
        [
            (
                condition_key(TEST_STAGE_CONDITION_URI),
                serde_json::json!("running")
            ),
            (
                condition_key(TEST_PAYLOAD_CONDITION_URI),
                serde_json::json!({ "step": 1 })
            ),
        ]
        .into()
    );
}

#[test_log::test(tokio::test)]
async fn test_writer_with_stale_status_generation_changes_nothing() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let read_by_both = harness.version(&id).await.status_generation;
    harness
        .update_conditions(id, read_by_both, stage_patch("running"))
        .await
        .unwrap();
    let version = harness.version(&id).await;
    let mark = harness.message_count().await;

    let result = harness
        .update_conditions(
            id,
            read_by_both,
            payload_patch(serde_json::json!({ "step": 1 })),
        )
        .await;

    assert_matches!(
        result,
        Err(ResourceConditionUpdateError::StatusGenerationChanged(_))
    );
    pretty_assertions::assert_eq!(harness.version(&id).await, version);
    pretty_assertions::assert_eq!(harness.message_kinds_since(mark).await, [] as [&str; 0]);
}

#[test_log::test(tokio::test)]
async fn test_invalid_condition_patch_changes_nothing() {
    let oversized = "x".repeat(70);
    let cases = [
        (
            stage_patch_value(serde_json::json!(5)),
            ResourceConditionPatchError::InvalidValue {
                key: uri(TEST_STAGE_CONDITION_URI),
                error: ResourceExtensionValueError::new("stage must be a string"),
            },
        ),
        (
            stage_patch(&oversized),
            ResourceConditionPatchError::TooLarge {
                key: uri(TEST_STAGE_CONDITION_URI),
                size: oversized.len() + 2,
                max: 64,
            },
        ),
        (
            set_patch("https://test.kamu.dev/schemas/test/v1/conditions/Unknown"),
            ResourceConditionPatchError::Resolution(ResourceExtensionResolutionError::UnknownUri {
                kind: ResourceExtensionKind::Condition,
                uri: uri("https://test.kamu.dev/schemas/test/v1/conditions/Unknown"),
            }),
        ),
        (
            set_patch(TEST_OTHER_TYPE_CONDITION_URI),
            ResourceConditionPatchError::Resolution(
                ResourceExtensionResolutionError::Inapplicable {
                    uri: uri(TEST_OTHER_TYPE_CONDITION_URI),
                    resource_schema: uri(TestResource::SCHEMA_STR),
                },
            ),
        ),
        (
            set_patch(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
            ResourceConditionPatchError::Resolution(
                ResourceExtensionResolutionError::KindMismatch {
                    uri: uri(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
                    expected_kind: ResourceExtensionKind::Condition,
                    actual_kind: ResourceExtensionKind::Label,
                },
            ),
        ),
        (
            ResourceConditionsPatch::new()
                .remove(uri(RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI)),
            ResourceConditionPatchError::FrameworkOwned {
                key: uri(RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI),
            },
        ),
    ];

    for (patch, expected) in cases {
        let harness = LifecyclePrimitivesHarness::new().await;
        let id = harness.apply("res-a").await;
        let version = harness.version(&id).await;
        let events = harness.event_count().await;
        let messages = harness.message_count().await;

        let result = harness
            .update_conditions(id, version.status_generation, patch)
            .await;

        let Err(ResourceConditionUpdateError::InvalidPatch(error)) = result else {
            panic!("expected {expected:?}, got {result:?}");
        };
        pretty_assertions::assert_eq!(error, expected);
        pretty_assertions::assert_eq!(harness.version(&id).await, version, "{expected:?}");
        pretty_assertions::assert_eq!(harness.event_count().await, events, "{expected:?}");
        pretty_assertions::assert_eq!(harness.message_count().await, messages, "{expected:?}");
    }
}

#[test_log::test(tokio::test)]
async fn test_change_rejected_by_status_rules_changes_nothing() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    harness
        .update_conditions(
            id,
            status_generation,
            stage_patch(TestResource::FINAL_STAGE),
        )
        .await
        .unwrap();
    let version = harness.version(&id).await;
    let events = harness.event_count().await;
    let messages = harness.message_count().await;

    for patch in [
        stage_patch("running"),
        ResourceConditionsPatch::new().remove(uri(TEST_STAGE_CONDITION_URI)),
    ] {
        let result = harness
            .update_conditions(id, version.status_generation, patch)
            .await;

        assert_matches!(result, Err(ResourceConditionUpdateError::Rejected(_)));
    }
    pretty_assertions::assert_eq!(harness.version(&id).await, version);
    pretty_assertions::assert_eq!(harness.event_count().await, events);
    pretty_assertions::assert_eq!(harness.message_count().await, messages);
}

#[test_log::test(tokio::test)]
async fn test_status_updated_message_carries_values_of_carried_conditions_only() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    let mark = harness.message_count().await;

    let updated = harness
        .update_conditions(
            id,
            status_generation,
            stage_patch("running").set(
                uri(TEST_PAYLOAD_CONDITION_URI),
                serde_json::json!({ "step": 1 }),
            ),
        )
        .await
        .unwrap();

    let [message] = harness.status_updates_since(mark).await.try_into().unwrap();
    pretty_assertions::assert_eq!(message.resource.id, id);
    pretty_assertions::assert_eq!(message.resource.schema, uri(TestResource::SCHEMA_STR));
    pretty_assertions::assert_eq!(message.resource.headers, *updated.headers());
    pretty_assertions::assert_eq!(
        harness.raw_messages_since(mark).await[0]["StatusUpdated"]["changed_conditions"],
        serde_json::json!({
            TEST_PAYLOAD_CONDITION_URI: { "Set": { "value": null } },
            TEST_STAGE_CONDITION_URI: { "Set": { "value": "running" } },
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_status_updated_message_names_only_conditions_that_changed() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    let staged = harness
        .update_conditions(
            id,
            status_generation,
            stage_patch("running").set(
                uri(TEST_PAYLOAD_CONDITION_URI),
                serde_json::json!({ "step": 1 }),
            ),
        )
        .await
        .unwrap();
    let mark = harness.message_count().await;

    harness
        .update_conditions(
            id,
            staged.headers().status_generation,
            stage_patch("running").remove(uri(TEST_PAYLOAD_CONDITION_URI)),
        )
        .await
        .unwrap();

    let [message] = harness.status_updates_since(mark).await.try_into().unwrap();
    pretty_assertions::assert_eq!(
        message.changed_conditions,
        [(
            uri(TEST_PAYLOAD_CONDITION_URI),
            ResourceLifecycleMessageConditionChange::Removed
        )]
        .into()
    );
}

#[test_log::test(tokio::test)]
async fn test_patch_that_changes_nothing_records_nothing() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    harness
        .update_conditions(id, status_generation, stage_patch("running"))
        .await
        .unwrap();
    let version = harness.version(&id).await;
    let events = harness.event_count().await;
    let messages = harness.message_count().await;

    harness
        .update_conditions(
            id,
            version.status_generation,
            stage_patch("running").remove(uri(TEST_PAYLOAD_CONDITION_URI)),
        )
        .await
        .unwrap();

    pretty_assertions::assert_eq!(harness.version(&id).await, version);
    pretty_assertions::assert_eq!(harness.event_count().await, events);
    pretty_assertions::assert_eq!(harness.message_count().await, messages);
}

#[test_log::test(tokio::test)]
async fn test_conditions_are_patched_while_deleting_but_not_once_deleted() {
    let harness = LifecyclePrimitivesHarness::new().await;
    let id = harness.apply_and_request_deletion("res-a").await;
    let status_generation = harness.version(&id).await.status_generation;
    let mark = harness.message_count().await;

    let deleting = harness
        .update_conditions(
            id,
            status_generation,
            stage_patch(TestResource::FINAL_STAGE),
        )
        .await
        .unwrap();
    pretty_assertions::assert_eq!(deleting.status().phase, ResourcePhase::Deleting);
    pretty_assertions::assert_eq!(
        harness.message_kinds_since(mark).await,
        ["StatusUpdated(Deleting)"]
    );

    harness
        .commit(|p| async move { p.complete_deletion(&[completion(id, None)]).await })
        .await
        .unwrap();
    let deleted = harness.version(&id).await;

    let result = harness
        .update_conditions(
            id,
            deleted.status_generation,
            payload_patch(serde_json::json!({ "step": 1 })),
        )
        .await;

    assert_matches!(result, Err(ResourceConditionUpdateError::Lifecycle(_)));
    pretty_assertions::assert_eq!(harness.version(&id).await, deleted);
}

#[test_log::test(tokio::test)]
async fn test_rolled_back_transition_leaves_no_event_and_no_message() {
    for transition in [
        Transition::BeginReconciliation,
        Transition::CompleteReconciliation,
        Transition::FailReconciliation,
        Transition::RequestDeletion,
        Transition::CompleteDeletion,
        Transition::RecordCleanupFailure,
        Transition::UpdateConditions,
    ] {
        let harness = LifecyclePrimitivesHarness::new().await;
        let id = harness.prepare_for(transition).await;
        let version = harness.version(&id).await;
        let events = harness.event_count().await;
        let messages = harness.message_count().await;

        harness
            .roll_back(|p| async move { transition.apply(p.as_ref(), id, version).await })
            .await;

        pretty_assertions::assert_eq!(harness.version(&id).await, version, "{transition:?}");
        pretty_assertions::assert_eq!(harness.event_count().await, events, "{transition:?}");
        pretty_assertions::assert_eq!(harness.message_count().await, messages, "{transition:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy)]
enum Staleness {
    Generation,
    StatusGeneration,
    DeletionRequested,
}

#[derive(Debug, Clone, Copy)]
enum Transition {
    BeginReconciliation,
    CompleteReconciliation,
    FailReconciliation,
    RequestDeletion,
    CompleteDeletion,
    RecordCleanupFailure,
    UpdateConditions,
}

impl Transition {
    /// Applies the transition and expects it to succeed
    async fn apply(
        self,
        primitives: &dyn ResourceLifecyclePrimitives<TestResource>,
        id: ResourceID,
        version: ResourceExpectedVersion,
    ) {
        match self {
            Self::BeginReconciliation => {
                primitives
                    .begin_reconciliation(&id, Some(version.status_generation))
                    .await
                    .unwrap();
            }
            Self::CompleteReconciliation => {
                primitives
                    .complete_reconciliation(&id, version)
                    .await
                    .unwrap();
            }
            Self::FailReconciliation => {
                primitives
                    .fail_reconciliation(&id, version, &reconcile_error())
                    .await
                    .unwrap();
            }
            Self::RequestDeletion => {
                primitives.request_deletion(&[id]).await.unwrap();
            }
            Self::CompleteDeletion => {
                let transition = primitives
                    .complete_deletion(&[completion(id, None)])
                    .await
                    .unwrap();
                pretty_assertions::assert_eq!(transitioned_ids(&transition), [id]);
            }
            Self::RecordCleanupFailure => {
                let transition = primitives
                    .record_cleanup_failure(&[cleanup_failure(id, version.status_generation)])
                    .await
                    .unwrap();
                pretty_assertions::assert_eq!(transitioned_ids(&transition), [id]);
            }
            Self::UpdateConditions => {
                primitives
                    .update_conditions(&id, version.status_generation, &stage_patch("running"))
                    .await
                    .unwrap();
            }
        }
    }
}

fn reconcile_error() -> TestResourceReconcileError {
    TestResourceReconcileError::Internal(InternalError::new("reconcile failed"))
}

fn completion(
    id: ResourceID,
    expected_status_generation: Option<u64>,
) -> ResourceDeletionCompletion {
    ResourceDeletionCompletion {
        id,
        expected_status_generation,
    }
}

fn cleanup_failure(id: ResourceID, expected_status_generation: u64) -> ResourceCleanupFailure {
    ResourceCleanupFailure {
        id,
        expected_status_generation,
        error: Arc::new(ResourceCleanupError::Internal(InternalError::new(
            "cleanup failed",
        ))),
    }
}

fn uri(uri: &str) -> TypeUri {
    TypeUri::new_unchecked(uri)
}

fn condition_key(uri: &str) -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(uri))
}

fn stage_patch(stage: &str) -> ResourceConditionsPatch {
    stage_patch_value(serde_json::json!(stage))
}

fn stage_patch_value(value: serde_json::Value) -> ResourceConditionsPatch {
    ResourceConditionsPatch::new().set(uri(TEST_STAGE_CONDITION_URI), value)
}

fn payload_patch(payload: serde_json::Value) -> ResourceConditionsPatch {
    ResourceConditionsPatch::new().set(uri(TEST_PAYLOAD_CONDITION_URI), payload)
}

/// Sets the condition `key` to a valid stage
fn set_patch(key: &str) -> ResourceConditionsPatch {
    ResourceConditionsPatch::new().set(uri(key), serde_json::json!("running"))
}

fn transitioned_ids(transition: &ResourceBatchTransition<TestResource>) -> Vec<ResourceID> {
    transition
        .transitioned
        .iter()
        .map(|resource| *resource.id())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ResourceUseCaseBaseHarness, base)]
struct LifecyclePrimitivesHarness {
    base: ResourceUseCaseBaseHarness,
    account: odf::ResourceOwnerHandle,
}

impl LifecyclePrimitivesHarness {
    async fn new() -> Self {
        let base = ResourceUseCaseBaseHarness::new_with_opts(ResourceUseCaseBaseHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
                transactional: true,
            },
            ..Default::default()
        });
        let account = base.ensure_test_account("test-owner").await;

        Self { base, account }
    }

    /// Runs `f` with the primitives of a transaction that commits
    async fn commit<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce(Arc<dyn ResourceLifecyclePrimitives<TestResource>>) -> Fut,
        Fut: Future<Output = T>,
    {
        in_committed_transaction(self.catalog(), |catalog| async move {
            f(Self::primitives_in(&catalog)).await
        })
        .await
    }

    /// Runs `f` with the primitives of a transaction that rolls back
    async fn roll_back<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<dyn ResourceLifecyclePrimitives<TestResource>>) -> Fut,
        Fut: Future<Output = ()>,
    {
        in_rolled_back_transaction(self.catalog(), |catalog| async move {
            f(Self::primitives_in(&catalog)).await;
        })
        .await;
    }

    fn primitives_in(catalog: &Catalog) -> Arc<dyn ResourceLifecyclePrimitives<TestResource>> {
        catalog.get_one().unwrap()
    }

    async fn apply(&self, name: &str) -> ResourceID {
        self.apply_and_get_id(&self.account, name).await
    }

    async fn apply_and_begin(&self, name: &str) -> ResourceID {
        let id = self.apply(name).await;
        let status_generation = self.version(&id).await.status_generation;
        self.commit(|p| async move { p.begin_reconciliation(&id, Some(status_generation)).await })
            .await
            .unwrap();
        id
    }

    async fn apply_and_request_deletion(&self, name: &str) -> ResourceID {
        let id = self.apply(name).await;
        self.commit(|p| async move { p.request_deletion(&[id]).await })
            .await
            .unwrap();
        id
    }

    /// Brings the resource into the state the transition applies to
    async fn prepare_for(&self, transition: Transition) -> ResourceID {
        match transition {
            Transition::BeginReconciliation
            | Transition::RequestDeletion
            | Transition::UpdateConditions => self.apply("res-a").await,
            Transition::CompleteReconciliation | Transition::FailReconciliation => {
                self.apply_and_begin("res-a").await
            }
            Transition::CompleteDeletion | Transition::RecordCleanupFailure => {
                self.apply_and_request_deletion("res-a").await
            }
        }
    }

    /// Changes the resource after a reconciliation began, so its outcome no
    /// longer applies
    async fn make_stale(&self, id: &ResourceID, staleness: Staleness) {
        match staleness {
            Staleness::Generation => {
                let snapshot = self.get_snapshot_by_id(id).await.unwrap();
                self.apply_test_uc()
                    .apply(ApplyResourceParams {
                        id: Some(*id),
                        headers: BaseResourceServiceHarness::make_headers_input(
                            self.account.clone(),
                            snapshot.headers.name.as_str(),
                        ),
                        spec: TestResourceSpec {
                            value: "updated".to_string(),
                        },
                    })
                    .await
                    .unwrap()
                    .expect_applied();
            }
            Staleness::StatusGeneration => self.update_status_concurrently(id).await,
            Staleness::DeletionRequested => {
                let id = *id;
                self.commit(|p| async move { p.request_deletion(&[id]).await })
                    .await
                    .unwrap();
            }
        }
    }

    /// Another status writer updates the status of the resource
    async fn update_status_concurrently(&self, id: &ResourceID) {
        let mut resource = self.load(id).await;
        let mut new_status = resource.status().clone();
        new_status.observed_at = Some(self.time_source().now() + chrono::Duration::seconds(1));
        let status_generation = resource.headers().status_generation;
        resource
            .try_update_status(self.time_source().now(), status_generation, new_status)
            .unwrap();
        self.catalog()
            .get_one::<dyn ResourcePersistenceService<TestResource>>()
            .unwrap()
            .save(&mut resource)
            .await
            .unwrap();
    }

    async fn update_conditions(
        &self,
        id: ResourceID,
        expected_status_generation: u64,
        patch: ResourceConditionsPatch,
    ) -> Result<TestResource, ResourceConditionUpdateError<TestResource>> {
        self.commit(|p| async move {
            p.update_conditions(&id, expected_status_generation, &patch)
                .await
        })
        .await
    }

    async fn conditions(&self, id: &ResourceID) -> BTreeMap<TypeRef, serde_json::Value> {
        self.load(id).await.status().conditions.entries.clone()
    }

    async fn version(&self, id: &ResourceID) -> ResourceExpectedVersion {
        let resource = self.load(id).await;
        ResourceExpectedVersion {
            generation: resource.headers().generation,
            status_generation: resource.headers().status_generation,
        }
    }

    async fn phase(&self, id: &ResourceID) -> ResourcePhase {
        self.load(id).await.status().phase
    }

    async fn assert_cleanup_failed(&self, id: &ResourceID) {
        let resource = self.load(id).await;
        pretty_assertions::assert_eq!(resource.status().phase, ResourcePhase::Deleting);
        BaseResourceServiceHarness::assert_reconciliation_error(
            resource.status(),
            Some("cleanup-failed"),
        );
    }

    async fn load(&self, id: &ResourceID) -> TestResource {
        self.catalog()
            .get_one::<dyn ResourceAggregateLoader<TestResource>>()
            .unwrap()
            .load(id)
            .await
            .unwrap()
    }

    async fn event_count(&self) -> usize {
        self.catalog()
            .get_one::<dyn ResourceRawEventStore>()
            .unwrap()
            .total_events_stored()
            .await
            .unwrap()
    }

    async fn message_count(&self) -> usize {
        self.lifecycle_messages().await.len()
    }

    /// The kind of every lifecycle message posted after the first `mark`, with
    /// the resource count of batch messages
    async fn message_kinds_since(&self, mark: usize) -> Vec<String> {
        self.lifecycle_messages()
            .await
            .into_iter()
            .skip(mark)
            .map(|message| match message {
                ResourceLifecycleMessage::Applied(_) => "Applied".to_string(),
                ResourceLifecycleMessage::DeletionRequested(message) => {
                    format!("DeletionRequested({})", message.resources.len())
                }
                ResourceLifecycleMessage::Deleted(message) => {
                    format!("Deleted({})", message.resources.len())
                }
                ResourceLifecycleMessage::StatusUpdated(message) => {
                    format!("StatusUpdated({:?})", message.status.phase)
                }
            })
            .collect()
    }

    async fn status_updates_since(
        &self,
        mark: usize,
    ) -> Vec<ResourceLifecycleMessageStatusUpdated> {
        self.lifecycle_messages()
            .await
            .into_iter()
            .skip(mark)
            .filter_map(|message| match message {
                ResourceLifecycleMessage::StatusUpdated(message) => Some(message),
                ResourceLifecycleMessage::Applied(_)
                | ResourceLifecycleMessage::DeletionRequested(_)
                | ResourceLifecycleMessage::Deleted(_) => None,
            })
            .collect()
    }

    async fn lifecycle_messages(&self) -> Vec<ResourceLifecycleMessage> {
        self.raw_messages_since(0)
            .await
            .into_iter()
            .map(|content| serde_json::from_value(content).unwrap())
            .collect()
    }

    /// The JSON of every lifecycle message posted after the first `mark`, as
    /// the outbox stores it
    async fn raw_messages_since(&self, mark: usize) -> Vec<serde_json::Value> {
        self.catalog()
            .get_one::<dyn OutboxMessageBridge>()
            .unwrap()
            .get_messages_by_producer(
                self.catalog(),
                MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                OutboxMessageBoundary::default(),
                usize::MAX,
            )
            .await
            .unwrap()
            .into_iter()
            .skip(mark)
            .map(|message| message.content_json)
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
