// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_resources::{
    DeclarativeResource,
    ManagedResource,
    ResourceDeletionInProgressError,
    ResourcePhase,
};
use pretty_assertions::assert_eq;

use crate::tests::utils::{ResourceLifecycleHarness, TestResourceLifecycleError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_requested_deletion_keeps_name_and_shows_deleting() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.request_deletion();

    let headers = harness.resource.headers();
    assert_eq!(headers.name.as_str(), "res");
    assert_eq!(headers.deletion_requested_at, Some(harness.now));
    assert_eq!(headers.deleted_at, None);
    assert_eq!(headers.updated_at, harness.now);
    assert_eq!(
        harness.generations(),
        (1, 4),
        "a deletion request moves the phase, which is a status update"
    );
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
    assert!(harness.resource.needs_cleanup());
    assert!(!harness.resource.needs_reconciliation());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_repeated_deletion_request_is_not_an_update() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.request_deletion();
    let requested_at = harness.now;

    harness.request_deletion();

    assert_eq!(
        harness.resource.headers().deletion_requested_at,
        Some(requested_at)
    );
    assert_eq!(harness.resource.headers().updated_at, requested_at);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_deletion_takes_precedence_over_pending_generation() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.apply_spec("v2");
    assert_eq!(harness.phase(), ResourcePhase::Pending);

    harness.request_deletion();

    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_completed_deletion_renames_to_tombstone() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.request_deletion();
    let requested_at = harness.now;

    harness.complete_deletion();

    let headers = harness.resource.headers();
    assert_eq!(
        headers.name.as_str(),
        format!("deleted-{}", harness.resource.id())
    );
    assert_eq!(headers.deletion_requested_at, Some(requested_at));
    assert_eq!(headers.deleted_at, Some(harness.now));
    assert_eq!(harness.phase(), ResourcePhase::Deleted);
    assert!(!harness.resource.needs_cleanup());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_completing_unrequested_deletion_is_an_invariant_violation() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    assert_matches!(
        harness.try_complete_deletion(),
        Err(TestResourceLifecycleError::InvariantViolation(_))
    );

    let headers = harness.resource.headers();
    assert_eq!(headers.deletion_requested_at, None);
    assert_eq!(headers.deleted_at, None);
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_deletion_without_request_replays_as_deleted() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.apply_stored_deletion();

    let headers = harness.resource.headers();
    assert_eq!(
        headers.name.as_str(),
        format!("deleted-{}", harness.resource.id())
    );
    assert_eq!(headers.deletion_requested_at, Some(harness.now));
    assert_eq!(headers.deleted_at, Some(harness.now));
    assert_eq!(harness.phase(), ResourcePhase::Deleted);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_headers_and_spec_of_resource_being_deleted_cannot_change() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.request_deletion();
    let id = *harness.resource.id();

    assert_matches!(
        harness.try_apply_headers(Some("prod")),
        Err(TestResourceLifecycleError::DeletionInProgress(
            ResourceDeletionInProgressError { id: err_id }
        )) if err_id == id
    );
    assert_matches!(
        harness.try_apply_spec("v2"),
        Err(TestResourceLifecycleError::DeletionInProgress(_))
    );
    assert_eq!(harness.generations(), (1, 4));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_headers_update_after_deletion_request_is_an_invariant_violation() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.request_deletion();

    assert_matches!(
        harness.try_apply_stored_headers_update(Some("prod")),
        Err(TestResourceLifecycleError::InvariantViolation(_))
    );
    assert_eq!(harness.generations(), (1, 4));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_reconciliation_of_resource_being_deleted_is_skipped() {
    let mut harness = ResourceLifecycleHarness::new();
    harness.mark_started();
    harness.request_deletion();

    harness.mark_succeeded();
    harness.mark_started();

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_cleanup_is_recorded_while_deletion_stays_in_progress() {
    let mut harness = ResourceLifecycleHarness::reconciled();
    harness.request_deletion();

    harness.mark_cleanup_failed();

    assert_eq!(harness.generations(), (1, 5));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
    assert!(harness.resource.needs_cleanup());
    harness.assert_reconciliation_error(Some("cleanup-failed"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_cleanup_of_resource_not_being_deleted_is_skipped() {
    let mut harness = ResourceLifecycleHarness::reconciled();

    harness.mark_cleanup_failed();

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
