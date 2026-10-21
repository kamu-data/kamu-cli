// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::{DateTime, Duration, TimeZone, Utc};
use internal_error::InternalError;
use kamu_resources::{
    DeclarativeResource,
    EventSourcedResource,
    ManagedResource,
    ResourceCleanupError,
    ResourceDeletionInProgressError,
    ResourceEvent,
    ResourceEventDeleted,
    ResourceEventHeadersUpdated,
    ResourceHeadersInput,
    ResourceName,
    ResourcePhase,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;
use pretty_assertions::assert_eq;

use crate::tests::utils::{TestResource, TestResourceLifecycleError, TestResourceSpec, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_requested_deletion_keeps_name_and_shows_deleting() {
    let mut harness = ResourceDeletionHarness::reconciled();

    harness.request_deletion();

    let headers = harness.resource.headers();
    assert_eq!(headers.name.as_str(), "res");
    assert_eq!(headers.deletion_requested_at, Some(harness.now));
    assert_eq!(headers.deleted_at, None);
    assert_eq!(headers.updated_at, harness.now);
    assert_eq!(
        harness.generations(),
        (1, 3),
        "a deletion request is not a status update"
    );
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
    assert!(harness.resource.needs_cleanup());
    assert!(!harness.resource.needs_reconciliation());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_repeated_deletion_request_is_not_an_update() {
    let mut harness = ResourceDeletionHarness::reconciled();
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
    let mut harness = ResourceDeletionHarness::reconciled();
    harness.apply_spec("v2");
    assert_eq!(harness.phase(), ResourcePhase::Pending);

    harness.request_deletion();

    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_completed_deletion_renames_to_tombstone() {
    let mut harness = ResourceDeletionHarness::reconciled();
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
    let mut harness = ResourceDeletionHarness::reconciled();

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
    let mut harness = ResourceDeletionHarness::reconciled();

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
    let mut harness = ResourceDeletionHarness::reconciled();
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
    assert_eq!(harness.generations(), (1, 3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_headers_update_after_deletion_request_is_an_invariant_violation() {
    let mut harness = ResourceDeletionHarness::reconciled();
    harness.request_deletion();

    assert_matches!(
        harness.try_apply_stored_headers_update(Some("prod")),
        Err(TestResourceLifecycleError::InvariantViolation(_))
    );
    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_reconciliation_of_resource_being_deleted_is_skipped() {
    let mut harness = ResourceDeletionHarness::new();
    harness.mark_started();
    harness.request_deletion();

    harness.mark_succeeded();
    harness.mark_started();

    assert_eq!(harness.generations(), (1, 2));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_cleanup_is_recorded_while_deletion_stays_in_progress() {
    let mut harness = ResourceDeletionHarness::reconciled();
    harness.request_deletion();

    harness.mark_cleanup_failed();

    assert_eq!(harness.generations(), (1, 4));
    assert_eq!(harness.phase(), ResourcePhase::Deleting);
    assert!(harness.resource.needs_cleanup());
    BaseResourceServiceHarness::assert_reconciliation_error(
        harness.resource.status(),
        Some("cleanup-failed"),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_cleanup_of_resource_not_being_deleted_is_skipped() {
    let mut harness = ResourceDeletionHarness::reconciled();

    harness.mark_cleanup_failed();

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceDeletionHarness {
    resource: TestResource,
    now: DateTime<Utc>,
}

impl ResourceDeletionHarness {
    fn new() -> Self {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let resource = <TestResource as ManagedResource>::try_create(
            now,
            make_id(),
            Self::headers_input(None),
            Self::spec("v1"),
        )
        .unwrap();
        Self { resource, now }
    }

    /// A resource whose first generation was reconciled successfully.
    fn reconciled() -> Self {
        let mut harness = Self::new();
        harness.mark_started();
        harness.mark_succeeded();
        harness
    }

    fn headers_input(env_label: Option<&str>) -> ResourceHeadersInput {
        let mut headers = BaseResourceServiceHarness::make_headers_input(
            odf::ResourceOwnerHandle::new_test("test"),
            "res",
        );
        if let Some(env) = env_label {
            headers
                .labels
                .as_mut()
                .unwrap()
                .entries
                .insert("env".parse().unwrap(), serde_json::json!(env));
        }
        headers
    }

    fn spec(value: &str) -> TestResourceSpec {
        TestResourceSpec {
            value: value.to_string(),
        }
    }

    fn tick(&mut self) -> DateTime<Utc> {
        self.now += Duration::minutes(1);
        self.now
    }

    fn try_apply_headers(
        &mut self,
        env_label: Option<&str>,
    ) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        let new_generation = self.resource.headers().generation + 1;
        <TestResource as ManagedResource>::try_update_headers(
            &mut self.resource,
            now,
            Self::headers_input(env_label),
            new_generation,
        )
    }

    fn try_apply_spec(&mut self, value: &str) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        let new_generation = self.resource.headers().generation + 1;
        <TestResource as ManagedResource>::try_update_spec(
            &mut self.resource,
            now,
            Self::spec(value),
            new_generation,
        )
    }

    fn apply_spec(&mut self, value: &str) {
        self.try_apply_spec(value).unwrap();
    }

    fn request_deletion(&mut self) {
        let now = self.tick();
        self.resource.try_request_deletion(now).unwrap();
    }

    fn complete_deletion(&mut self) {
        self.try_complete_deletion().unwrap();
    }

    fn try_complete_deletion(&mut self) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        self.resource.try_complete_deletion(now)
    }

    /// Applies a deletion as stored before it was preceded by a request.
    fn apply_stored_deletion(&mut self) {
        let id = *self.resource.id();
        let event = ResourceEventDeleted {
            event_time: self.tick(),
            id,
            tombstone_name: ResourceName::new_unchecked(&format!("deleted-{id}")),
        };
        self.resource
            .apply_event(ResourceEvent::Deleted(event))
            .unwrap();
    }

    /// Applies a stored headers update directly, bypassing the command checks.
    fn try_apply_stored_headers_update(
        &mut self,
        env_label: Option<&str>,
    ) -> Result<(), TestResourceLifecycleError> {
        let event = ResourceEventHeadersUpdated {
            event_time: self.tick(),
            id: *self.resource.id(),
            new_headers: Self::headers_input(env_label),
            new_generation: Some(self.resource.headers().generation + 1),
        };
        self.resource
            .apply_event(ResourceEvent::HeadersUpdated(event))
    }

    fn mark_started(&mut self) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_started(now, status_generation)
            .unwrap();
    }

    fn mark_succeeded(&mut self) {
        let now = self.tick();
        let generation = self.resource.headers().generation;
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_succeeded(now, generation, status_generation)
            .unwrap();
    }

    fn mark_cleanup_failed(&mut self) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        let error = ResourceCleanupError::Internal(InternalError::new("boom"));
        self.resource
            .try_mark_cleanup_failed(now, status_generation, &error)
            .unwrap();
    }

    /// `(generation, status_generation)`
    fn generations(&self) -> (u64, u64) {
        let headers = self.resource.headers();
        (headers.generation, headers.status_generation)
    }

    fn phase(&self) -> ResourcePhase {
        self.resource.status().phase
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
