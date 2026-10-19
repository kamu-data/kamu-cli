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
    ResourceEvent,
    ResourceEventHeadersUpdated,
    ResourceHeadersInput,
    ResourcePhase,
    ResourceStatus,
    ResourceStatusConflictError,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;
use pretty_assertions::assert_eq;

use crate::tests::utils::{
    TestResource,
    TestResourceLifecycleError,
    TestResourceReconcileError,
    TestResourceSpec,
    make_id,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_new_resource_starts_both_generations_at_one() {
    let harness = ResourceGenerationsHarness::new();

    assert_eq!(harness.generations(), (1, 1));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_status_updates_bump_status_generation_and_updated_at() {
    let mut harness = ResourceGenerationsHarness::new();

    harness.mark_started();
    assert_eq!(harness.generations(), (1, 2));
    assert_eq!(harness.phase(), ResourcePhase::Reconciling);
    assert_eq!(harness.updated_at(), harness.now());

    harness.mark_succeeded();
    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
    assert_eq!(harness.updated_at(), harness.now());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_spec_update_writes_no_status() {
    let mut harness = ResourceGenerationsHarness::reconciled();
    let status_before = harness.status();

    harness.apply(None, "v2");

    assert_eq!(harness.generations(), (2, 3));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
    assert_eq!(
        harness.status().conditions,
        status_before.conditions,
        "conditions stay until the controller replaces them"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_headers_update_bumps_generation() {
    let mut harness = ResourceGenerationsHarness::reconciled();

    harness.apply(Some("prod"), "v1");

    assert_eq!(harness.generations(), (2, 3));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
    assert!(harness.needs_reconciliation());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_headers_and_spec_update_in_one_apply_bump_generation_once() {
    let mut harness = ResourceGenerationsHarness::reconciled();

    harness.apply(Some("prod"), "v2");

    assert_eq!(harness.generations(), (2, 3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unchanged_apply_bumps_nothing() {
    let mut harness = ResourceGenerationsHarness::reconciled();

    harness.apply(None, "v1");

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_headers_update_without_generation_keeps_it() {
    let mut harness = ResourceGenerationsHarness::reconciled();

    harness.apply_stored_headers_update(Some("prod"));

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.phase(), ResourcePhase::Ready);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stale_status_generation_is_a_conflict() {
    let mut harness = ResourceGenerationsHarness::new();
    harness.mark_started();
    let status_before = harness.status();

    let result = harness.update_status(1, ResourceGenerationsHarness::ready_status());

    assert_matches!(
        result,
        Err(TestResourceLifecycleError::StatusConflict(
            ResourceStatusConflictError {
                expected: 1,
                actual: 2
            }
        ))
    );
    assert_eq!(harness.status(), status_before);
    assert_eq!(harness.generations(), (1, 2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unchanged_status_is_not_an_update() {
    let mut harness = ResourceGenerationsHarness::reconciled();
    let updated_at_before = harness.updated_at();

    harness.update_status(3, harness.status()).unwrap();

    assert_eq!(harness.generations(), (1, 3));
    assert_eq!(harness.updated_at(), updated_at_before);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_failed_then_retried_then_degraded() {
    let mut harness = ResourceGenerationsHarness::new();

    harness.mark_started();
    harness.mark_failed();
    assert_eq!(harness.phase(), ResourcePhase::Failed);
    assert!(harness.needs_reconciliation());

    harness.mark_started();
    harness.mark_succeeded();
    assert_eq!(harness.phase(), ResourcePhase::Ready);
    assert_eq!(harness.generations(), (1, 5));

    harness.apply(None, "v2");
    harness.mark_started();
    harness.mark_failed();
    assert_eq!(harness.phase(), ResourcePhase::Degraded);
    assert_eq!(harness.generations(), (2, 7));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_outcome_for_superseded_generation_is_skipped() {
    let mut harness = ResourceGenerationsHarness::new();
    harness.mark_started();

    harness.apply(None, "v2");
    harness.mark_succeeded_for_generation(1);

    assert_eq!(harness.generations(), (2, 2));
    assert_eq!(harness.phase(), ResourcePhase::Pending);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceGenerationsHarness {
    resource: TestResource,
    now: DateTime<Utc>,
}

impl ResourceGenerationsHarness {
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

    fn ready_status() -> ResourceStatus {
        Self::reconciled().resource.status().clone()
    }

    fn tick(&mut self) -> DateTime<Utc> {
        self.now += Duration::minutes(1);
        self.now
    }

    /// Applies headers and spec the way one apply does.
    fn apply(&mut self, env_label: Option<&str>, value: &str) {
        let now = self.tick();
        let new_generation = self.resource.headers().generation + 1;
        <TestResource as ManagedResource>::try_update_headers(
            &mut self.resource,
            now,
            Self::headers_input(env_label),
            new_generation,
        )
        .unwrap();
        <TestResource as ManagedResource>::try_update_spec(
            &mut self.resource,
            now,
            Self::spec(value),
            new_generation,
        )
        .unwrap();
    }

    /// Applies a headers update as stored before it carried a generation.
    fn apply_stored_headers_update(&mut self, env_label: Option<&str>) {
        let event = ResourceEventHeadersUpdated {
            event_time: self.tick(),
            id: *self.resource.id(),
            new_headers: Self::headers_input(env_label),
            new_generation: Some(u64::MAX),
        };
        let mut payload = serde_json::to_value(event).unwrap();
        payload.as_object_mut().unwrap().remove("new_generation");

        let stored_event: ResourceEventHeadersUpdated = serde_json::from_value(payload).unwrap();
        assert_eq!(stored_event.new_generation, None);

        self.resource
            .apply_event(ResourceEvent::HeadersUpdated(stored_event))
            .unwrap();
    }

    fn mark_started(&mut self) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_started(now, status_generation)
            .unwrap();
    }

    fn mark_succeeded(&mut self) {
        self.mark_succeeded_for_generation(self.resource.headers().generation);
    }

    fn mark_succeeded_for_generation(&mut self, generation: u64) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_succeeded(now, generation, status_generation)
            .unwrap();
    }

    fn mark_failed(&mut self) {
        let now = self.tick();
        let generation = self.resource.headers().generation;
        let status_generation = self.resource.headers().status_generation;
        let error = TestResourceReconcileError::Internal(InternalError::new("boom"));
        self.resource
            .try_mark_reconciliation_failed(now, generation, status_generation, &error)
            .unwrap();
    }

    fn update_status(
        &mut self,
        expected_status_generation: u64,
        new_status: ResourceStatus,
    ) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        self.resource
            .try_update_status(now, expected_status_generation, new_status)
    }

    fn now(&self) -> DateTime<Utc> {
        self.now
    }

    /// `(generation, status_generation)`
    fn generations(&self) -> (u64, u64) {
        let headers = self.resource.headers();
        (headers.generation, headers.status_generation)
    }

    fn phase(&self) -> ResourcePhase {
        self.resource.status().phase
    }

    fn status(&self) -> ResourceStatus {
        self.resource.status().clone()
    }

    fn updated_at(&self) -> DateTime<Utc> {
        self.resource.headers().updated_at
    }

    fn needs_reconciliation(&self) -> bool {
        self.resource.needs_reconciliation()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
