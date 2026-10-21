// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, TimeZone, Utc};
use internal_error::InternalError;
use kamu_resources::{
    DeclarativeResource,
    EventSourcedResource,
    ManagedResource,
    ResourceCleanupError,
    ResourceEvent,
    ResourceEventDeleted,
    ResourceEventHeadersUpdated,
    ResourceHeadersInput,
    ResourceName,
    ResourcePhase,
    ResourceStatus,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

use crate::tests::utils::{
    TestResource,
    TestResourceLifecycleError,
    TestResourceReconcileError,
    TestResourceSpec,
    make_id,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Drives one in-memory [`TestResource`] through its lifecycle commands, with
/// a clock that moves one minute per command
pub struct ResourceLifecycleHarness {
    pub resource: TestResource,
    pub now: DateTime<Utc>,
}

impl ResourceLifecycleHarness {
    pub fn new() -> Self {
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
    pub fn reconciled() -> Self {
        let mut harness = Self::new();
        harness.mark_started();
        harness.mark_succeeded();
        harness
    }

    pub fn headers_input(env_label: Option<&str>) -> ResourceHeadersInput {
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

    pub fn spec(value: &str) -> TestResourceSpec {
        TestResourceSpec {
            value: value.to_string(),
        }
    }

    pub fn ready_status() -> ResourceStatus {
        Self::reconciled().resource.status().clone()
    }

    fn tick(&mut self) -> DateTime<Utc> {
        self.now += Duration::minutes(1);
        self.now
    }

    /// Applies headers and spec the way one apply does.
    pub fn apply(&mut self, env_label: Option<&str>, value: &str) {
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

    pub fn try_apply_headers(
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

    pub fn try_apply_spec(&mut self, value: &str) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        let new_generation = self.resource.headers().generation + 1;
        <TestResource as ManagedResource>::try_update_spec(
            &mut self.resource,
            now,
            Self::spec(value),
            new_generation,
        )
    }

    pub fn apply_spec(&mut self, value: &str) {
        self.try_apply_spec(value).unwrap();
    }

    /// Applies a headers update as stored before it carried a generation.
    pub fn apply_stored_headers_update_without_generation(&mut self, env_label: Option<&str>) {
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

    /// Applies a stored headers update directly, bypassing the command checks.
    pub fn try_apply_stored_headers_update(
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

    pub fn update_status(
        &mut self,
        expected_status_generation: u64,
        new_status: ResourceStatus,
    ) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        self.resource
            .try_update_status(now, expected_status_generation, new_status)
    }

    pub fn mark_started(&mut self) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_started(now, status_generation)
            .unwrap();
    }

    pub fn mark_succeeded(&mut self) {
        self.mark_succeeded_for_generation(self.resource.headers().generation);
    }

    pub fn mark_succeeded_for_generation(&mut self, generation: u64) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        self.resource
            .try_mark_reconciliation_succeeded(now, generation, status_generation)
            .unwrap();
    }

    pub fn mark_failed(&mut self) {
        let now = self.tick();
        let generation = self.resource.headers().generation;
        let status_generation = self.resource.headers().status_generation;
        let error = TestResourceReconcileError::Internal(InternalError::new("boom"));
        self.resource
            .try_mark_reconciliation_failed(now, generation, status_generation, &error)
            .unwrap();
    }

    pub fn request_deletion(&mut self) {
        let now = self.tick();
        self.resource.try_request_deletion(now).unwrap();
    }

    pub fn complete_deletion(&mut self) {
        self.try_complete_deletion().unwrap();
    }

    pub fn try_complete_deletion(&mut self) -> Result<(), TestResourceLifecycleError> {
        let now = self.tick();
        self.resource.try_complete_deletion(now)
    }

    /// Applies a deletion as stored before it was preceded by a request.
    pub fn apply_stored_deletion(&mut self) {
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

    pub fn mark_cleanup_failed(&mut self) {
        let now = self.tick();
        let status_generation = self.resource.headers().status_generation;
        let error = ResourceCleanupError::Internal(InternalError::new("boom"));
        self.resource
            .try_mark_cleanup_failed(now, status_generation, &error)
            .unwrap();
    }

    /// `(generation, status_generation)`
    pub fn generations(&self) -> (u64, u64) {
        let headers = self.resource.headers();
        (headers.generation, headers.status_generation)
    }

    pub fn phase(&self) -> ResourcePhase {
        self.resource.status().phase
    }

    pub fn status(&self) -> ResourceStatus {
        self.resource.status().clone()
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.resource.headers().updated_at
    }

    pub fn needs_reconciliation(&self) -> bool {
        self.resource.needs_reconciliation()
    }

    #[track_caller]
    pub fn assert_reconciliation_error(&self, expected_code: Option<&str>) {
        BaseResourceServiceHarness::assert_reconciliation_error(
            self.resource.status(),
            expected_code,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
