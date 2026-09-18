// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    ResourceConditionStatus,
    ResourceConditionValue,
    ResourcePhase,
    empty_resource_conditions,
    ready_condition_type_ref,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceStatus = odf::metadata::resource::ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn new_pending_resource_status() -> ResourceStatus {
    ResourceStatus {
        phase: ResourcePhase::Pending,
        observed_generation: None,
        observed_at: None,
        reconciled_generation: None,
        reconciled_at: None,
        conditions: empty_resource_conditions(),
    }
}

pub fn resource_status_from_json(value: &serde_json::Value) -> Option<ResourceStatus> {
    let proxy: odf::metadata::serde::yaml::resource::ResourceStatus =
        serde_json::from_value(value.clone()).ok()?;
    proxy.try_into().ok()
}

pub fn resource_status_to_json(status: &ResourceStatus) -> serde_json::Value {
    let proxy: odf::metadata::serde::yaml::resource::ResourceStatus = status.clone().into();
    serde_json::to_value(proxy).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusExt {
    fn was_observed(&self, generation: u64) -> bool;

    fn was_reconciled(&self, generation: u64) -> bool;

    /// Acknowledges the new spec generation before starting to reconcile
    fn mark_pending(&mut self);

    /// Updates observed generation and indicates that reconcilation had started
    fn mark_reconciling(&mut self, observed_generation: u64, now: DateTime<Utc>);

    /// Updated reconciled generation
    fn mark_ready(&mut self, reconciled_generation: u64, now: DateTime<Utc>);

    /// Indicates failed reconciliation attempt
    fn mark_failed_or_degraded(
        &mut self,
        reason: impl Into<String>,
        message: impl Into<String>,
        now: DateTime<Utc>,
    );

    // TODO: Remove?
    fn ready_condition_status(&self) -> Option<ResourceConditionStatus>;
}

impl ResourceStatusExt for ResourceStatus {
    fn was_observed(&self, generation: u64) -> bool {
        self.observed_generation
            .is_some_and(|observed_generation| observed_generation >= generation)
    }

    fn was_reconciled(&self, generation: u64) -> bool {
        self.reconciled_generation
            .is_some_and(|reconciled_generation| reconciled_generation >= generation)
    }

    fn mark_pending(&mut self) {
        self.phase = ResourcePhase::Pending;

        // TODO: Should only clear main controller's conditions, not all of them?
        self.conditions = empty_resource_conditions();
    }

    fn mark_reconciling(&mut self, observed_generation: u64, now: DateTime<Utc>) {
        assert!(
            self.observed_generation.is_none()
                || self.observed_generation.unwrap() < observed_generation,
            "Previous observed generation {:?} >= {observed_generation}",
            self.reconciled_generation,
        );
        self.phase = ResourcePhase::Reconciling;
        self.observed_generation = Some(observed_generation);
        self.observed_at = Some(now);

        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_true(now),
        );
    }

    fn mark_ready(&mut self, reconciled_generation: u64, now: DateTime<Utc>) {
        assert!(
            self.observed_generation.is_some()
                && self.observed_generation.unwrap() >= reconciled_generation,
            "Observed generation {:?} < {reconciled_generation}",
            self.observed_generation,
        );
        assert!(
            self.reconciled_generation.is_none()
                || self.reconciled_generation.unwrap() < reconciled_generation,
            "Previous reconciled generation {:?} >= {reconciled_generation}",
            self.reconciled_generation,
        );

        self.phase = ResourcePhase::Ready;
        self.reconciled_generation = Some(reconciled_generation);
        self.reconciled_at = Some(now);

        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::ready_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_false(now),
        );
    }

    fn mark_failed_or_degraded(
        &mut self,
        reason: impl Into<String>,
        message: impl Into<String>,
        now: DateTime<Utc>,
    ) {
        self.phase = if self.reconciled_generation.is_none() {
            ResourcePhase::Failed
        } else {
            ResourcePhase::Degraded
        };

        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::ready_false(now, reason, message),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_false(now),
        );
    }

    fn ready_condition_status(&self) -> Option<ResourceConditionStatus> {
        ready_condition(self).map(|condition| condition.value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn ready_condition(status: &ResourceStatus) -> Option<ResourceConditionValue> {
    let condition_key = ready_condition_type_ref();

    let value = status.conditions.entries.get(&condition_key)?;

    match serde_json::from_value(value.clone()) {
        Ok(value) => Some(value),
        Err(error) => {
            tracing::warn!(
                %condition_key,
                %value,
                %error,
                "Failed to parse condition value - ignoring",
            );
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
