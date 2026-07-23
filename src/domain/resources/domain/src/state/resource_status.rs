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
    fn needs_reconciliation(&self, generation: u64) -> bool;
    fn ready_condition_status(&self) -> Option<ResourceConditionStatus>;
    fn mark_reconciling(&mut self, now: DateTime<Utc>);
    fn mark_ready(&mut self, now: DateTime<Utc>, observed_generation: u64);
    fn mark_failed(
        &mut self,
        now: DateTime<Utc>,
        observed_generation: u64,
        reason: impl Into<String>,
        message: impl Into<String>,
    );
    fn mark_pending_for_new_generation(&mut self);
}

impl ResourceStatusExt for ResourceStatus {
    fn needs_reconciliation(&self, generation: u64) -> bool {
        self.observed_generation
            .is_none_or(|observed_generation| observed_generation < generation)
    }

    fn ready_condition_status(&self) -> Option<ResourceConditionStatus> {
        ready_condition(self).map(|condition| condition.value)
    }

    fn mark_reconciling(&mut self, now: DateTime<Utc>) {
        self.phase = ResourcePhase::Reconciling;
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_true(now),
        );
    }

    fn mark_ready(&mut self, now: DateTime<Utc>, observed_generation: u64) {
        self.phase = ResourcePhase::Ready;
        self.observed_generation = Some(observed_generation);
        self.reconciled_at = Some(now);

        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::accepted_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::ready_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_false(now),
        );
    }

    fn mark_failed(
        &mut self,
        now: DateTime<Utc>,
        observed_generation: u64,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.phase = ResourcePhase::Failed;
        self.observed_generation = Some(observed_generation);
        self.reconciled_at = Some(now);

        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::accepted_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::ready_false(now, reason, message),
        );
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_false(now),
        );
    }

    fn mark_pending_for_new_generation(&mut self) {
        self.phase = ResourcePhase::Pending;
        self.conditions = empty_resource_conditions();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn ready_condition(status: &ResourceStatus) -> Option<ResourceConditionValue> {
    status
        .conditions
        .entries
        .get(&ready_condition_type_ref())
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
