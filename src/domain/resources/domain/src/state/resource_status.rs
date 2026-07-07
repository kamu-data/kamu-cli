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
        conditions: None,
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
        ready_condition(self).map(|condition| condition.status)
    }

    fn mark_reconciling(&mut self, now: DateTime<Utc>) {
        self.phase = ResourcePhase::Reconciling;
        let conditions = self
            .conditions
            .get_or_insert_with(empty_resource_conditions);
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::reconciling_true(now),
        );
    }

    fn mark_ready(&mut self, now: DateTime<Utc>, observed_generation: u64) {
        self.phase = ResourcePhase::Ready;
        self.observed_generation = Some(observed_generation);
        self.reconciled_at = Some(now);

        let conditions = self
            .conditions
            .get_or_insert_with(empty_resource_conditions);
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::accepted_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::ready_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
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

        let conditions = self
            .conditions
            .get_or_insert_with(empty_resource_conditions);
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::accepted_true(now),
        );
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::ready_false(now, reason, message),
        );
        ResourceConditionValue::set_condition(
            &mut conditions.entries,
            ResourceConditionValue::reconciling_false(now),
        );
    }

    fn mark_pending_for_new_generation(&mut self) {
        self.phase = ResourcePhase::Pending;
        self.conditions = None;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusLike: Send + Sync {
    fn resource_status(&self) -> &ResourceStatus;
    fn resource_status_mut(&mut self) -> &mut ResourceStatus;
}

impl ResourceStatusLike for ResourceStatus {
    fn resource_status(&self) -> &ResourceStatus {
        self
    }

    fn resource_status_mut(&mut self) -> &mut ResourceStatus {
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusJson: ResourceStatusLike + Sized {
    fn status_from_json<TSpec>(
        value: serde_json::Value,
        spec: &TSpec,
    ) -> Result<Self, serde_json::Error>
    where
        Self: crate::PendingStatusFromSpec<TSpec>;

    fn status_to_json(&self) -> serde_json::Value;
}

impl ResourceStatusJson for ResourceStatus {
    fn status_from_json<TSpec>(
        value: serde_json::Value,
        _spec: &TSpec,
    ) -> Result<Self, serde_json::Error>
    where
        Self: crate::PendingStatusFromSpec<TSpec>,
    {
        let proxy: odf::metadata::serde::yaml::resource::ResourceStatus =
            serde_json::from_value(value)?;
        proxy.try_into().map_err(serde::de::Error::custom)
    }

    fn status_to_json(&self) -> serde_json::Value {
        resource_status_to_json(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn ready_condition(status: &ResourceStatus) -> Option<ResourceConditionValue> {
    status
        .conditions
        .as_ref()?
        .entries
        .get(&ready_condition_type_ref())
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
