// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    ResourceConditionStatus,
    ResourceConditionValue,
    ResourceConditions,
    ResourcePhase,
    empty_resource_conditions,
    ready_condition_type_ref,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceStatus {
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourcePhase")]
    pub phase: ResourcePhase,
    pub observed_generation: u64,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceConditions")]
    #[serde(default = "empty_resource_conditions")]
    pub conditions: ResourceConditions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceStatus {
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let serde_json::Value::Object(status_map) = value else {
            return None;
        };

        let observed_generation = status_map
            .get("observedGeneration")
            .or_else(|| status_map.get("observed_generation"))?
            .clone();

        let mut status_json = serde_json::Map::with_capacity(3);
        status_json.insert("phase".to_string(), status_map.get("phase")?.clone());
        status_json.insert("observedGeneration".to_string(), observed_generation);
        if let Some(conditions) = status_map.get("conditions") {
            status_json.insert("conditions".to_string(), conditions.clone());
        }

        serde_json::from_value(serde_json::Value::Object(status_json)).ok()
    }

    pub fn new_pending() -> Self {
        Self {
            phase: ResourcePhase::Pending,
            observed_generation: 0,
            conditions: empty_resource_conditions(),
        }
    }

    pub fn needs_reconciliation(&self, generation: u64) -> bool {
        self.observed_generation < generation
    }

    pub fn last_reconciled_at(&self) -> Option<DateTime<Utc>> {
        self.ready_condition()
            .map(|condition| condition.last_transition_time)
    }

    pub fn ready_condition_status(&self) -> Option<ResourceConditionStatus> {
        self.ready_condition().map(|condition| condition.status)
    }

    pub fn mark_reconciling(&mut self, now: DateTime<Utc>) {
        self.phase = ResourcePhase::Reconciling;
        ResourceConditionValue::set_condition(
            &mut self.conditions.entries,
            ResourceConditionValue::reconciling_true(now),
        );
    }

    pub fn mark_ready(&mut self, now: DateTime<Utc>, observed_generation: u64) {
        self.phase = ResourcePhase::Ready;
        self.observed_generation = observed_generation;

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

    pub fn mark_failed(
        &mut self,
        now: DateTime<Utc>,
        observed_generation: u64,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.phase = ResourcePhase::Failed;
        self.observed_generation = observed_generation;

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

    pub fn mark_pending_for_new_generation(&mut self) {
        self.phase = ResourcePhase::Pending;
        self.conditions.entries.clear();
    }

    fn ready_condition(&self) -> Option<ResourceConditionValue> {
        self.conditions
            .entries
            .get(&ready_condition_type_ref())
            .and_then(|value| serde_json::from_value(value.clone()).ok())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusLike: Send + Sync {
    fn resource_status(&self) -> &ResourceStatus;
    fn resource_status_mut(&mut self) -> &mut ResourceStatus;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
