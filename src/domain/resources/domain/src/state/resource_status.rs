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

use crate::{ResourceCondition, ResourcePhase};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceStatus {
    pub phase: ResourcePhase,
    pub observed_generation: u64,
    pub conditions: Vec<ResourceCondition>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceStatus {
    pub fn from_json(value: &serde_json::Value) -> Option<Self> {
        let serde_json::Value::Object(status_map) = value else {
            return None;
        };

        let mut status_json = serde_json::Map::with_capacity(3);
        status_json.insert("phase".to_string(), status_map.get("phase")?.clone());
        status_json.insert(
            "observed_generation".to_string(),
            status_map.get("observed_generation")?.clone(),
        );
        status_json.insert(
            "conditions".to_string(),
            status_map.get("conditions")?.clone(),
        );

        serde_json::from_value(serde_json::Value::Object(status_json)).ok()
    }

    pub fn new_pending() -> Self {
        Self {
            phase: ResourcePhase::Pending,
            observed_generation: 0,
            conditions: Vec::new(),
        }
    }

    pub fn needs_reconciliation(&self, generation: u64) -> bool {
        self.observed_generation < generation
    }

    pub fn last_reconciled_at(&self) -> Option<DateTime<Utc>> {
        self.conditions.iter().find_map(|condition| {
            if condition.type_ == crate::ResourceConditionType::Ready {
                Some(condition.last_transition_time)
            } else {
                None
            }
        })
    }

    pub fn mark_reconciling(&mut self, now: DateTime<Utc>) {
        self.phase = ResourcePhase::Reconciling;
        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::reconciling_true(now),
        );
    }

    pub fn mark_ready(&mut self, now: DateTime<Utc>, observed_generation: u64) {
        self.phase = ResourcePhase::Ready;
        self.observed_generation = observed_generation;

        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::accepted_true(now),
        );
        ResourceCondition::set_condition(&mut self.conditions, ResourceCondition::ready_true(now));
        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::reconciling_false(now),
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

        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::accepted_true(now),
        );
        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::ready_false(now, reason, message),
        );
        ResourceCondition::set_condition(
            &mut self.conditions,
            ResourceCondition::reconciling_false(now),
        );
    }

    pub fn mark_pending_for_new_generation(&mut self) {
        self.phase = ResourcePhase::Pending;
        self.conditions.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceStatusLike: Send + Sync {
    fn resource_status(&self) -> &ResourceStatus;
    fn resource_status_mut(&mut self) -> &mut ResourceStatus;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
