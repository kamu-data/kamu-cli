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
    pub fn new_pending() -> Self {
        Self {
            phase: ResourcePhase::Pending,
            observed_generation: 0,
            conditions: Vec::new(),
        }
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

    pub fn reset_for_new_spec(&mut self) {
        self.phase = ResourcePhase::Pending;
        self.conditions.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
