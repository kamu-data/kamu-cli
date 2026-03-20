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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceCondition {
    pub type_: ResourceConditionType,
    pub status: ResourceConditionStatus,
    pub reason: String,
    pub message: Option<String>,
    pub last_transition_time: DateTime<Utc>,
}

impl ResourceCondition {
    pub fn set_condition(
        conditions: &mut Vec<ResourceCondition>,
        new_condition: ResourceCondition,
    ) {
        if let Some(existing) = conditions
            .iter_mut()
            .find(|c| c.type_ == new_condition.type_)
        {
            let status_changed = existing.status != new_condition.status;
            *existing = ResourceCondition {
                last_transition_time: if status_changed {
                    new_condition.last_transition_time
                } else {
                    existing.last_transition_time
                },
                ..new_condition
            };
        } else {
            conditions.push(new_condition);
        }
    }

    pub fn accepted_true(now: DateTime<Utc>) -> Self {
        Self {
            type_: ResourceConditionType::Accepted,
            status: ResourceConditionStatus::True,
            reason: "ValidationPassed".to_string(),
            message: None,
            last_transition_time: now,
        }
    }

    pub fn accepted_false(
        now: DateTime<Utc>,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            type_: ResourceConditionType::Accepted,
            status: ResourceConditionStatus::False,
            reason: reason.into(),
            message: Some(message.into()),
            last_transition_time: now,
        }
    }

    pub fn ready_true(now: DateTime<Utc>) -> Self {
        Self {
            type_: ResourceConditionType::Ready,
            status: ResourceConditionStatus::True,
            reason: "Reconciled".to_string(),
            message: None,
            last_transition_time: now,
        }
    }

    pub fn ready_false(
        now: DateTime<Utc>,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            type_: ResourceConditionType::Ready,
            status: ResourceConditionStatus::False,
            reason: reason.into(),
            message: Some(message.into()),
            last_transition_time: now,
        }
    }

    pub fn reconciling_true(now: DateTime<Utc>) -> Self {
        Self {
            type_: ResourceConditionType::Reconciling,
            status: ResourceConditionStatus::True,
            reason: "Processing".to_string(),
            message: None,
            last_transition_time: now,
        }
    }

    pub fn reconciling_false(now: DateTime<Utc>) -> Self {
        Self {
            type_: ResourceConditionType::Reconciling,
            status: ResourceConditionStatus::False,
            reason: "Idle".to_string(),
            message: None,
            last_transition_time: now,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceConditionStatus {
    True,
    False,
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceConditionType {
    Accepted,
    Ready,
    Reconciling,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
