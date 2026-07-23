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

use crate::{TypeRef, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_CONDITION_ACCEPTED_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/conditions/Accepted";
pub const RESOURCE_CONDITION_READY_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready";
pub const RESOURCE_CONDITION_RECONCILING_SCHEMA_URI: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResourceConditionValue {
    pub value: ResourceConditionStatus,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_transition_time: DateTime<Utc>,
}

impl ResourceConditionValue {
    pub fn set_condition(
        conditions: &mut std::collections::BTreeMap<TypeRef, serde_json::Value>,
        (condition_key, new_condition): (TypeRef, ResourceConditionValue),
    ) {
        let condition = if let Some(existing_value) = conditions.get(&condition_key) {
            if let Ok(existing) =
                serde_json::from_value::<ResourceConditionValue>(existing_value.clone())
            {
                let status_changed = existing.value != new_condition.value;
                ResourceConditionValue {
                    last_transition_time: if status_changed {
                        new_condition.last_transition_time
                    } else {
                        existing.last_transition_time
                    },
                    ..new_condition
                }
            } else {
                new_condition
            }
        } else {
            new_condition
        };

        conditions.insert(condition_key, serde_json::to_value(condition).unwrap());
    }

    pub fn accepted_true(now: DateTime<Utc>) -> (TypeRef, Self) {
        (
            accepted_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::True,
                reason: "ValidationPassed".to_string(),
                message: None,
                last_transition_time: now,
            },
        )
    }

    pub fn accepted_false(
        now: DateTime<Utc>,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> (TypeRef, Self) {
        (
            accepted_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::False,
                reason: reason.into(),
                message: Some(message.into()),
                last_transition_time: now,
            },
        )
    }

    pub fn ready_true(now: DateTime<Utc>) -> (TypeRef, Self) {
        (
            ready_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::True,
                reason: "Reconciled".to_string(),
                message: None,
                last_transition_time: now,
            },
        )
    }

    pub fn ready_false(
        now: DateTime<Utc>,
        reason: impl Into<String>,
        message: impl Into<String>,
    ) -> (TypeRef, Self) {
        (
            ready_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::False,
                reason: reason.into(),
                message: Some(message.into()),
                last_transition_time: now,
            },
        )
    }

    pub fn reconciling_true(now: DateTime<Utc>) -> (TypeRef, Self) {
        (
            reconciling_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::True,
                reason: "Processing".to_string(),
                message: None,
                last_transition_time: now,
            },
        )
    }

    pub fn reconciling_false(now: DateTime<Utc>) -> (TypeRef, Self) {
        (
            reconciling_condition_type_ref(),
            Self {
                value: ResourceConditionStatus::False,
                reason: "Idle".to_string(),
                message: None,
                last_transition_time: now,
            },
        )
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

pub fn accepted_condition_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_CONDITION_ACCEPTED_SCHEMA_URI,
    ))
}

pub fn ready_condition_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(RESOURCE_CONDITION_READY_SCHEMA_URI))
}

pub fn reconciling_condition_type_ref() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(
        RESOURCE_CONDITION_RECONCILING_SCHEMA_URI,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
