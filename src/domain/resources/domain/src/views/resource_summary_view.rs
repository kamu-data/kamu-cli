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
    ResourceConditionType,
    ResourceListColumnValueView,
    ResourceName,
    ResourcePhase,
    ResourceSnapshot,
    ResourceStatus,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSummaryView {
    pub kind: String,
    pub api_version: String,
    pub uid: ResourceUID,
    pub name: ResourceName,
    pub description: Option<String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummaryView>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceStatusSummaryView {
    pub phase: Option<ResourcePhase>,
    pub observed_generation: Option<u64>,
    pub ready: Option<bool>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ResourceSnapshot> for ResourceSummaryView {
    fn from(value: ResourceSnapshot) -> Self {
        let status = value.basic_status().map(Into::into);

        Self {
            kind: value.kind,
            api_version: value.api_version,
            uid: value.uid,
            name: value.headers.name,
            description: value.headers.description,
            generation: value.headers.generation,
            created_at: value.headers.created_at,
            updated_at: value.headers.updated_at,
            status,
            list_values: Vec::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ResourceStatus> for ResourceStatusSummaryView {
    fn from(value: ResourceStatus) -> Self {
        let ready = value
            .conditions
            .iter()
            .find(|condition| condition.type_ == ResourceConditionType::Ready)
            .map(|condition| match condition.status {
                ResourceConditionStatus::True => true,
                ResourceConditionStatus::False | ResourceConditionStatus::Unknown => false,
            });

        Self {
            phase: Some(value.phase),
            observed_generation: Some(value.observed_generation),
            ready,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
