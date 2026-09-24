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
    ResourceID,
    ResourceListColumnValueView,
    ResourceName,
    ResourcePhase,
    ResourceSnapshot,
    ResourceStatus,
    TypeUri,
    get_description,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceSummaryView {
    pub schema: TypeUri,
    pub id: ResourceID,
    pub name: ResourceName,
    pub description: Option<String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: Option<ResourceStatusSummaryView>,
    pub list_values: Vec<ResourceListColumnValueView>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceStatusSummaryView {
    #[serde_as(as = "Option<odf::metadata::serde::yaml::resources::ResourcePhase>")]
    pub phase: Option<ResourcePhase>,
    pub observed_generation: Option<u64>,
    pub observed_at: Option<DateTime<Utc>>,
    pub reconciled_generation: Option<u64>,
    pub reconciled_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<ResourceSnapshot> for ResourceSummaryView {
    fn from(value: ResourceSnapshot) -> Self {
        let status = value.basic_status().map(Into::into);

        Self {
            schema: value.schema,
            id: value.id,
            name: value.headers.name,
            description: get_description(&value.headers.annotations.entries).map(str::to_string),
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
        let ResourceStatus {
            phase,
            observed_generation,
            observed_at,
            reconciled_generation,
            reconciled_at,
            conditions: _,
        } = value;
        Self {
            phase: Some(phase),
            observed_generation,
            observed_at,
            reconciled_generation,
            reconciled_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
