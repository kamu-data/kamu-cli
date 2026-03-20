// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ResourceName = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceMetadata {
    pub name: ResourceName,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

impl From<ResourceMetadataInput> for ResourceMetadata {
    fn from(input: ResourceMetadataInput) -> Self {
        Self {
            name: input.name,
            description: input.description,
            labels: input.labels,
            annotations: input.annotations,
            generation: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            deleted_at: None,
        }
    }
}

impl ResourceMetadata {
    pub fn is_equivalent_to(&self, input: &ResourceMetadataInput) -> bool {
        self.name == input.name
            && self.description == input.description
            && self.labels == input.labels
            && self.annotations == input.annotations
    }

    pub fn update(&mut self, input: ResourceMetadataInput) {
        self.name = input.name;
        self.description = input.description;
        self.labels = input.labels;
        self.annotations = input.annotations;

        self.generation += 1;
        self.updated_at = Utc::now();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceMetadataInput {
    pub name: ResourceName,
    pub description: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
