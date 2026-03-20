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

use crate::ResourceMetadataInput;

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

impl ResourceMetadata {
    pub fn from_input(now: DateTime<Utc>, input: ResourceMetadataInput) -> Self {
        Self {
            name: input.name,
            description: input.description,
            labels: input.labels,
            annotations: input.annotations,
            generation: 1,
            created_at: now,
            updated_at: now,
            deleted_at: None,
        }
    }

    pub fn is_equivalent_to(&self, input: &ResourceMetadataInput) -> bool {
        self.name == input.name
            && self.description == input.description
            && self.labels == input.labels
            && self.annotations == input.annotations
    }

    pub fn apply_update(&mut self, now: DateTime<Utc>, input: ResourceMetadataInput) {
        self.name = input.name;
        self.description = input.description;
        self.labels = input.labels;
        self.annotations = input.annotations;

        self.updated_at = now;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
