// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceListColumnDescriptor;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceKindDescriptor {
    pub name: String,
    pub short_names: Vec<String>,
    pub kind: String,
    pub api_version: String,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

impl ResourceKindDescriptor {
    pub fn matches_selector(&self, selector: &str) -> bool {
        self.name.eq_ignore_ascii_case(selector)
            || self
                .short_names
                .iter()
                .any(|short_name| short_name.eq_ignore_ascii_case(selector))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
