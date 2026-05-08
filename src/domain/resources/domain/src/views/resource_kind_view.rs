// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use like::ILike;

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

    pub fn matches_selector_pattern(&self, pattern: &str) -> bool {
        matches_wildcard_pattern(pattern, &self.name)
            || self
                .short_names
                .iter()
                .any(|short_name| matches_wildcard_pattern(pattern, short_name))
    }
}

fn matches_wildcard_pattern(pattern: &str, value: &str) -> bool {
    ILike::<false>::ilike(value, pattern).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_selectors_case_insensitively() {
        let descriptor = ResourceKindDescriptor {
            name: "variablesets".to_owned(),
            short_names: vec!["vs".to_owned()],
            kind: "dev.kamu/variableset".to_owned(),
            api_version: "v1".to_owned(),
            list_columns: Vec::new(),
        };

        assert!(descriptor.matches_selector("VARIABLESETS"));
        assert!(descriptor.matches_selector("VS"));
    }

    #[test]
    fn matches_patterns_case_insensitively_for_names_and_short_names() {
        let descriptor = ResourceKindDescriptor {
            name: "secretsets".to_owned(),
            short_names: vec!["ss".to_owned()],
            kind: "dev.kamu/secretset".to_owned(),
            api_version: "v1".to_owned(),
            list_columns: Vec::new(),
        };

        assert!(descriptor.matches_selector_pattern("S%"));
        assert!(descriptor.matches_selector_pattern("%TS"));
        assert!(!descriptor.matches_selector_pattern("V%"));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
