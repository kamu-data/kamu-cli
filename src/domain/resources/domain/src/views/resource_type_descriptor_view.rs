// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use like::ILike;

use crate::{ResourceListColumnDescriptor, ResourceSelectorName, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceTypeDescriptor {
    pub canonical_selector: ResourceSelectorName,
    pub selector_aliases: Vec<ResourceSelectorName>,
    pub schema: TypeUri,
    pub list_columns: Vec<ResourceListColumnDescriptor>,
}

impl ResourceTypeDescriptor {
    pub fn matches_selector(&self, selector: impl AsRef<str>) -> bool {
        resource_type_matches_selector(&self.canonical_selector, &self.selector_aliases, selector)
    }

    pub fn matches_selector_pattern(&self, pattern: &str) -> bool {
        matches_wildcard_pattern(pattern, self.canonical_selector.as_str())
            || self
                .selector_aliases
                .iter()
                .any(|alias| matches_wildcard_pattern(pattern, alias.as_str()))
    }
}

pub fn resource_type_matches_selector(
    selector_name: &ResourceSelectorName,
    selector_aliases: &[ResourceSelectorName],
    candidate_selector: impl AsRef<str>,
) -> bool {
    resource_selector_parts_match(
        selector_name.as_str(),
        selector_aliases.iter().map(ResourceSelectorName::as_str),
        candidate_selector,
    )
}

pub fn resource_selector_parts_match<'a>(
    selector_name: &str,
    selector_aliases: impl IntoIterator<Item = &'a str>,
    candidate_selector: impl AsRef<str>,
) -> bool {
    let selector = candidate_selector.as_ref();
    selector_name.eq_ignore_ascii_case(selector)
        || selector_aliases
            .into_iter()
            .any(|alias| alias.eq_ignore_ascii_case(selector))
}

fn matches_wildcard_pattern(pattern: &str, value: &str) -> bool {
    ILike::<false>::ilike(value, pattern).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceTypeSelectorRaw;

    #[test]
    fn matches_selectors_case_insensitively() {
        let descriptor = ResourceTypeDescriptor {
            canonical_selector: ResourceSelectorName::new("variablesets").unwrap(),
            selector_aliases: vec![ResourceSelectorName::new("vs").unwrap()],
            schema: odf::metadata::config::VariableSet::schema().clone(),
            list_columns: Vec::new(),
        };

        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("VARIABLESETS").unwrap()));
        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("VS").unwrap()));
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("VariableSet").unwrap()));
    }

    #[test]
    fn matches_patterns_case_insensitively_for_names_and_short_names() {
        let descriptor = ResourceTypeDescriptor {
            canonical_selector: ResourceSelectorName::new("secretsets").unwrap(),
            selector_aliases: vec![ResourceSelectorName::new("ss").unwrap()],
            schema: odf::metadata::config::SecretSet::schema().clone(),
            list_columns: Vec::new(),
        };

        assert!(descriptor.matches_selector_pattern("S%"));
        assert!(descriptor.matches_selector_pattern("%TS"));
        assert!(!descriptor.matches_selector_pattern("V%"));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
