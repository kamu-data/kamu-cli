// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
        resource_type_matches_selector(
            &self.canonical_selector,
            &self.selector_aliases,
            &self.schema,
            selector,
        )
    }
}

pub fn resource_type_matches_selector(
    selector_name: &ResourceSelectorName,
    selector_aliases: &[ResourceSelectorName],
    schema: &TypeUri,
    candidate_selector: impl AsRef<str>,
) -> bool {
    resource_selector_parts_match(
        selector_name.as_str(),
        selector_aliases.iter().map(ResourceSelectorName::as_str),
        schema.as_str(),
        candidate_selector,
    )
}

pub fn resource_selector_parts_match<'a>(
    selector_name: &str,
    selector_aliases: impl IntoIterator<Item = &'a str>,
    schema: &str,
    candidate_selector: impl AsRef<str>,
) -> bool {
    let selector = candidate_selector.as_ref();
    selector_name.eq_ignore_ascii_case(selector)
        || selector_aliases
            .into_iter()
            .any(|alias| alias.eq_ignore_ascii_case(selector))
        || schema_matches_selector(schema, selector)
}

/// Whether `selector` is one of the two forms an ODF `TypeRef` can take: the
/// full schema URI, or the type name that ends it.
///
/// Accepting both is what lets the facade take ODF types directly rather than
/// twinning them — a manifest saying `VariableSet` and a CLI arg saying `vs`
/// resolve through one vocabulary.
fn schema_matches_selector(schema: &str, selector: &str) -> bool {
    // URIs are case-sensitive in the path segment, but the surrounding grammar
    // is already case-insensitive, so stay consistent with the aliases above.
    if schema.eq_ignore_ascii_case(selector) {
        return true;
    }

    // The ODF type name is the URI's last path segment
    // (`.../config/v1alpha1/VariableSet` -> `VariableSet`). Matching it here
    // avoids storing it separately and drifting from the schema.
    schema
        .rsplit('/')
        .next()
        .is_some_and(|type_name| !type_name.is_empty() && type_name.eq_ignore_ascii_case(selector))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceTypeSelectorRaw;

    fn variable_set_descriptor() -> ResourceTypeDescriptor {
        ResourceTypeDescriptor {
            canonical_selector: ResourceSelectorName::new("variablesets").unwrap(),
            selector_aliases: vec![ResourceSelectorName::new("vs").unwrap()],
            schema: odf::metadata::config::VariableSet::schema().clone(),
            list_columns: Vec::new(),
        }
    }

    #[test]
    fn matches_selectors_case_insensitively() {
        let descriptor = variable_set_descriptor();

        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("VARIABLESETS").unwrap()));
        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("VS").unwrap()));
    }

    /// The ODF type name is a first-class selector, so the facade can take ODF
    /// `TypeRef`s directly instead of twinning them.
    #[test]
    fn matches_the_odf_type_name() {
        let descriptor = variable_set_descriptor();

        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("VariableSet").unwrap()));
        assert!(descriptor.matches_selector(ResourceTypeSelectorRaw::new("variableset").unwrap()));
    }

    #[test]
    fn matches_the_full_schema_uri() {
        let descriptor = variable_set_descriptor();
        let schema = descriptor.schema.as_str().to_owned();

        assert!(descriptor.matches_selector(&schema));
    }

    /// The type name is derived from the schema's last segment, so a selector
    /// matching some *other* segment of the URI must not match.
    #[test]
    fn does_not_match_other_schema_path_segments() {
        let descriptor = variable_set_descriptor();

        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("config").unwrap()));
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("v1alpha1").unwrap()));
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("schemas").unwrap()));
    }

    /// A different type sharing the same URI prefix must not match.
    #[test]
    fn does_not_match_a_sibling_type_name() {
        let descriptor = variable_set_descriptor();

        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("SecretSet").unwrap()));
        assert!(!descriptor.matches_selector(odf::metadata::config::SecretSet::schema().as_str()));
    }

    #[test]
    fn does_not_match_wildcard_type_selectors() {
        let descriptor = ResourceTypeDescriptor {
            canonical_selector: ResourceSelectorName::new("secretsets").unwrap(),
            selector_aliases: vec![ResourceSelectorName::new("ss").unwrap()],
            schema: odf::metadata::config::SecretSet::schema().clone(),
            list_columns: Vec::new(),
        };

        // Type selectors are matched exactly — `%` carries no wildcard meaning
        // here, and is resolved as an all-types token by the CLI instead.
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("S%").unwrap()));
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("%TS").unwrap()));
        assert!(!descriptor.matches_selector(ResourceTypeSelectorRaw::new("%").unwrap()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
