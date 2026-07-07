// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ResourceAnnotations, ResourceLabels, ResourceName, ResourceValidateHeaders, TypeRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceHeadersInput {
    pub account: odf::AccountID,
    pub name: ResourceName,
    pub description: Option<String>,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceLabels")]
    pub labels: ResourceLabels,
    #[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceAnnotations")]
    pub annotations: ResourceAnnotations,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceHeadersInput {
    pub const MAX_NAME_LEN: usize = 128;
    pub const MAX_DESCRIPTION_LEN: usize = 4096;
    pub const MAX_LABELS: usize = 64;
    pub const MAX_ANNOTATIONS: usize = 128;

    pub fn try_new(
        account: odf::AccountID,
        name: &str,
        description: Option<String>,
        labels: Vec<(TypeRef, serde_json::Value)>,
        annotations: Vec<(TypeRef, serde_json::Value)>,
    ) -> Result<Self, ResourceHeadersValidationError> {
        if name.is_empty() {
            return Err(ResourceHeadersValidationError::EmptyName);
        }

        let name = name
            .parse()
            .map_err(|_| ResourceHeadersValidationError::InvalidName {
                name: name.to_string(),
            })?;

        let headers = Self {
            account,
            name,
            description,
            labels: ResourceLabels {
                entries: Self::entries_to_map(labels, ResourceHeaderField::Labels)?,
            },
            annotations: ResourceAnnotations {
                entries: Self::entries_to_map(annotations, ResourceHeaderField::Annotations)?,
            },
        };

        headers.validate()?;

        Ok(headers)
    }

    fn entries_to_map(
        entries: Vec<(TypeRef, serde_json::Value)>,
        field: ResourceHeaderField,
    ) -> Result<BTreeMap<TypeRef, serde_json::Value>, ResourceHeadersValidationError> {
        let mut map = BTreeMap::new();

        for (key, value) in entries {
            if map.insert(key.clone(), value).is_some() {
                return Err(match field {
                    ResourceHeaderField::Labels => {
                        ResourceHeadersValidationError::DuplicateLabelKey {
                            key: key.to_string(),
                        }
                    }
                    ResourceHeaderField::Annotations => {
                        ResourceHeadersValidationError::DuplicateAnnotationKey {
                            key: key.to_string(),
                        }
                    }
                });
            }
        }

        Ok(map)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum ResourceHeaderField {
    Labels,
    Annotations,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateHeaders for ResourceHeadersInput {
    type ValidationError = ResourceHeadersValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.name.is_empty() {
            return Err(ResourceHeadersValidationError::EmptyName);
        }

        if self.name.len() > Self::MAX_NAME_LEN {
            return Err(ResourceHeadersValidationError::NameTooLong {
                actual: self.name.len(),
                max: Self::MAX_NAME_LEN,
            });
        }

        if let Some(description) = &self.description
            && description.len() > Self::MAX_DESCRIPTION_LEN
        {
            return Err(ResourceHeadersValidationError::DescriptionTooLong {
                actual: description.len(),
                max: Self::MAX_DESCRIPTION_LEN,
            });
        }

        if self.labels.entries.len() > Self::MAX_LABELS {
            return Err(ResourceHeadersValidationError::TooManyLabels {
                actual: self.labels.entries.len(),
                max: Self::MAX_LABELS,
            });
        }

        if self.annotations.entries.len() > Self::MAX_ANNOTATIONS {
            return Err(ResourceHeadersValidationError::TooManyAnnotations {
                actual: self.annotations.entries.len(),
                max: Self::MAX_ANNOTATIONS,
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ResourceHeadersValidationError {
    #[error("resource name must not be empty")]
    EmptyName,

    #[error("resource name is too long: got {actual}, max is {max}")]
    NameTooLong { actual: usize, max: usize },

    #[error("invalid resource name '{name}'")]
    InvalidName { name: String },

    #[error("description is too long: got {actual}, max is {max}")]
    DescriptionTooLong { actual: usize, max: usize },

    #[error("too many labels: got {actual}, max is {max}")]
    TooManyLabels { actual: usize, max: usize },

    #[error("duplicate label key '{key}'")]
    DuplicateLabelKey { key: String },

    #[error("too many annotations: got {actual}, max is {max}")]
    TooManyAnnotations { actual: usize, max: usize },

    #[error("duplicate annotation key '{key}'")]
    DuplicateAnnotationKey { key: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;

    fn account() -> odf::AccountID {
        odf::AccountID::new_seeded_ed25519(b"test-account")
    }

    fn entry(key: &str, value: serde_json::Value) -> (TypeRef, serde_json::Value) {
        (key.parse().unwrap(), value)
    }

    #[test]
    fn accepts_short_name_and_full_uri_keys_with_nested_values() {
        let headers = ResourceHeadersInput::try_new(
            account(),
            "n",
            None,
            vec![
                entry("env", serde_json::json!("prod")),
                entry(
                    "https://opendatafabric.org/schemas/labels/v1/Team",
                    serde_json::json!({ "name": "data-platform", "oncall": ["a", "b"] }),
                ),
            ],
            vec![entry(
                "owner",
                serde_json::json!("https://github.com/open-data-fabric"),
            )],
        )
        .unwrap();

        assert_eq!(headers.labels.entries.len(), 2);
        assert_eq!(headers.annotations.entries.len(), 1);
    }

    #[test]
    fn rejects_duplicate_label_key() {
        let result = ResourceHeadersInput::try_new(
            account(),
            "n",
            None,
            vec![
                entry("env", serde_json::json!("prod")),
                entry("env", serde_json::json!("staging")),
            ],
            vec![],
        );

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::DuplicateLabelKey { key }) if key == "env"
        );
    }

    #[test]
    fn rejects_duplicate_annotation_key() {
        let result = ResourceHeadersInput::try_new(
            account(),
            "n",
            None,
            vec![],
            vec![
                entry("owner", serde_json::json!("a")),
                entry("owner", serde_json::json!("b")),
            ],
        );

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::DuplicateAnnotationKey { key }) if key == "owner"
        );
    }

    #[test]
    fn rejects_too_many_labels() {
        let labels = (0..=ResourceHeadersInput::MAX_LABELS)
            .map(|i| entry(&format!("label{i}"), serde_json::json!(i)))
            .collect();

        let result = ResourceHeadersInput::try_new(account(), "n", None, labels, vec![]);

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::TooManyLabels { actual, max })
                if actual == ResourceHeadersInput::MAX_LABELS + 1 && max == ResourceHeadersInput::MAX_LABELS
        );
    }

    #[test]
    fn rejects_too_many_annotations() {
        let annotations = (0..=ResourceHeadersInput::MAX_ANNOTATIONS)
            .map(|i| entry(&format!("annotation{i}"), serde_json::json!(i)))
            .collect();

        let result = ResourceHeadersInput::try_new(account(), "n", None, vec![], annotations);

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::TooManyAnnotations { actual, max })
                if actual == ResourceHeadersInput::MAX_ANNOTATIONS + 1
                    && max == ResourceHeadersInput::MAX_ANNOTATIONS
        );
    }

    #[test]
    fn accepts_arbitrarily_large_label_value() {
        // Values can be arbitrarily complex JSON — no size limit is enforced.
        let large_value = serde_json::json!("x".repeat(100_000));

        let headers = ResourceHeadersInput::try_new(
            account(),
            "n",
            None,
            vec![entry("big", large_value.clone())],
            vec![],
        )
        .unwrap();

        assert_eq!(
            headers.labels.entries.get(&"big".parse().unwrap()),
            Some(&large_value)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
