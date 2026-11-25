// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use odf::metadata::auth;

use crate::{
    ResourceAnnotations,
    ResourceHeadersInput,
    ResourceHeadersValidationError,
    ResourceLabels,
    ResourceName,
    ResourceRef,
    TypeRef,
    validate_entry_counts,
    validate_name,
    validate_owner_references,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// [`ResourceHeadersInput`] whose name may still be left to the resource type.
/// The resource to update is addressed beside the headers, never in them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyResourceHeaders {
    /// `None` keeps the name of an existing resource and has a new one's name
    /// generated
    pub name: Option<ResourceName>,
    pub account: Option<auth::ResourceOwnerRef>,
    pub labels: Option<ResourceLabels>,
    pub annotations: Option<ResourceAnnotations>,
    pub owner_references: Option<Vec<ResourceRef>>,
}

impl ApplyResourceHeaders {
    pub fn try_new(
        account: Option<auth::ResourceOwnerRef>,
        name: Option<&str>,
        labels: Vec<(TypeRef, serde_json::Value)>,
        annotations: Vec<(TypeRef, serde_json::Value)>,
    ) -> Result<Self, ResourceHeadersValidationError> {
        let name = name.map(Self::parse_name).transpose()?;

        let labels = ResourceLabels {
            entries: entries_to_map(labels, ResourceHeaderField::Labels)?,
        };
        let annotations = ResourceAnnotations {
            entries: entries_to_map(annotations, ResourceHeaderField::Annotations)?,
        };
        validate_entry_counts(Some(&labels), Some(&annotations))?;

        Ok(Self {
            name,
            account,
            labels: Some(labels),
            annotations: Some(annotations),
            owner_references: None,
        })
    }

    pub fn try_with_owner_references(
        self,
        owner_references: Vec<ResourceRef>,
    ) -> Result<Self, ResourceHeadersValidationError> {
        validate_owner_references(&owner_references)?;

        Ok(Self {
            owner_references: Some(owner_references),
            ..self
        })
    }

    fn parse_name(name: &str) -> Result<ResourceName, ResourceHeadersValidationError> {
        if name.is_empty() {
            return Err(ResourceHeadersValidationError::EmptyName);
        }

        let name = name
            .parse()
            .map_err(|_| ResourceHeadersValidationError::InvalidName {
                name: name.to_string(),
            })?;
        validate_name(&name)?;

        Ok(name)
    }

    /// For headers given to an
    /// [`ApplyResourceUseCase`](crate::ApplyResourceUseCase), which takes them
    /// with the account resolved
    pub fn resolved_account_id(&self) -> &odf::AccountID {
        self.resolved_account()
            .did
            .as_ref()
            .expect("resource headers must carry a resolved account by apply time")
    }

    /// See [`Self::resolved_account_id`]
    pub fn resolved_account(&self) -> &auth::ResourceOwnerRef {
        self.account
            .as_ref()
            .expect("resource headers must carry a resolved account by apply time")
    }

    /// Headers of a resource that has the name `name`, whatever `self.name` is
    pub fn into_named(self, name: ResourceName) -> ResourceHeadersInput {
        ResourceHeadersInput {
            id: None,
            name,
            account: self.account,
            labels: self.labels,
            annotations: self.annotations,
            owner_references: self.owner_references,
        }
    }
}

impl From<ResourceHeadersInput> for ApplyResourceHeaders {
    fn from(value: ResourceHeadersInput) -> Self {
        Self {
            name: Some(value.name),
            account: value.account,
            labels: value.labels,
            annotations: value.annotations,
            owner_references: value.owner_references,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn entries_to_map(
    entries: Vec<(TypeRef, serde_json::Value)>,
    field: ResourceHeaderField,
) -> Result<BTreeMap<TypeRef, serde_json::Value>, ResourceHeadersValidationError> {
    let mut map = BTreeMap::new();

    for (key, value) in entries {
        if map.insert(key.clone(), value).is_some() {
            return Err(match field {
                ResourceHeaderField::Labels => ResourceHeadersValidationError::DuplicateLabelKey {
                    key: key.to_string(),
                },
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum ResourceHeaderField {
    Labels,
    Annotations,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_matches;

    use super::*;
    use crate::{MAX_ANNOTATIONS, MAX_LABELS, MAX_OWNER_REFERENCES, ResourceID};

    fn account() -> auth::ResourceOwnerRef {
        auth::ResourceOwnerRef {
            r#type: None,
            id: None,
            did: Some(odf::AccountID::new_seeded_ed25519(b"test-account")),
            name: None,
        }
    }

    fn entry(key: &str, value: serde_json::Value) -> (TypeRef, serde_json::Value) {
        (key.parse().unwrap(), value)
    }

    #[test]
    fn accepts_short_name_and_full_uri_keys_with_nested_values() {
        let headers = ApplyResourceHeaders::try_new(
            Some(account()),
            Some("n"),
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

        assert_eq!(headers.labels.unwrap().entries.len(), 2);
        assert_eq!(headers.annotations.unwrap().entries.len(), 1);
    }

    #[test]
    fn accepts_omitted_name_but_not_an_empty_one() {
        let headers = ApplyResourceHeaders::try_new(Some(account()), None, vec![], vec![]).unwrap();
        assert_eq!(headers.name, None);

        assert_matches!(
            ApplyResourceHeaders::try_new(Some(account()), Some(""), vec![], vec![]),
            Err(ResourceHeadersValidationError::EmptyName)
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn rejects_duplicate_label_key() {
        let result = ApplyResourceHeaders::try_new(
            Some(account()),
            Some("n"),
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
        let result = ApplyResourceHeaders::try_new(
            Some(account()),
            Some("n"),
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
        let labels = (0..=MAX_LABELS)
            .map(|i| entry(&format!("label{i}"), serde_json::json!(i)))
            .collect();

        let result = ApplyResourceHeaders::try_new(Some(account()), Some("n"), labels, vec![]);

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::TooManyLabels { actual, max })
                if actual == MAX_LABELS + 1 && max == MAX_LABELS
        );
    }

    #[test]
    fn rejects_too_many_annotations() {
        let annotations = (0..=MAX_ANNOTATIONS)
            .map(|i| entry(&format!("annotation{i}"), serde_json::json!(i)))
            .collect();

        let result = ApplyResourceHeaders::try_new(Some(account()), Some("n"), vec![], annotations);

        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::TooManyAnnotations { actual, max })
                if actual == MAX_ANNOTATIONS + 1
                    && max == MAX_ANNOTATIONS
        );
    }

    #[test]
    fn accepts_arbitrarily_large_label_value() {
        // Values can be arbitrarily complex JSON — no size limit is enforced.
        let large_value = serde_json::json!("x".repeat(100_000));

        let headers = ApplyResourceHeaders::try_new(
            Some(account()),
            Some("n"),
            vec![entry("big", large_value.clone())],
            vec![],
        )
        .unwrap();

        assert_eq!(
            headers.labels.unwrap().entries.get(&"big".parse().unwrap()),
            Some(&large_value)
        );
    }

    #[test]
    fn rejects_too_many_owner_references() {
        let owner_references = vec![
            ResourceRef {
                id: Some(ResourceID::new(uuid::Uuid::new_v4())),
                ..ResourceRef::default()
            };
            MAX_OWNER_REFERENCES + 1
        ];

        let result = ApplyResourceHeaders::try_new(Some(account()), Some("n"), vec![], vec![])
            .unwrap()
            .try_with_owner_references(owner_references);
        assert_matches!(
            result,
            Err(ResourceHeadersValidationError::TooManyOwnerReferences { actual, max })
                if actual == MAX_OWNER_REFERENCES + 1 && max == MAX_OWNER_REFERENCES
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
