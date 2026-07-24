// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    ResourceExtensionKind,
    ResourceSchemaId,
    TypeName,
    TypeRef,
    TypeUri,
};
use kamu_resources_services::{
    ResourceExtensionResolutionError,
    ResourceExtensionSchemaResolver,
    WARNING_CODE_FREEFORM_ANNOTATIONS,
    WARNING_CODE_FREEFORM_LABELS,
};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_short_name_is_canonicalized_and_validated_with_no_warning() {
    let harness = ExtensionResolverHarness::new();

    let resolved = harness.canonicalize_labels(vec![("environment", serde_json::json!("prod"))]);

    assert_eq!(
        resolved.entries,
        vec![(
            harness.uri_ref(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
            serde_json::json!("prod")
        )]
    );
    assert!(resolved.warnings.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unknown_short_name_is_preserved_as_free_form_with_warning() {
    let harness = ExtensionResolverHarness::new();

    let resolved = harness.canonicalize_labels(vec![
        ("team", serde_json::json!("data-platform")),
        ("description", serde_json::json!("label-not-annotation")),
    ]);

    assert_eq!(
        resolved.entries,
        vec![
            (harness.name_ref("team"), serde_json::json!("data-platform")),
            (
                harness.name_ref("description"),
                serde_json::json!("label-not-annotation")
            ),
        ]
    );
    assert_eq!(resolved.warnings.len(), 1);
    assert_eq!(resolved.warnings[0].code, WARNING_CODE_FREEFORM_LABELS);
    assert_eq!(resolved.warnings[0].path.as_deref(), Some("headers.labels"));
    assert_eq!(
        resolved.warnings[0].message,
        "resource headers contain free-form labels not backed by a registered schema: \
         description, team"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_unknown_annotation_short_name_uses_annotation_free_form_warning() {
    let harness = ExtensionResolverHarness::new();

    let resolved =
        harness.canonicalize_annotations(vec![("note", serde_json::json!("check this later"))]);

    assert_eq!(
        resolved.entries,
        vec![(
            harness.name_ref("note"),
            serde_json::json!("check this later")
        )]
    );
    assert_eq!(resolved.warnings.len(), 1);
    assert_eq!(resolved.warnings[0].code, WARNING_CODE_FREEFORM_ANNOTATIONS);
    assert_eq!(
        resolved.warnings[0].path.as_deref(),
        Some("headers.annotations")
    );
    assert_eq!(
        resolved.warnings[0].message,
        "resource headers contain free-form annotations not backed by a registered schema: note"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_uri_resolution_is_strict_for_unknown_and_wrong_kind() {
    let harness = ExtensionResolverHarness::new();

    harness.assert_label_error(
        &harness.uri_ref("https://kamu.dev/schemas/resource/v1alpha1/labels/Missing"),
        &serde_json::json!("prod"),
        &ResourceExtensionResolutionError::UnknownUri {
            kind: ResourceExtensionKind::Label,
            uri: TypeUri::new_unchecked(
                "https://kamu.dev/schemas/resource/v1alpha1/labels/Missing",
            ),
        },
    );
    harness.assert_label_error(
        &harness.uri_ref(RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI),
        &serde_json::json!("prod"),
        &ResourceExtensionResolutionError::KindMismatch {
            uri: TypeUri::new_unchecked(RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI),
            expected_kind: ResourceExtensionKind::Label,
            actual_kind: ResourceExtensionKind::Annotation,
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_invalid_registered_value_uses_concise_dispatcher_message() {
    let harness = ExtensionResolverHarness::new();

    harness.assert_label_error(
        &harness.name_ref("environment"),
        &serde_json::json!(["prod"]),
        &ResourceExtensionResolutionError::InvalidValue {
            kind: ResourceExtensionKind::Label,
            authored_key: harness.name_ref("environment"),
            canonical_uri: TypeUri::new_unchecked(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
            reason: "environment must be a string".to_string(),
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_duplicate_after_canonicalization_is_rejected() {
    let harness = ExtensionResolverHarness::new();

    let error = harness.canonicalize_label_error(vec![
        ("environment", serde_json::json!("prod")),
        (
            RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
            serde_json::json!("stage"),
        ),
    ]);

    assert_eq!(
        error,
        ResourceExtensionResolutionError::DuplicateAfterCanonicalization {
            kind: ResourceExtensionKind::Label,
            canonical_key: harness.uri_ref(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
            authored_keys: vec![
                harness.name_ref("environment"),
                harness.uri_ref(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
            ],
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ExtensionResolverHarness {
    resolver: std::sync::Arc<ResourceExtensionSchemaResolver>,
    resource_schema: ResourceSchemaId,
}

impl ExtensionResolverHarness {
    fn new() -> Self {
        let mut builder = dill::CatalogBuilder::new();
        kamu_resources_services::register_dependencies(&mut builder);
        let catalog =
            kamu_resources_services::build_catalog_with_resource_extension_schema_registry(builder)
                .unwrap();

        Self {
            resolver: catalog.get_one().unwrap(),
            resource_schema: ResourceSchemaId::parse(
                "https://example.com/schemas/config/v1alpha1/Widget",
            )
            .unwrap(),
        }
    }

    fn canonicalize_labels(
        &self,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> kamu_resources_services::CanonicalResourceExtensionEntries {
        self.canonicalize(ResourceExtensionKind::Label, entries)
    }

    fn canonicalize_annotations(
        &self,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> kamu_resources_services::CanonicalResourceExtensionEntries {
        self.canonicalize(ResourceExtensionKind::Annotation, entries)
    }

    fn canonicalize(
        &self,
        kind: ResourceExtensionKind,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> kamu_resources_services::CanonicalResourceExtensionEntries {
        self.resolver
            .canonicalize_entries(kind, self.make_entries(entries), &self.resource_schema)
            .unwrap()
    }

    fn canonicalize_label_error(
        &self,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> ResourceExtensionResolutionError {
        self.resolver
            .canonicalize_entries(
                ResourceExtensionKind::Label,
                self.make_entries(entries),
                &self.resource_schema,
            )
            .unwrap_err()
    }

    fn assert_label_error(
        &self,
        key: &TypeRef,
        value: &serde_json::Value,
        expected: &ResourceExtensionResolutionError,
    ) {
        let error = self
            .resolver
            .resolve_key(
                ResourceExtensionKind::Label,
                key,
                value,
                &self.resource_schema,
            )
            .unwrap_err();

        assert_eq!(&error, expected);
    }

    fn make_entries(
        &self,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> Vec<(TypeRef, serde_json::Value)> {
        entries
            .into_iter()
            .map(|(key, value)| (key.parse().unwrap(), value))
            .collect()
    }

    fn name_ref(&self, name: &str) -> TypeRef {
        TypeRef::Name(name.parse::<TypeName>().unwrap())
    }

    fn uri_ref(&self, uri: &str) -> TypeRef {
        TypeRef::Uri(TypeUri::new_unchecked(uri))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
