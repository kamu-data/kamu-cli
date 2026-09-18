// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_configuration::RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI;
use kamu_resources::{
    ResourceExtensionKind,
    ResourceExtensionResolutionError,
    ResourceSchemaId,
    TypeName,
    TypeRef,
    TypeUri,
    WARNING_CODE_RESOURCE_FREEFORM_LABELS,
};
use kamu_resources_services::ResourceExtensionSchemaResolver;
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SAMPLE_DATASET_DID: &str =
    "did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4";

const SHORT_NAME: &str = "legacy-config-target-dataset";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_label_resolves_on_config_resources() {
    // `VariableSet` and `SecretSet` both live in the ODF `config` context that
    // the label is scoped to.
    for schema in [
        "https://opendatafabric.org/schemas/config/v1alpha1/VariableSet",
        "https://opendatafabric.org/schemas/config/v1alpha1/SecretSet",
    ] {
        let harness = LabelScopeHarness::new(schema);

        let resolved =
            harness.canonicalize_labels(vec![(SHORT_NAME, serde_json::json!(SAMPLE_DATASET_DID))]);

        assert_eq!(
            resolved.entries,
            vec![(
                harness.uri_ref(RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI),
                serde_json::json!(SAMPLE_DATASET_DID)
            )],
            "expected the label to canonicalize on {schema}"
        );
        assert!(
            resolved.warnings.is_empty(),
            "expected no free-form warning on {schema}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_label_short_name_is_free_form_outside_the_config_context() {
    // Same `config` context segment, different authority: the scope must not
    // match, so the short name stays free-form rather than canonicalizing.
    let harness = LabelScopeHarness::new("https://example.com/schemas/config/v1alpha1/Widget");

    let resolved =
        harness.canonicalize_labels(vec![(SHORT_NAME, serde_json::json!(SAMPLE_DATASET_DID))]);

    assert_eq!(
        resolved.entries,
        vec![(
            harness.name_ref(SHORT_NAME),
            serde_json::json!(SAMPLE_DATASET_DID)
        )]
    );
    assert_eq!(resolved.warnings.len(), 1);
    assert_eq!(
        resolved.warnings[0].code,
        WARNING_CODE_RESOURCE_FREEFORM_LABELS
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_label_uri_is_rejected_outside_the_config_context() {
    // A full URI cannot fall back to free-form, so an out-of-scope resource
    // gets a hard `Inapplicable` rejection.
    let harness = LabelScopeHarness::new("https://example.com/schemas/config/v1alpha1/Widget");

    let error = harness
        .resolver
        .resolve_key(
            ResourceExtensionKind::Label,
            &harness.uri_ref(RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI),
            &serde_json::json!(SAMPLE_DATASET_DID),
            &harness.resource_schema,
        )
        .unwrap_err();

    assert_eq!(
        error,
        ResourceExtensionResolutionError::Inapplicable {
            uri: TypeUri::new_unchecked(RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI),
            resource_schema: harness.resource_schema.typ().clone(),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_label_rejects_a_non_did_value() {
    let harness =
        LabelScopeHarness::new("https://opendatafabric.org/schemas/config/v1alpha1/VariableSet");

    let error = harness
        .resolver
        .canonicalize_entries(
            ResourceExtensionKind::Label,
            vec![(SHORT_NAME.parse().unwrap(), serde_json::json!("not-a-did"))],
            &harness.resource_schema,
        )
        .unwrap_err();

    assert_matches!(error, ResourceExtensionResolutionError::InvalidValue { .. });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct LabelScopeHarness {
    resolver: std::sync::Arc<ResourceExtensionSchemaResolver>,
    resource_schema: ResourceSchemaId,
}

impl LabelScopeHarness {
    fn new(schema: &str) -> Self {
        let mut builder = dill::CatalogBuilder::new();
        kamu_resources_services::register_dependencies(&mut builder);
        kamu_configuration_services::register_dependencies(&mut builder);
        let catalog = builder.build();

        Self {
            resolver: catalog.get_one().unwrap(),
            resource_schema: ResourceSchemaId::parse(schema).unwrap(),
        }
    }

    fn canonicalize_labels(
        &self,
        entries: Vec<(&str, serde_json::Value)>,
    ) -> kamu_resources_services::CanonicalResourceExtensionEntries {
        self.resolver
            .canonicalize_entries(
                ResourceExtensionKind::Label,
                entries
                    .into_iter()
                    .map(|(key, value)| (key.parse().unwrap(), value))
                    .collect(),
                &self.resource_schema,
            )
            .unwrap()
    }

    fn name_ref(&self, name: &str) -> TypeRef {
        TypeRef::Name(name.parse::<TypeName>().unwrap())
    }

    fn uri_ref(&self, uri: &str) -> TypeRef {
        TypeRef::Uri(TypeUri::new_unchecked(uri))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
