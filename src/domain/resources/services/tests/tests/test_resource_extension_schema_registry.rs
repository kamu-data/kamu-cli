// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_resources::{
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    RESOURCE_CONDITION_READY_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
    ResourceExtensionValueError,
    ResourceSchemaId,
    ResourceValidateSchemaValue,
    TypeName,
    TypeRef,
    TypeUri,
};
use kamu_resources_services::{ResourceExtensionResolutionError, ResourceExtensionSchemaRegistry};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_resolves_built_in_label_by_uri_and_short_name() {
    let harness = ExtensionRegistryHarness::built_ins();

    harness.assert_resolves_to_uri(
        ResourceExtensionKind::Label,
        &harness.uri_ref(RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI),
        RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    );
    harness.assert_resolves_to_uri(
        ResourceExtensionKind::Label,
        &harness.name_ref("environment"),
        RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_built_in_dispatchers_validate_values_with_clean_messages() {
    let harness = ExtensionRegistryHarness::built_ins();

    harness.assert_value_valid(
        ResourceExtensionKind::Label,
        RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
        &serde_json::json!("production"),
    );
    harness.assert_value_invalid(
        ResourceExtensionKind::Label,
        RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
        &serde_json::json!(["production"]),
        "environment must be a string",
    );
    harness.assert_value_invalid(
        ResourceExtensionKind::Annotation,
        RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
        &serde_json::json!(42),
        "description must be a string",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_description_length_counts_characters() {
    let harness = ExtensionRegistryHarness::built_ins();

    harness.assert_value_valid(
        ResourceExtensionKind::Annotation,
        RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
        &serde_json::json!("ї".repeat(4096)),
    );
    harness.assert_value_invalid(
        ResourceExtensionKind::Annotation,
        RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
        &serde_json::json!("ї".repeat(4097)),
        "description is too long: got 4097, max is 4096",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_condition_unknown_field_is_rejected() {
    let harness = ExtensionRegistryHarness::built_ins();

    harness.assert_value_invalid_contains(
        ResourceExtensionKind::Condition,
        RESOURCE_CONDITION_READY_SCHEMA_URI,
        &serde_json::json!({
            "value": "True",
            "reason": "Reconciled",
            "lastTransitionTime": "2026-01-01T00:00:00Z",
            "extra": true
        }),
        "condition value is invalid: unknown field `extra`",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_uri_resolution_reports_unknown_kind_mismatch_and_inapplicable() {
    let harness = ExtensionRegistryHarness::with_test_dispatchers(|b| {
        b.add::<ExactOnlyDispatcher>();
    });

    harness.assert_resolve_error(
        ResourceExtensionKind::Label,
        &harness.uri_ref("https://kamu.dev/schemas/resource/v1alpha1/labels/Missing"),
        &ResourceExtensionResolutionError::UnknownUri {
            kind: ResourceExtensionKind::Label,
            uri: TypeUri::new_unchecked(
                "https://kamu.dev/schemas/resource/v1alpha1/labels/Missing",
            ),
        },
    );
    harness.assert_resolve_error(
        ResourceExtensionKind::Label,
        &harness.uri_ref(RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI),
        &ResourceExtensionResolutionError::KindMismatch {
            uri: TypeUri::new_unchecked(RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI),
            expected_kind: ResourceExtensionKind::Label,
            actual_kind: ResourceExtensionKind::Annotation,
        },
    );
    harness.assert_resolve_error_for_schema(
        ResourceExtensionKind::Label,
        &harness.uri_ref(EXACT_ONLY_SCHEMA_ID),
        &ExtensionRegistryHarness::schema("https://example.com/schemas/config/v1alpha1/Other"),
        &ResourceExtensionResolutionError::Inapplicable {
            uri: TypeUri::new_unchecked(EXACT_ONLY_SCHEMA_ID),
            resource_schema: TypeUri::new_unchecked(
                "https://example.com/schemas/config/v1alpha1/Other",
            ),
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_short_name_resolution_uses_precedence_tiers() {
    let harness = ExtensionRegistryHarness::with_test_dispatchers(|b| {
        b.add::<AnyTierDispatcher>();
        b.add::<ContextTierDispatcher>();
        b.add::<VersionedContextTierDispatcher>();
        b.add::<ExactTierDispatcher>();
    });

    harness.assert_resolves_to_uri_for_schema(
        ResourceExtensionKind::Label,
        &harness.name_ref("tier"),
        &ExtensionRegistryHarness::schema("https://example.com/schemas/config/v1alpha1/Widget"),
        EXACT_TIER_SCHEMA_ID,
    );
    harness.assert_resolves_to_uri_for_schema(
        ResourceExtensionKind::Label,
        &harness.name_ref("tier"),
        &ExtensionRegistryHarness::schema("https://example.com/schemas/config/v1alpha1/Other"),
        VERSIONED_CONTEXT_TIER_SCHEMA_ID,
    );
    harness.assert_resolves_to_uri_for_schema(
        ResourceExtensionKind::Label,
        &harness.name_ref("tier"),
        &ExtensionRegistryHarness::schema("https://example.com/schemas/config/v2/Other"),
        CONTEXT_TIER_SCHEMA_ID,
    );
    harness.assert_resolves_to_uri_for_schema(
        ResourceExtensionKind::Label,
        &harness.name_ref("tier"),
        &ExtensionRegistryHarness::schema(
            "https://elsewhere.example/schemas/config/v1alpha1/Other",
        ),
        ANY_TIER_SCHEMA_ID,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_duplicate_schema_registration_fails_registry_construction() {
    let error = expect_registry_construction_error(|| {
        ExtensionRegistryHarness::try_with_test_dispatchers(|b| {
            b.add::<DuplicateEnvironmentDispatcher>();
        })
    });

    assert!(
        error
            .reason()
            .contains("Duplicate resource extension schema URI")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_duplicate_alias_registration_fails_registry_construction() {
    let error = expect_registry_construction_error(|| {
        ExtensionRegistryHarness::try_with_test_dispatchers(|b| {
            b.add::<AnyTierDispatcher>();
            b.add::<AnyTierAliasConflictDispatcher>();
        })
    });

    assert!(
        error
            .reason()
            .contains("Resource extension short-name conflict")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ExtensionRegistryHarness {
    registry: std::sync::Arc<ResourceExtensionSchemaRegistry>,
    default_schema: ResourceSchemaId,
}

impl ExtensionRegistryHarness {
    fn built_ins() -> Self {
        Self::with_test_dispatchers(|_| {})
    }

    fn with_test_dispatchers(register: impl FnOnce(&mut CatalogBuilder)) -> Self {
        Self::try_with_test_dispatchers(register).unwrap()
    }

    fn try_with_test_dispatchers(
        register: impl FnOnce(&mut CatalogBuilder),
    ) -> Result<Self, internal_error::InternalError> {
        let mut builder = CatalogBuilder::new();
        kamu_resources_services::register_dependencies(&mut builder);
        register(&mut builder);
        let catalog =
            kamu_resources_services::build_catalog_with_resource_extension_schema_registry(
                builder,
            )?;

        Ok(Self {
            registry: catalog.get_one().unwrap(),
            default_schema: Self::schema("https://example.com/schemas/config/v1alpha1/Widget"),
        })
    }

    fn schema(schema: &str) -> ResourceSchemaId {
        ResourceSchemaId::parse(schema).unwrap()
    }

    fn name_ref(&self, name: &str) -> TypeRef {
        TypeRef::Name(name.parse::<TypeName>().unwrap())
    }

    fn uri_ref(&self, uri: &str) -> TypeRef {
        TypeRef::Uri(TypeUri::new_unchecked(uri))
    }

    fn assert_resolves_to_uri(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        expected_uri: &str,
    ) {
        self.assert_resolves_to_uri_for_schema(kind, key, &self.default_schema, expected_uri);
    }

    fn assert_resolves_to_uri_for_schema(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        resource_schema: &ResourceSchemaId,
        expected_uri: &str,
    ) {
        let registration = self
            .registry
            .resolve(kind, key, resource_schema)
            .unwrap()
            .unwrap();

        assert_eq!(registration.schema_id().as_str(), expected_uri);
    }

    fn assert_resolve_error(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        expected: &ResourceExtensionResolutionError,
    ) {
        self.assert_resolve_error_for_schema(kind, key, &self.default_schema, expected);
    }

    fn assert_resolve_error_for_schema(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        resource_schema: &ResourceSchemaId,
        expected: &ResourceExtensionResolutionError,
    ) {
        let Err(error) = self.registry.resolve(kind, key, resource_schema) else {
            panic!("expected resolution error")
        };

        assert_eq!(&error, expected);
    }

    fn assert_value_valid(
        &self,
        kind: ResourceExtensionKind,
        uri: &str,
        value: &serde_json::Value,
    ) {
        let registration = self
            .registry
            .resolve(kind, &self.uri_ref(uri), &self.default_schema)
            .unwrap()
            .unwrap();

        registration.dispatcher().validate_value(value).unwrap();
    }

    fn assert_value_invalid(
        &self,
        kind: ResourceExtensionKind,
        uri: &str,
        value: &serde_json::Value,
        expected_message: &str,
    ) {
        let error = self.value_error(kind, uri, value);

        assert_eq!(error.message(), expected_message);
    }

    fn assert_value_invalid_contains(
        &self,
        kind: ResourceExtensionKind,
        uri: &str,
        value: &serde_json::Value,
        expected_message_part: &str,
    ) {
        let error = self.value_error(kind, uri, value);

        assert!(
            error.message().contains(expected_message_part),
            "actual message: {}",
            error.message()
        );
    }

    fn value_error(
        &self,
        kind: ResourceExtensionKind,
        uri: &str,
        value: &serde_json::Value,
    ) -> ResourceExtensionValueError {
        let registration = self
            .registry
            .resolve(kind, &self.uri_ref(uri), &self.default_schema)
            .unwrap()
            .unwrap();

        registration.dispatcher().validate_value(value).unwrap_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn expect_registry_construction_error(
    f: impl FnOnce() -> Result<ExtensionRegistryHarness, internal_error::InternalError>,
) -> internal_error::InternalError {
    match f() {
        Ok(_) => panic!("expected registry construction error"),
        Err(error) => error,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestStringValue;

impl ResourceValidateSchemaValue for TestStringValue {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        if value.is_string() {
            Ok(())
        } else {
            Err(ResourceExtensionValueError::new(
                "test value must be a string",
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const EXACT_ONLY_SCHEMA_ID: &str = "https://kamu.dev/schemas/resource/v1alpha1/labels/ExactOnly";
const EXACT_ONLY_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceType {
            schema: "https://example.com/schemas/config/v1alpha1/Widget",
        },
        preferred_name: Some("exactonly"),
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = ExactOnlyDispatcher,
    value = TestStringValue,
    schema_id = EXACT_ONLY_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = EXACT_ONLY_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ANY_TIER_SCHEMA_ID: &str = "https://kamu.dev/schemas/resource/v1alpha1/labels/AnyTier";
const ANY_TIER_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::AnyResource,
        preferred_name: Some("tier"),
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = AnyTierDispatcher,
    value = TestStringValue,
    schema_id = ANY_TIER_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = ANY_TIER_APPLICATIONS
);

const ANY_TIER_ALIAS_CONFLICT_SCHEMA_ID: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/AnyTierConflict";

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = AnyTierAliasConflictDispatcher,
    value = TestStringValue,
    schema_id = ANY_TIER_ALIAS_CONFLICT_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = ANY_TIER_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CONTEXT_TIER_SCHEMA_ID: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/ContextTier";
const CONTEXT_TIER_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceContext {
            authority: "https://example.com/schemas",
            context: "config",
            version: None,
        },
        preferred_name: Some("tier"),
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = ContextTierDispatcher,
    value = TestStringValue,
    schema_id = CONTEXT_TIER_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = CONTEXT_TIER_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const VERSIONED_CONTEXT_TIER_SCHEMA_ID: &str =
    "https://kamu.dev/schemas/resource/v1alpha1/labels/VersionedContextTier";
const VERSIONED_CONTEXT_TIER_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceContext {
            authority: "https://example.com/schemas",
            context: "config",
            version: Some("v1alpha1"),
        },
        preferred_name: Some("tier"),
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = VersionedContextTierDispatcher,
    value = TestStringValue,
    schema_id = VERSIONED_CONTEXT_TIER_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = VERSIONED_CONTEXT_TIER_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const EXACT_TIER_SCHEMA_ID: &str = "https://kamu.dev/schemas/resource/v1alpha1/labels/ExactTier";
const EXACT_TIER_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceType {
            schema: "https://example.com/schemas/config/v1alpha1/Widget",
        },
        preferred_name: Some("tier"),
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = ExactTierDispatcher,
    value = TestStringValue,
    schema_id = EXACT_TIER_SCHEMA_ID,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = EXACT_TIER_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = DuplicateEnvironmentDispatcher,
    value = TestStringValue,
    schema_id = RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    kind = ResourceExtensionKind::Label,
    document = "{}",
    applications = ANY_TIER_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
