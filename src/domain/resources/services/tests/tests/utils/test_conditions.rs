// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::{
    ResourceConditionSchemaMeta,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
    ResourceExtensionValueError,
    ResourceValidateSchemaValue,
    TypeRef,
    TypeUri,
};

use crate::tests::utils::TestResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A string condition, like a task's status: small and carried in messages
pub const TEST_STAGE_CONDITION_URI: &str =
    "https://test.kamu.dev/schemas/test/v1/conditions/TestStage";

pub const TEST_STAGE_MAX_SERIALIZED_SIZE: usize = 64;

pub fn test_stage_condition_key() -> TypeRef {
    TypeRef::Uri(TypeUri::new_unchecked(TEST_STAGE_CONDITION_URI))
}

/// An object condition, like a task's plan: larger, and named by key only in
/// messages
pub const TEST_PAYLOAD_CONDITION_URI: &str =
    "https://test.kamu.dev/schemas/test/v1/conditions/TestPayload";

pub const TEST_PAYLOAD_MAX_SERIALIZED_SIZE: usize = 1024;

/// A condition registered for another resource type only
pub const TEST_OTHER_TYPE_CONDITION_URI: &str =
    "https://test.kamu.dev/schemas/test/v1/conditions/TestOtherType";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_RESOURCE_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceType {
            schema: TestResource::SCHEMA_STR,
        },
        preferred_name: None,
        aliases: &[],
    }];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestStageCondition;

impl ResourceValidateSchemaValue for TestStageCondition {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        if value.is_string() {
            Ok(())
        } else {
            Err(ResourceExtensionValueError::new("stage must be a string"))
        }
    }
}

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = TestStageConditionDispatcher,
    value = TestStageCondition,
    schema_id = TEST_STAGE_CONDITION_URI,
    kind = ResourceExtensionKind::Condition,
    document = None,
    applications = TEST_RESOURCE_APPLICATIONS,
    condition = Some(ResourceConditionSchemaMeta {
        max_serialized_size: TEST_STAGE_MAX_SERIALIZED_SIZE,
        carried: true,
    })
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestPayloadCondition;

impl ResourceValidateSchemaValue for TestPayloadCondition {
    type ValidationError = ResourceExtensionValueError;

    fn validate(value: &serde_json::Value) -> Result<(), Self::ValidationError> {
        if value.is_object() {
            Ok(())
        } else {
            Err(ResourceExtensionValueError::new(
                "payload must be an object",
            ))
        }
    }
}

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = TestPayloadConditionDispatcher,
    value = TestPayloadCondition,
    schema_id = TEST_PAYLOAD_CONDITION_URI,
    kind = ResourceExtensionKind::Condition,
    document = None,
    applications = TEST_RESOURCE_APPLICATIONS,
    condition = Some(ResourceConditionSchemaMeta {
        max_serialized_size: TEST_PAYLOAD_MAX_SERIALIZED_SIZE,
        carried: false,
    })
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const OTHER_TYPE_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceType {
            schema: "https://test.kamu.dev/schemas/test/v1/OtherResource",
        },
        preferred_name: None,
        aliases: &[],
    }];

kamu_resources_services::declare_resource_extension_schema_dispatcher!(
    dispatcher = TestOtherTypeConditionDispatcher,
    value = TestStageCondition,
    schema_id = TEST_OTHER_TYPE_CONDITION_URI,
    kind = ResourceExtensionKind::Condition,
    document = None,
    applications = OTHER_TYPE_APPLICATIONS,
    condition = Some(ResourceConditionSchemaMeta {
        max_serialized_size: TEST_STAGE_MAX_SERIALIZED_SIZE,
        carried: true,
    })
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_test_condition_schema_dispatchers(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<TestStageConditionDispatcher>();
    catalog_builder.add::<TestPayloadConditionDispatcher>();
    catalog_builder.add::<TestOtherTypeConditionDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
