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

pub fn register_test_condition_schema_dispatchers(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<TestStageConditionDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
