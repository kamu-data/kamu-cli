// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceExtensionSchemaRegistry;
use crate::domain::{
    RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI,
    ResourceConditionPatchChange,
    ResourceConditionPatchError,
    ResourceConditionsPatch,
    ResourceExtensionKind,
    ResourceSchemaId,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checks a condition patch against the conditions registered for a resource
/// type, before any of it is applied
pub(crate) struct ResourceConditionPatchChecker<'a> {
    extension_schema_registry: &'a ResourceExtensionSchemaRegistry,
}

impl<'a> ResourceConditionPatchChecker<'a> {
    pub fn new(extension_schema_registry: &'a ResourceExtensionSchemaRegistry) -> Self {
        Self {
            extension_schema_registry,
        }
    }

    /// A set condition must be registered for the type, valid and within its
    /// size limit. A removed one only needs to be outside the framework's own
    /// conditions, so a condition whose schema is no longer registered can
    /// still be removed
    pub fn check(
        &self,
        patch: &ResourceConditionsPatch,
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceConditionPatchError> {
        for (key, change) in patch.changes() {
            if key.as_str() == RESOURCE_CONDITION_RECONCILIATION_ERROR_SCHEMA_URI {
                return Err(ResourceConditionPatchError::FrameworkOwned { key: key.clone() });
            }

            match change {
                ResourceConditionPatchChange::Set(value) => {
                    self.check_set(key, value, resource_schema)?;
                }
                ResourceConditionPatchChange::Remove => {}
            }
        }

        Ok(())
    }

    fn check_set(
        &self,
        key: &TypeUri,
        value: &serde_json::Value,
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceConditionPatchError> {
        let registration = self.extension_schema_registry.resolve_uri(
            ResourceExtensionKind::Condition,
            key,
            resource_schema,
        )?;

        registration
            .dispatcher()
            .validate_value(value)
            .map_err(|error| ResourceConditionPatchError::InvalidValue {
                key: key.clone(),
                error,
            })?;

        let condition = registration
            .condition()
            .expect("the registry requires condition metadata of every condition schema");

        // The compact JSON a status event stores
        let size = value.to_string().len();
        if size > condition.max_serialized_size {
            return Err(ResourceConditionPatchError::TooLarge {
                key: key.clone(),
                size,
                max: condition.max_serialized_size,
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
