// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use crate::ResourceExtensionSchemaRegistry;
use crate::domain::{
    ResourceDurableStateValidationError,
    ResourceExtensionKind,
    ResourceHeaders,
    ResourceSchemaId,
    ResourceStatus,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The parts of a resource that carry extension entries
#[derive(Debug, Clone, Copy)]
pub struct ResourceExtensionState<'a> {
    pub headers: &'a ResourceHeaders,
    pub status: &'a ResourceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceDurableStateValidator<'a> {
    extension_schema_registry: &'a ResourceExtensionSchemaRegistry,
}

impl<'a> ResourceDurableStateValidator<'a> {
    pub fn new(extension_schema_registry: &'a ResourceExtensionSchemaRegistry) -> Self {
        Self {
            extension_schema_registry,
        }
    }

    /// Checks the labels, annotations and conditions a write adds or changes
    /// against `stored`, the state last saved. An entry equal to the stored
    /// one is not checked again, so a schema change never blocks a write that
    /// leaves the entry alone. Without a stored state every entry is checked
    pub fn validate_changes(
        &self,
        schema: &TypeUri,
        state: ResourceExtensionState<'_>,
        stored: Option<ResourceExtensionState<'_>>,
    ) -> Result<(), ResourceDurableStateValidationError> {
        let labels = changed_entries(
            &state.headers.labels.entries,
            stored.map(|stored| &stored.headers.labels.entries),
        );
        let annotations = changed_entries(
            &state.headers.annotations.entries,
            stored.map(|stored| &stored.headers.annotations.entries),
        );
        let conditions = changed_entries(
            &state.status.conditions.entries,
            stored.map(|stored| &stored.status.conditions.entries),
        );
        if labels.is_empty() && annotations.is_empty() && conditions.is_empty() {
            return Ok(());
        }

        let resource_schema = ResourceSchemaId::parse(schema.as_str())?;
        self.validate_header_extensions(ResourceExtensionKind::Label, &labels, &resource_schema)?;
        self.validate_header_extensions(
            ResourceExtensionKind::Annotation,
            &annotations,
            &resource_schema,
        )?;
        self.validate_conditions(&conditions, &resource_schema)
    }

    fn validate_header_extensions(
        &self,
        kind: ResourceExtensionKind,
        entries: &[(&TypeRef, &serde_json::Value)],
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceDurableStateValidationError> {
        for &(key, value) in entries {
            match key {
                TypeRef::Uri(uri) => {
                    let registration = self
                        .extension_schema_registry
                        .resolve_uri(kind, uri, resource_schema)
                        .map_err(ResourceDurableStateValidationError::ExtensionResolution)?;

                    registration
                        .dispatcher()
                        .validate_value(value)
                        .map_err(|err| {
                            ResourceDurableStateValidationError::InvalidExtensionValue {
                                kind,
                                key: key.clone(),
                                error: err,
                            }
                        })?;
                }
                TypeRef::Name(_) => {
                    if self
                        .extension_schema_registry
                        .resolve(kind, key, resource_schema)
                        .map_err(ResourceDurableStateValidationError::ExtensionResolution)?
                        .is_some()
                    {
                        return Err(
                            ResourceDurableStateValidationError::NonCanonicalExtensionKey {
                                kind,
                                key: key.clone(),
                            },
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_conditions(
        &self,
        entries: &[(&TypeRef, &serde_json::Value)],
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceDurableStateValidationError> {
        for &(key, value) in entries {
            let TypeRef::Uri(uri) = key else {
                return Err(
                    ResourceDurableStateValidationError::NonCanonicalExtensionKey {
                        kind: ResourceExtensionKind::Condition,
                        key: key.clone(),
                    },
                );
            };

            let registration = self
                .extension_schema_registry
                .resolve_uri(ResourceExtensionKind::Condition, uri, resource_schema)
                .map_err(ResourceDurableStateValidationError::ExtensionResolution)?;

            registration
                .dispatcher()
                .validate_value(value)
                .map_err(
                    |err| ResourceDurableStateValidationError::InvalidExtensionValue {
                        kind: ResourceExtensionKind::Condition,
                        key: key.clone(),
                        error: err,
                    },
                )?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn changed_entries<'a>(
    entries: &'a BTreeMap<TypeRef, serde_json::Value>,
    stored: Option<&BTreeMap<TypeRef, serde_json::Value>>,
) -> Vec<(&'a TypeRef, &'a serde_json::Value)> {
    entries
        .iter()
        .filter(|(key, value)| stored.and_then(|stored| stored.get(*key)) != Some(*value))
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
