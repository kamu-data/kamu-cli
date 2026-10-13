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
    ResourceDurableStateValidationError,
    ResourceExtensionKind,
    ResourceSchemaId,
    ResourceSnapshot,
    TypeRef,
};

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

    pub fn validate_snapshot(
        &self,
        snapshot: &ResourceSnapshot,
    ) -> Result<(), ResourceDurableStateValidationError> {
        let resource_schema = ResourceSchemaId::parse(snapshot.schema.as_str())?;

        self.validate_header_extensions(
            ResourceExtensionKind::Label,
            &snapshot.headers.labels.entries,
            &resource_schema,
        )?;
        self.validate_header_extensions(
            ResourceExtensionKind::Annotation,
            &snapshot.headers.annotations.entries,
            &resource_schema,
        )?;

        if let Some(status) = &snapshot.status {
            self.validate_conditions(&status.conditions.entries, &resource_schema)?;
        }

        Ok(())
    }

    fn validate_header_extensions(
        &self,
        kind: ResourceExtensionKind,
        entries: &std::collections::BTreeMap<TypeRef, serde_json::Value>,
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceDurableStateValidationError> {
        for (key, value) in entries {
            match key {
                TypeRef::Uri(_) => {
                    let registration = self
                        .extension_schema_registry
                        .resolve(kind, key, resource_schema)
                        .map_err(ResourceDurableStateValidationError::ExtensionResolution)?
                        .unwrap_or_else(|| {
                            unreachable!(
                                "URI extension resolution must return a registration or an error"
                            )
                        });

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
        entries: &std::collections::BTreeMap<TypeRef, serde_json::Value>,
        resource_schema: &ResourceSchemaId,
    ) -> Result<(), ResourceDurableStateValidationError> {
        for (key, value) in entries {
            let TypeRef::Uri(_) = key else {
                return Err(
                    ResourceDurableStateValidationError::NonCanonicalExtensionKey {
                        kind: ResourceExtensionKind::Condition,
                        key: key.clone(),
                    },
                );
            };

            let registration = self
                .extension_schema_registry
                .resolve(ResourceExtensionKind::Condition, key, resource_schema)
                .map_err(ResourceDurableStateValidationError::ExtensionResolution)?
                .unwrap_or_else(|| {
                    unreachable!("URI extension resolution must return a registration or an error")
                });

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
