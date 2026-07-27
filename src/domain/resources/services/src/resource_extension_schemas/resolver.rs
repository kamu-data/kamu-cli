// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use kamu_resources::{
    ResourceExtensionKind,
    ResourceExtensionResolutionError,
    ResourceSchemaId,
    ResourceWarning,
    TypeRef,
    TypeUri,
    resource_free_form_extension_warning,
};

use crate::ResourceExtensionSchemaRegistry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceExtensionKeyResolution {
    pub authored_key: TypeRef,
    pub canonical_key: TypeRef,
    pub canonical_uri: Option<TypeUri>,
    pub is_free_form: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct CanonicalResourceExtensionEntries {
    pub entries: Vec<(TypeRef, serde_json::Value)>,
    pub warnings: Vec<ResourceWarning>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResourceExtensionSchemaResolver {
    registry: Arc<ResourceExtensionSchemaRegistry>,
}

#[dill::component(pub)]
impl ResourceExtensionSchemaResolver {
    pub fn new(registry: Arc<ResourceExtensionSchemaRegistry>) -> Self {
        Self { registry }
    }

    pub fn resolve_key(
        &self,
        kind: ResourceExtensionKind,
        authored_key: &TypeRef,
        value: &serde_json::Value,
        resource_schema: &ResourceSchemaId,
    ) -> Result<ResourceExtensionKeyResolution, ResourceExtensionResolutionError> {
        let Some(registration) = self.registry.resolve(kind, authored_key, resource_schema)? else {
            return Ok(ResourceExtensionKeyResolution {
                authored_key: authored_key.clone(),
                canonical_key: authored_key.clone(),
                canonical_uri: None,
                is_free_form: true,
            });
        };

        let canonical_uri = registration.schema_id().typ().clone();

        registration
            .dispatcher()
            .validate_value(value)
            .map_err(|err| ResourceExtensionResolutionError::InvalidValue {
                kind,
                authored_key: authored_key.clone(),
                canonical_uri: canonical_uri.clone(),
                reason: err.message().to_string(),
            })?;

        Ok(ResourceExtensionKeyResolution {
            authored_key: authored_key.clone(),
            canonical_key: TypeRef::Uri(canonical_uri.clone()),
            canonical_uri: Some(canonical_uri),
            is_free_form: false,
        })
    }

    pub fn canonicalize_entries(
        &self,
        kind: ResourceExtensionKind,
        entries: Vec<(TypeRef, serde_json::Value)>,
        resource_schema: &ResourceSchemaId,
    ) -> Result<CanonicalResourceExtensionEntries, ResourceExtensionResolutionError> {
        let mut canonical_entries = Vec::with_capacity(entries.len());
        let mut seen_keys: BTreeMap<TypeRef, TypeRef> = BTreeMap::new();
        let mut free_form_keys = Vec::new();

        for (authored_key, value) in entries {
            let resolution = self.resolve_key(kind, &authored_key, &value, resource_schema)?;
            if let Some(first_authored_key) = seen_keys.insert(
                resolution.canonical_key.clone(),
                resolution.authored_key.clone(),
            ) {
                return Err(
                    ResourceExtensionResolutionError::DuplicateAfterCanonicalization {
                        kind,
                        canonical_key: resolution.canonical_key,
                        authored_keys: vec![first_authored_key, resolution.authored_key],
                    },
                );
            }

            if resolution.is_free_form {
                free_form_keys.push(resolution.authored_key.to_string());
            }

            canonical_entries.push((resolution.canonical_key, value));
        }

        Ok(CanonicalResourceExtensionEntries {
            entries: canonical_entries,
            warnings: make_free_form_warnings(kind, free_form_keys),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_free_form_warnings(kind: ResourceExtensionKind, keys: Vec<String>) -> Vec<ResourceWarning> {
    resource_free_form_extension_warning(kind, keys)
        .into_iter()
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
