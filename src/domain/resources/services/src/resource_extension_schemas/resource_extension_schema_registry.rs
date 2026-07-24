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

use dill::{BuilderExt, Catalog};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionSchemaDispatcher,
    ResourceExtensionSchemaMeta,
    ResourceExtensionScope,
    ResourceExtensionScopeMeta,
    ResourceSchemaId,
    TypeName,
    TypeRef,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResourceExtensionSchemaRegistration {
    schema_id: ResourceSchemaId,
    kind: ResourceExtensionKind,
    applications: Vec<ResourceExtensionApplication>,
    document: &'static str,
    dispatcher: Arc<dyn ResourceExtensionSchemaDispatcher>,
}

impl ResourceExtensionSchemaRegistration {
    pub fn schema_id(&self) -> &ResourceSchemaId {
        &self.schema_id
    }

    pub fn kind(&self) -> ResourceExtensionKind {
        self.kind
    }

    pub fn document(&self) -> &'static str {
        self.document
    }

    pub fn dispatcher(&self) -> &Arc<dyn ResourceExtensionSchemaDispatcher> {
        &self.dispatcher
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceExtensionApplication {
    pub scope: ResourceExtensionScope,
    pub preferred_name: Option<TypeName>,
    pub aliases: Vec<TypeName>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceExtensionResolutionError {
    #[error("unknown {kind:?} extension URI '{uri}'")]
    UnknownUri {
        kind: ResourceExtensionKind,
        uri: TypeUri,
    },

    #[error(
        "extension URI '{uri}' is registered as {actual_kind:?}, but was used as {expected_kind:?}"
    )]
    KindMismatch {
        uri: TypeUri,
        expected_kind: ResourceExtensionKind,
        actual_kind: ResourceExtensionKind,
    },

    #[error("extension URI '{uri}' is not applicable to resource schema '{resource_schema}'")]
    Inapplicable {
        uri: TypeUri,
        resource_schema: TypeUri,
    },

    #[error("{kind:?} extension '{authored_key}' value is invalid: {reason}")]
    InvalidValue {
        kind: ResourceExtensionKind,
        authored_key: TypeRef,
        canonical_uri: TypeUri,
        reason: String,
    },

    #[error(
        "{kind:?} extension keys {authored_keys:?} resolve to duplicate canonical key \
         '{canonical_key}'"
    )]
    DuplicateAfterCanonicalization {
        kind: ResourceExtensionKind,
        canonical_key: TypeRef,
        authored_keys: Vec<TypeRef>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResourceExtensionSchemaRegistry {
    by_uri: BTreeMap<TypeUri, Arc<ResourceExtensionSchemaRegistration>>,
    exact_type_names: BTreeMap<ScopedNameKey, Arc<ResourceExtensionSchemaRegistration>>,
    versioned_context_names: BTreeMap<ScopedNameKey, Arc<ResourceExtensionSchemaRegistration>>,
    context_names: BTreeMap<ScopedNameKey, Arc<ResourceExtensionSchemaRegistration>>,
    any_resource_names: BTreeMap<AnyNameKey, Arc<ResourceExtensionSchemaRegistration>>,
}

impl ResourceExtensionSchemaRegistry {
    pub fn try_new(catalog: &Catalog) -> Result<Self, InternalError> {
        let mut registry = Self {
            by_uri: BTreeMap::new(),
            exact_type_names: BTreeMap::new(),
            versioned_context_names: BTreeMap::new(),
            context_names: BTreeMap::new(),
            any_resource_names: BTreeMap::new(),
        };

        for builder in catalog.builders_for_with_meta::<dyn ResourceExtensionSchemaDispatcher, _>(
            |_: &ResourceExtensionSchemaMeta| true,
        ) {
            let meta = builder
                .metadata_get_first::<ResourceExtensionSchemaMeta>()
                .ok_or_else(|| {
                    InternalError::new(
                        "Resource extension schema dispatcher is missing registry metadata",
                    )
                })?;

            let dispatcher = builder.get(catalog).int_err()?;
            let registration = Arc::new(Self::parse_registration(meta, dispatcher)?);

            if registry
                .by_uri
                .insert(registration.schema_id.typ().clone(), registration.clone())
                .is_some()
            {
                return Err(InternalError::new(format!(
                    "Duplicate resource extension schema URI '{}'",
                    registration.schema_id
                )));
            }

            registry.index_names(&registration)?;
        }

        Ok(registry)
    }

    pub fn registrations(&self) -> impl Iterator<Item = &Arc<ResourceExtensionSchemaRegistration>> {
        self.by_uri.values()
    }

    pub fn resolve(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        resource_schema: &ResourceSchemaId,
    ) -> Result<Option<Arc<ResourceExtensionSchemaRegistration>>, ResourceExtensionResolutionError>
    {
        match key {
            TypeRef::Uri(uri) => self.resolve_uri(kind, uri, resource_schema).map(Some),
            TypeRef::Name(name) => Ok(self.resolve_name(kind, name, resource_schema)),
        }
    }

    pub fn resolve_for_trusted(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        resource_schema: &ResourceSchemaId,
    ) -> Result<Arc<ResourceExtensionSchemaRegistration>, InternalError> {
        self.resolve(kind, key, resource_schema)
            .map_err(|err| InternalError::new(err.to_string()))?
            .ok_or_else(|| {
                InternalError::new(format!(
                    "Trusted {kind:?} extension key '{key}' is not registered"
                ))
            })
    }

    fn parse_registration(
        meta: &ResourceExtensionSchemaMeta,
        dispatcher: Arc<dyn ResourceExtensionSchemaDispatcher>,
    ) -> Result<ResourceExtensionSchemaRegistration, InternalError> {
        ensure_https_uri(meta.schema_id, "resource extension schema URI")?;

        let schema_id = ResourceSchemaId::parse(meta.schema_id).map_err(|err| {
            InternalError::new(format!(
                "Invalid resource extension schema URI '{}': {err}",
                meta.schema_id
            ))
        })?;

        let applications = meta
            .applications
            .iter()
            .map(Self::parse_application)
            .collect::<Result<Vec<_>, _>>()?;

        if applications.is_empty() {
            return Err(InternalError::new(format!(
                "Resource extension schema '{}' has no applications",
                meta.schema_id
            )));
        }

        Ok(ResourceExtensionSchemaRegistration {
            schema_id,
            kind: meta.kind,
            applications,
            document: meta.document,
            dispatcher,
        })
    }

    fn parse_application(
        meta: &ResourceExtensionApplicationMeta,
    ) -> Result<ResourceExtensionApplication, InternalError> {
        let preferred_name = meta.preferred_name.map(parse_type_name).transpose()?;
        let aliases = meta
            .aliases
            .iter()
            .copied()
            .map(parse_type_name)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ResourceExtensionApplication {
            scope: parse_scope_meta(&meta.scope)?,
            preferred_name,
            aliases,
        })
    }

    fn index_names(
        &mut self,
        registration: &Arc<ResourceExtensionSchemaRegistration>,
    ) -> Result<(), InternalError> {
        for application in &registration.applications {
            let names = application
                .preferred_name
                .iter()
                .chain(application.aliases.iter());

            for name in names {
                match &application.scope {
                    ResourceExtensionScope::AnyResource => {
                        insert_alias(
                            &mut self.any_resource_names,
                            AnyNameKey {
                                kind: registration.kind,
                                name: name.clone(),
                            },
                            registration,
                        )?;
                    }
                    ResourceExtensionScope::ResourceContext {
                        authority,
                        context,
                        version: Some(version),
                    } => {
                        insert_alias(
                            &mut self.versioned_context_names,
                            ScopedNameKey {
                                kind: registration.kind,
                                authority: authority.clone(),
                                context: context.clone(),
                                version: Some(version.clone()),
                                schema: None,
                                name: name.clone(),
                            },
                            registration,
                        )?;
                    }
                    ResourceExtensionScope::ResourceContext {
                        authority,
                        context,
                        version: None,
                    } => {
                        insert_alias(
                            &mut self.context_names,
                            ScopedNameKey {
                                kind: registration.kind,
                                authority: authority.clone(),
                                context: context.clone(),
                                version: None,
                                schema: None,
                                name: name.clone(),
                            },
                            registration,
                        )?;
                    }
                    ResourceExtensionScope::ResourceType { schema } => {
                        insert_alias(
                            &mut self.exact_type_names,
                            ScopedNameKey {
                                kind: registration.kind,
                                authority: String::new(),
                                context: String::new(),
                                version: None,
                                schema: Some(schema.as_str().to_string()),
                                name: name.clone(),
                            },
                            registration,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn resolve_uri(
        &self,
        kind: ResourceExtensionKind,
        uri: &TypeUri,
        resource_schema: &ResourceSchemaId,
    ) -> Result<Arc<ResourceExtensionSchemaRegistration>, ResourceExtensionResolutionError> {
        let Some(registration) = self.by_uri.get(uri) else {
            return Err(ResourceExtensionResolutionError::UnknownUri {
                kind,
                uri: uri.clone(),
            });
        };

        if registration.kind != kind {
            return Err(ResourceExtensionResolutionError::KindMismatch {
                uri: uri.clone(),
                expected_kind: kind,
                actual_kind: registration.kind,
            });
        }

        if !registration_applies_to(registration, resource_schema) {
            return Err(ResourceExtensionResolutionError::Inapplicable {
                uri: uri.clone(),
                resource_schema: resource_schema.typ().clone(),
            });
        }

        Ok(registration.clone())
    }

    fn resolve_name(
        &self,
        kind: ResourceExtensionKind,
        name: &TypeName,
        resource_schema: &ResourceSchemaId,
    ) -> Option<Arc<ResourceExtensionSchemaRegistration>> {
        self.exact_type_names
            .get(&ScopedNameKey {
                kind,
                authority: String::new(),
                context: String::new(),
                version: None,
                schema: Some(resource_schema.as_str().to_string()),
                name: name.clone(),
            })
            .or_else(|| {
                self.versioned_context_names.get(&ScopedNameKey {
                    kind,
                    authority: resource_schema.base().to_string(),
                    context: resource_schema.context().to_string(),
                    version: Some(resource_schema.version().to_string()),
                    schema: None,
                    name: name.clone(),
                })
            })
            .or_else(|| {
                self.context_names.get(&ScopedNameKey {
                    kind,
                    authority: resource_schema.base().to_string(),
                    context: resource_schema.context().to_string(),
                    version: None,
                    schema: None,
                    name: name.clone(),
                })
            })
            .or_else(|| {
                self.any_resource_names.get(&AnyNameKey {
                    kind,
                    name: name.clone(),
                })
            })
            .cloned()
            .filter(|registration| registration_applies_to(registration, resource_schema))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AnyNameKey {
    kind: ResourceExtensionKind,
    name: TypeName,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ScopedNameKey {
    kind: ResourceExtensionKind,
    authority: String,
    context: String,
    version: Option<String>,
    schema: Option<String>,
    name: TypeName,
}

fn insert_alias<TKey>(
    aliases: &mut BTreeMap<TKey, Arc<ResourceExtensionSchemaRegistration>>,
    key: TKey,
    registration: &Arc<ResourceExtensionSchemaRegistration>,
) -> Result<(), InternalError>
where
    TKey: Ord,
{
    if let Some(existing) = aliases.insert(key, Arc::clone(registration))
        && existing.schema_id != registration.schema_id
    {
        return Err(InternalError::new(format!(
            "Resource extension short-name conflict between '{}' and '{}'",
            existing.schema_id, registration.schema_id
        )));
    }

    Ok(())
}

fn parse_scope_meta(
    meta: &ResourceExtensionScopeMeta,
) -> Result<ResourceExtensionScope, InternalError> {
    match meta {
        ResourceExtensionScopeMeta::AnyResource => Ok(ResourceExtensionScope::AnyResource),
        ResourceExtensionScopeMeta::ResourceContext {
            authority,
            context,
            version,
        } => {
            ensure_https_uri(authority, "resource extension context authority")?;
            Ok(ResourceExtensionScope::ResourceContext {
                authority: (*authority).to_string(),
                context: (*context).to_string(),
                version: version.map(str::to_string),
            })
        }
        ResourceExtensionScopeMeta::ResourceType { schema } => {
            ensure_https_uri(schema, "resource extension resource-type scope")?;
            Ok(ResourceExtensionScope::ResourceType {
                schema: ResourceSchemaId::parse(schema).map_err(|err| {
                    InternalError::new(format!(
                        "Invalid resource extension resource-type scope '{schema}': {err}"
                    ))
                })?,
            })
        }
    }
}

fn parse_type_name(name: &'static str) -> Result<TypeName, InternalError> {
    name.parse::<TypeName>().map_err(|err| {
        InternalError::new(format!(
            "Invalid resource extension short name '{name}': {err}"
        ))
    })
}

fn ensure_https_uri(value: &str, what: &str) -> Result<(), InternalError> {
    if value.starts_with("https://") {
        Ok(())
    } else {
        Err(InternalError::new(format!(
            "Invalid {what} '{value}': only https:// URIs are supported"
        )))
    }
}

fn registration_applies_to(
    registration: &ResourceExtensionSchemaRegistration,
    resource_schema: &ResourceSchemaId,
) -> bool {
    registration
        .applications
        .iter()
        .any(|application| scope_applies_to(&application.scope, resource_schema))
}

fn scope_applies_to(scope: &ResourceExtensionScope, resource_schema: &ResourceSchemaId) -> bool {
    match scope {
        ResourceExtensionScope::AnyResource => true,
        ResourceExtensionScope::ResourceContext {
            authority,
            context,
            version,
        } => {
            authority == resource_schema.base()
                && context == resource_schema.context()
                && version
                    .as_deref()
                    .is_none_or(|version| version == resource_schema.version())
        }
        ResourceExtensionScope::ResourceType { schema } => schema == resource_schema,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
