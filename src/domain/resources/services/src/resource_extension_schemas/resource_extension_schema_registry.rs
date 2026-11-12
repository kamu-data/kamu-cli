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
    ResourceConditionSchemaMeta,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionResolutionError,
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
    document: Option<&'static str>,
    condition: Option<ResourceConditionSchemaMeta>,
    dispatcher: Arc<dyn ResourceExtensionSchemaDispatcher>,
}

impl ResourceExtensionSchemaRegistration {
    pub fn schema_id(&self) -> &ResourceSchemaId {
        &self.schema_id
    }

    pub fn kind(&self) -> ResourceExtensionKind {
        self.kind
    }

    pub fn document(&self) -> Option<&'static str> {
        self.document
    }

    /// Present exactly when the schema is a condition
    pub fn condition(&self) -> Option<&ResourceConditionSchemaMeta> {
        self.condition.as_ref()
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

#[derive(Clone)]
pub struct ResourceExtensionSchemaRegistry {
    by_uri: BTreeMap<TypeUri, Arc<ResourceExtensionSchemaRegistration>>,
    /// Every scope each short name is declared in, so a lookup by name builds
    /// no key
    by_name: BTreeMap<TypeName, Vec<ShortNameDeclaration>>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl ResourceExtensionSchemaRegistry {
    pub fn new(catalog: &Catalog) -> Self {
        Self::try_new(catalog).unwrap()
    }

    pub fn try_new(catalog: &Catalog) -> Result<Self, InternalError> {
        let mut registry = Self {
            by_uri: BTreeMap::new(),
            by_name: BTreeMap::new(),
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

    pub fn resolve(
        &self,
        kind: ResourceExtensionKind,
        key: &TypeRef,
        resource_schema: &ResourceSchemaId,
    ) -> Result<Option<Arc<ResourceExtensionSchemaRegistration>>, ResourceExtensionResolutionError>
    {
        match key {
            TypeRef::Uri(uri) => self
                .resolve_uri(kind, uri, resource_schema)
                .map(|registration| Some(Arc::clone(registration))),
            TypeRef::Name(name) => Ok(self.resolve_name(kind, name, resource_schema)),
        }
    }

    /// The metadata of the condition registered under `uri`, or `None` when no
    /// condition is. A map lookup only: unlike [`Self::resolve`], it does not
    /// check that the condition applies to a resource type
    pub fn condition_meta(&self, uri: &TypeUri) -> Option<&ResourceConditionSchemaMeta> {
        self.by_uri
            .get(uri)
            .and_then(|registration| registration.condition.as_ref())
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

        Self::check_condition_meta(meta)?;

        Ok(ResourceExtensionSchemaRegistration {
            schema_id,
            kind: meta.kind,
            applications,
            document: meta.document,
            condition: meta.condition,
            dispatcher,
        })
    }

    fn check_condition_meta(meta: &ResourceExtensionSchemaMeta) -> Result<(), InternalError> {
        match (meta.kind, meta.condition) {
            (ResourceExtensionKind::Condition, None) => Err(InternalError::new(format!(
                "Resource condition schema '{}' declares no condition metadata",
                meta.schema_id
            ))),
            (ResourceExtensionKind::Condition, Some(condition))
                if condition.max_serialized_size == 0 =>
            {
                Err(InternalError::new(format!(
                    "Resource condition schema '{}' declares a zero size limit",
                    meta.schema_id
                )))
            }
            (ResourceExtensionKind::Label | ResourceExtensionKind::Annotation, Some(_)) => {
                Err(InternalError::new(format!(
                    "Resource extension schema '{}' declares condition metadata but is not a \
                     condition",
                    meta.schema_id
                )))
            }
            (ResourceExtensionKind::Condition, Some(_))
            | (ResourceExtensionKind::Label | ResourceExtensionKind::Annotation, None) => Ok(()),
        }
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
                let declarations = self.by_name.entry(name.clone()).or_default();
                if let Some(existing) = declarations.iter().find(|declaration| {
                    declaration.registration.kind == registration.kind
                        && declaration.scope == application.scope
                }) {
                    if existing.registration.schema_id != registration.schema_id {
                        return Err(InternalError::new(format!(
                            "Resource extension short-name conflict between '{}' and '{}'",
                            existing.registration.schema_id, registration.schema_id
                        )));
                    }
                    continue;
                }

                declarations.push(ShortNameDeclaration {
                    scope: application.scope.clone(),
                    registration: Arc::clone(registration),
                });
            }
        }

        Ok(())
    }

    /// Resolves a canonical key, which must be registered with `kind` and
    /// apply to `resource_schema`
    pub fn resolve_uri(
        &self,
        kind: ResourceExtensionKind,
        uri: &TypeUri,
        resource_schema: &ResourceSchemaId,
    ) -> Result<&Arc<ResourceExtensionSchemaRegistration>, ResourceExtensionResolutionError> {
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

        Ok(registration)
    }

    fn resolve_name(
        &self,
        kind: ResourceExtensionKind,
        name: &TypeName,
        resource_schema: &ResourceSchemaId,
    ) -> Option<Arc<ResourceExtensionSchemaRegistration>> {
        self.by_name
            .get(name)?
            .iter()
            .filter(|declaration| declaration.registration.kind == kind)
            .filter_map(|declaration| {
                ShortNamePrecedence::of(&declaration.scope, resource_schema)
                    .map(|precedence| (precedence, &declaration.registration))
            })
            .min_by_key(|(precedence, _)| *precedence)
            .map(|(_, registration)| Arc::clone(registration))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct ShortNameDeclaration {
    scope: ResourceExtensionScope,
    registration: Arc<ResourceExtensionSchemaRegistration>,
}

/// Which declaration of a short name wins for a resource type: the narrowest
/// scope that applies to it
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShortNamePrecedence {
    ResourceType,
    VersionedContext,
    Context,
    AnyResource,
}

impl ShortNamePrecedence {
    fn of(scope: &ResourceExtensionScope, resource_schema: &ResourceSchemaId) -> Option<Self> {
        if !scope_applies_to(scope, resource_schema) {
            return None;
        }

        Some(match scope {
            ResourceExtensionScope::ResourceType { .. } => Self::ResourceType,
            ResourceExtensionScope::ResourceContext {
                version: Some(_), ..
            } => Self::VersionedContext,
            ResourceExtensionScope::ResourceContext { version: None, .. } => Self::Context,
            ResourceExtensionScope::AnyResource => Self::AnyResource,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
