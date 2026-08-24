// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceCrudDispatcher,
    ResourceDispatcherMeta,
    ResourceLifecycleEventDispatcher,
    ResourcePresentationDispatcher,
    ResourceSpecViewDispatcher,
    TypeUri,
    UnsupportedResourceDescriptorError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum GetResourceCrudDispatcherError {
    #[error(transparent)]
    Unsupported(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves per-resource-type dispatchers on behalf of services, so that they
/// don't have to hold a [`dill::Catalog`] themselves.
///
/// Dispatchers are `Transient` and their dependencies (use cases, query
/// services) are transaction-scoped, so nothing here can be cached: every
/// lookup must construct the dispatcher against the *current* catalog. That
/// catalog is the one this factory was itself resolved from — dill resolves
/// builders against the catalog the resolution started from, so a factory
/// injected into a component built from a transaction-chained catalog carries
/// that chained catalog, and the dispatchers it builds see the transaction.
///
/// This is why the factory is `Transient` and injects the catalog by value:
/// dill only permits by-value catalog injection in a `Transient` scope,
/// precisely because longer-lived scopes would capture a stale catalog.
///
/// **Invariant: never make this reachable from a `Singleton`.** A singleton
/// holding it — directly, or transitively through any injected dependency —
/// would pin the catalog it was first built from and keep serving dispatchers
/// bound to an already-committed transaction. `CatalogBuilder::validate()`
/// catches only the *direct* edge (`Singleton` -> `Transient` scope
/// inversion); it does not check transitive paths, and it cannot see
/// components that resolve implementations dynamically via `builders_for*`
/// (e.g. `TaskAgentImpl`, which enumerates `dyn TaskRunner` against a
/// transaction-less base catalog). If this type ever needs to be reached from
/// a long-lived scope, inject `dill::CatalogWeakRef` and resolve per call
/// instead of capturing a `Catalog`.
pub struct ResourceDispatcherFactory {
    catalog: dill::Catalog,
}

#[dill::component(pub)]
impl ResourceDispatcherFactory {
    pub fn new(catalog: dill::Catalog) -> Self {
        Self { catalog }
    }

    /// Resolves the single dispatcher of kind `TDispatcher` registered for
    /// `schema`, or `None` if there is none. Shared by every dispatcher kind:
    /// they differ only in how a miss is reported, never in how the lookup
    /// runs.
    ///
    /// A duplicate registration is a static wiring bug and is always an
    /// [`InternalError`], regardless of how the caller treats a miss.
    fn try_dispatcher_in<TDispatcher: ?Sized + 'static>(
        target_catalog: &dill::Catalog,
        schema: &str,
        dispatcher_name: &str,
    ) -> Result<Option<Arc<TDispatcher>>, InternalError> {
        let mut dispatchers = target_catalog.builders_for_with_meta::<TDispatcher, _>(
            |meta: &ResourceDispatcherMeta| meta.schema == schema,
        );

        let Some(builder) = dispatchers.next() else {
            return Ok(None);
        };

        if dispatchers.next().is_some() {
            return Err(InternalError::new(format!(
                "Duplicate {dispatcher_name} registered for schema='{schema}'"
            )));
        }

        builder.get(target_catalog).int_err().map(Some)
    }

    /// Constructs the single CRUD dispatcher registered for `schema` in
    /// `target_catalog`, or `None` if there is none. A duplicate registration
    /// is a static wiring bug and is always an [`InternalError`],
    /// regardless of how the caller treats a miss.
    fn try_crud_dispatcher_in(
        target_catalog: &dill::Catalog,
        schema: &str,
    ) -> Result<Option<Arc<dyn ResourceCrudDispatcher>>, InternalError> {
        Self::try_dispatcher_in::<dyn ResourceCrudDispatcher>(
            target_catalog,
            schema,
            "CRUD dispatcher",
        )
    }

    /// Resolves a CRUD dispatcher for a schema that came from unvalidated input
    /// (e.g. the `$schema` of a user-supplied manifest), so an unknown schema
    /// is a user-facing error rather than an internal one.
    pub fn crud_dispatcher(
        &self,
        schema: &str,
    ) -> Result<Arc<dyn ResourceCrudDispatcher>, GetResourceCrudDispatcherError> {
        Self::crud_dispatcher_in(&self.catalog, schema)
    }

    /// [`Self::crud_dispatcher`] against an explicitly supplied catalog, for
    /// callers that are handed one rather than injecting this factory (see the
    /// type-level docs).
    pub fn crud_dispatcher_in(
        target_catalog: &dill::Catalog,
        schema: &str,
    ) -> Result<Arc<dyn ResourceCrudDispatcher>, GetResourceCrudDispatcherError> {
        Self::try_crud_dispatcher_in(target_catalog, schema)?.ok_or_else(|| {
            UnsupportedResourceDescriptorError::NotFound {
                schema: TypeUri::new_unchecked(schema),
            }
            .into()
        })
    }

    /// Resolves a CRUD dispatcher for a schema that is already known to be
    /// valid (e.g. read from a stored snapshot, or resolved from a
    /// registered selector). A missing/duplicate dispatcher is therefore a
    /// data-integrity catastrophe, so every failure — including "not found"
    /// — is surfaced as an [`InternalError`] rather than a user-facing
    /// unsupported-descriptor error.
    pub fn crud_dispatcher_for_trusted_schema(
        &self,
        schema: &str,
    ) -> Result<Arc<dyn ResourceCrudDispatcher>, InternalError> {
        Self::crud_dispatcher_for_trusted_schema_in(&self.catalog, schema)
    }

    /// [`Self::crud_dispatcher_for_trusted_schema`] against an explicitly
    /// supplied catalog.
    pub fn crud_dispatcher_for_trusted_schema_in(
        target_catalog: &dill::Catalog,
        schema: &str,
    ) -> Result<Arc<dyn ResourceCrudDispatcher>, InternalError> {
        Self::try_crud_dispatcher_in(target_catalog, schema)?.ok_or_else(|| {
            InternalError::new(format!(
                "No CRUD dispatcher registered for trusted schema '{schema}'"
            ))
        })
    }

    /// Returns the spec view dispatcher for the given resource schema, or
    /// `None` if none is registered. Absence is normal for resource types that
    /// have no sensitive fields.
    pub fn spec_view_dispatcher(
        &self,
        schema: &TypeUri,
    ) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
        Self::spec_view_dispatcher_in(&self.catalog, schema)
    }

    /// [`Self::spec_view_dispatcher`] against an explicitly supplied catalog.
    pub fn spec_view_dispatcher_in(
        target_catalog: &dill::Catalog,
        schema: &TypeUri,
    ) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
        Self::try_dispatcher_in::<dyn ResourceSpecViewDispatcher>(
            target_catalog,
            schema.as_str(),
            "resource spec view dispatcher",
        )
        .ok()
        .flatten()
    }

    /// Resolves the lifecycle dispatcher for `schema` against an explicitly
    /// supplied catalog.
    ///
    /// Associated-only on purpose: its caller is a message consumer that is
    /// handed the per-message transaction catalog, so there is no instance form
    /// that would be correct (see the type-level docs).
    pub fn lifecycle_dispatcher_in(
        target_catalog: &dill::Catalog,
        schema: &TypeUri,
    ) -> Result<Arc<dyn ResourceLifecycleEventDispatcher>, InternalError> {
        Self::try_dispatcher_in::<dyn ResourceLifecycleEventDispatcher>(
            target_catalog,
            schema.as_str(),
            "resource lifecycle dispatcher",
        )?
        .ok_or_else(|| {
            InternalError::new(format!(
                "No resource lifecycle dispatcher registered for schema='{schema}'"
            ))
        })
    }

    /// Constructs every registered presentation dispatcher.
    ///
    /// Callers typically index the result by
    /// [`ResourcePresentationDispatcher::schema`]; this is deliberately not
    /// done here so that a caller spanning several resource types pays for
    /// the construction only once.
    pub fn presentation_dispatchers(
        &self,
    ) -> Result<Vec<Arc<dyn ResourcePresentationDispatcher>>, InternalError> {
        Self::presentation_dispatchers_in(&self.catalog)
    }

    /// [`Self::presentation_dispatchers`] against an explicitly supplied
    /// catalog.
    pub fn presentation_dispatchers_in(
        target_catalog: &dill::Catalog,
    ) -> Result<Vec<Arc<dyn ResourcePresentationDispatcher>>, InternalError> {
        target_catalog
            .builders_for::<dyn ResourcePresentationDispatcher>()
            .map(|builder| builder.get(target_catalog).int_err())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
