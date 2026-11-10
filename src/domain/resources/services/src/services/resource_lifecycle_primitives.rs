// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use messaging_outbox::{Outbox, OutboxExt};
use serde::Serialize;
use time_source::SystemTimeSource;

use crate::domain::{
    DeclarativeResource,
    EventSourcedResource,
    InvariantViolationOf,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceAggregateLoader,
    ResourceBatchTransition,
    ResourceCleanupFailure,
    ResourceConditionUpdateError,
    ResourceConditions,
    ResourceConditionsPatch,
    ResourceDeletionCompletion,
    ResourceDeletionInProgressError,
    ResourceExpectedVersion,
    ResourceExtensionKind,
    ResourceGenerationConflictError,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageConditionChange,
    ResourceLifecycleMessageResourceRef,
    ResourceLifecycleTransitionError,
    ResourceLoadError,
    ResourcePersistenceService,
    ResourceSchemaId,
    ResourceSchemaProvider,
    ResourceStaleExpectationError,
    ResourceStatus,
    ResourceStatusConflictError,
    ResourceTransitionSkip,
    ResourceTransitionSkipReason,
    StatusConflictOf,
    TypeRef,
    TypeUri,
};
use crate::{ResourceConditionPatchChecker, ResourceExtensionSchemaRegistry};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements `ResourceLifecyclePrimitives` over the transaction-scoped
/// services it is given
pub struct ResourceLifecyclePrimitivesHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
    resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
    outbox: &'a dyn Outbox,
    time_source: &'a dyn SystemTimeSource,
    extension_schema_registry: &'a ResourceExtensionSchemaRegistry,
}

type TransitionResult<T, R> = Result<T, ResourceLifecycleTransitionError<R>>;

impl<'a, R> ResourceLifecyclePrimitivesHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::LifecycleError: InvariantViolationOf<<R as DeclarativeResource>::ResourceState>,
    R::Spec: Serialize,
{
    pub fn new(
        resource_aggregate_loader: &'a dyn ResourceAggregateLoader<R>,
        resource_persistence_service: &'a dyn ResourcePersistenceService<R>,
        outbox: &'a dyn Outbox,
        time_source: &'a dyn SystemTimeSource,
        extension_schema_registry: &'a ResourceExtensionSchemaRegistry,
    ) -> Self {
        Self {
            resource_aggregate_loader,
            resource_persistence_service,
            outbox,
            time_source,
            extension_schema_registry,
        }
    }

    pub async fn begin_reconciliation(
        &self,
        id: &ResourceID,
        expected_status_generation: Option<u64>,
    ) -> TransitionResult<Option<R>, R> {
        let mut resource = self.load(id).await?;
        if !resource.needs_reconciliation() {
            return Ok(None);
        }

        let expected_status_generation =
            expected_status_generation.unwrap_or(resource.headers().status_generation);
        let old_status = resource.status().clone();
        let now = self.time_source.now();
        resource
            .try_mark_reconciliation_started(now, expected_status_generation)
            .map_err(lifecycle_error)?;
        self.resource_persistence_service
            .save(&mut resource)
            .await?;
        self.post_status_updated(now, &resource, &old_status)
            .await?;

        Ok(Some(resource))
    }

    pub async fn complete_reconciliation(
        &self,
        id: &ResourceID,
        expected: ResourceExpectedVersion,
    ) -> TransitionResult<R, R> {
        let mut resource = self.load_reconcilable(id, expected).await?;
        let old_status = resource.status().clone();
        let now = self.time_source.now();

        resource
            .try_mark_reconciliation_succeeded(now, expected.generation, expected.status_generation)
            .map_err(lifecycle_error)?;
        self.resource_persistence_service
            .save(&mut resource)
            .await?;
        self.post_status_updated(now, &resource, &old_status)
            .await?;

        Ok(resource)
    }

    pub async fn fail_reconciliation(
        &self,
        id: &ResourceID,
        expected: ResourceExpectedVersion,
        error: &R::ReconcileError,
    ) -> TransitionResult<R, R> {
        let mut resource = self.load_reconcilable(id, expected).await?;
        let old_status = resource.status().clone();
        let now = self.time_source.now();

        resource
            .try_mark_reconciliation_failed(
                now,
                expected.generation,
                expected.status_generation,
                error,
            )
            .map_err(lifecycle_error)?;
        self.resource_persistence_service
            .save(&mut resource)
            .await?;
        self.post_status_updated(now, &resource, &old_status)
            .await?;

        Ok(resource)
    }

    pub async fn request_deletion(&self, ids: &[ResourceID]) -> TransitionResult<Vec<R>, R> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let now = self.time_source.now();
        let mut resources = self.load_many(ids).await?;
        for resource in &mut resources {
            resource.try_request_deletion(now).map_err(|err| {
                // The lifecycle error does not name the resource
                tracing::error!(
                    resource_id = %resource.id(),
                    "Failed to request deletion of a resource"
                );
                ResourceLifecycleTransitionError::Lifecycle(err)
            })?;
        }

        self.resource_persistence_service
            .save_many(&mut resources)
            .await?;

        let snapshots = resources
            .iter()
            .map(EventSourcedResource::make_resource_snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        self.post(ResourceLifecycleMessage::deletion_requested(now, snapshots))
            .await?;

        Ok(resources)
    }

    pub async fn complete_deletion(
        &self,
        completions: &[ResourceDeletionCompletion],
    ) -> TransitionResult<ResourceBatchTransition<R>, R> {
        let now = self.time_source.now();
        let ids = completions
            .iter()
            .map(|completion| completion.id)
            .collect::<Vec<_>>();

        let (transition, _) = self
            .transition_awaiting_cleanup(&ids, |resource, index| {
                if let Some(expected) = completions[index].expected_status_generation
                    && let Some(conflict) = status_conflict(resource, expected)
                {
                    return Err(TransitionStep::Skip(
                        ResourceTransitionSkipReason::StatusConflict(conflict),
                    ));
                }
                resource
                    .try_complete_deletion(now)
                    .map_err(TransitionStep::from_lifecycle_error)
            })
            .await?;

        let deleted = transition
            .transitioned
            .iter()
            .filter(|resource| resource.headers().deleted_at.is_some())
            .map(EventSourcedResource::make_resource_snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        if !deleted.is_empty() {
            self.post(ResourceLifecycleMessage::deleted(now, deleted))
                .await?;
        }

        Ok(transition)
    }

    pub async fn record_cleanup_failure(
        &self,
        failures: &[ResourceCleanupFailure],
    ) -> TransitionResult<ResourceBatchTransition<R>, R> {
        let now = self.time_source.now();
        let ids = failures
            .iter()
            .map(|failure| failure.id)
            .collect::<Vec<_>>();

        let (transition, old_statuses) = self
            .transition_awaiting_cleanup(&ids, |resource, index| {
                let failure = &failures[index];
                resource
                    .try_mark_cleanup_failed(
                        now,
                        failure.expected_status_generation,
                        &failure.error,
                    )
                    .map_err(TransitionStep::from_lifecycle_error)
            })
            .await?;

        for (resource, old_status) in transition.transitioned.iter().zip(&old_statuses) {
            self.post_status_updated(now, resource, old_status).await?;
        }

        Ok(transition)
    }

    /// Applies `transition` to every resource still awaiting its cleanup and
    /// saves the changed ones in one batch. `transition` gets the index of the
    /// resource in `ids`. Also returns the status each transitioned resource
    /// had before
    async fn transition_awaiting_cleanup(
        &self,
        ids: &[ResourceID],
        transition: impl Fn(&mut R, usize) -> Result<(), TransitionStep<R>>,
    ) -> TransitionResult<(ResourceBatchTransition<R>, Vec<ResourceStatus>), R> {
        let mut batch = ResourceBatchTransition {
            transitioned: Vec::new(),
            skipped: Vec::new(),
        };
        let mut old_statuses = Vec::new();
        if ids.is_empty() {
            return Ok((batch, old_statuses));
        }

        for (index, mut resource) in self.load_many(ids).await?.into_iter().enumerate() {
            let id = *resource.id();
            if !resource.needs_cleanup() {
                batch.skipped.push(ResourceTransitionSkip {
                    id,
                    reason: ResourceTransitionSkipReason::NotAwaitingCleanup,
                });
                continue;
            }

            let old_status = resource.status().clone();
            match transition(&mut resource, index) {
                Ok(()) => {
                    batch.transitioned.push(resource);
                    old_statuses.push(old_status);
                }
                Err(TransitionStep::Skip(reason)) => {
                    batch.skipped.push(ResourceTransitionSkip { id, reason });
                }
                Err(TransitionStep::Fail(err)) => {
                    return Err(ResourceLifecycleTransitionError::Lifecycle(err));
                }
            }
        }

        if !batch.transitioned.is_empty() {
            self.resource_persistence_service
                .save_many(&mut batch.transitioned)
                .await?;
        }

        Ok((batch, old_statuses))
    }

    pub async fn update_conditions(
        &self,
        id: &ResourceID,
        expected_status_generation: u64,
        patch: &ResourceConditionsPatch,
    ) -> Result<R, ResourceConditionUpdateError<R>> {
        let mut resource = self
            .resource_aggregate_loader
            .load(id)
            .await
            .map_err(ResourceLoadError)?;
        if let Some(conflict) = status_conflict(&resource, expected_status_generation) {
            return Err(ResourceConditionUpdateError::StatusGenerationChanged(
                conflict,
            ));
        }

        ResourceConditionPatchChecker::new(self.extension_schema_registry)
            .check(patch, &self.resource_schema()?)?;

        let old_status = resource.status().clone();
        let mut new_status = old_status.clone();
        if patch.apply_to(&mut new_status.conditions).is_empty() {
            return Ok(resource);
        }
        R::validate_status_change(&old_status, &new_status)?;

        let now = self.time_source.now();
        resource
            .try_update_status(now, expected_status_generation, new_status)
            .map_err(condition_update_lifecycle_error)?;
        self.resource_persistence_service
            .save(&mut resource)
            .await?;
        self.post_status_updated(now, &resource, &old_status)
            .await?;

        Ok(resource)
    }

    /// Announces the status write that moved the resource from `old_status`.
    /// A write that left the status as it was recorded no event, so it posts
    /// nothing
    async fn post_status_updated(
        &self,
        now: DateTime<Utc>,
        resource: &R,
        old_status: &ResourceStatus,
    ) -> Result<(), InternalError> {
        let new_status = resource.status();
        if new_status == old_status {
            return Ok(());
        }

        let changed_conditions =
            self.condition_changes(&old_status.conditions, &new_status.conditions)?;
        let message = ResourceLifecycleMessage::status_updated(
            now,
            ResourceLifecycleMessageResourceRef {
                id: *resource.id(),
                schema: R::schema().clone(),
                headers: resource.headers().clone(),
            },
            new_status,
            changed_conditions,
        );
        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, message)
            .await
    }

    /// How each condition that differs between `old` and `new` ended up, with
    /// the value where its registration marks it as carried
    fn condition_changes(
        &self,
        old: &ResourceConditions,
        new: &ResourceConditions,
    ) -> Result<BTreeMap<TypeUri, ResourceLifecycleMessageConditionChange>, InternalError> {
        let resource_schema = self.resource_schema()?;
        let keys = old.entries.keys().chain(new.entries.keys());

        let mut changes = BTreeMap::new();
        for key in keys {
            let new_value = new.entries.get(key);
            if old.entries.get(key) == new_value {
                continue;
            }
            let TypeRef::Uri(uri) = key else {
                return Err(InternalError::new(format!(
                    "Stored condition '{key}' is not keyed by its schema URI"
                )));
            };

            let change = match new_value {
                Some(value) => ResourceLifecycleMessageConditionChange::Set {
                    value: self
                        .is_carried(key, &resource_schema)
                        .then(|| value.clone()),
                },
                None => ResourceLifecycleMessageConditionChange::Removed,
            };
            changes.insert(uri.clone(), change);
        }

        Ok(changes)
    }

    /// A condition whose schema is no longer registered is not carried
    fn is_carried(&self, key: &TypeRef, resource_schema: &ResourceSchemaId) -> bool {
        self.extension_schema_registry
            .resolve(ResourceExtensionKind::Condition, key, resource_schema)
            .ok()
            .flatten()
            .and_then(|registration| registration.condition().map(|condition| condition.carried))
            .unwrap_or(false)
    }

    fn resource_schema(&self) -> Result<ResourceSchemaId, InternalError> {
        ResourceSchemaId::parse(R::schema().as_str()).int_err()
    }

    /// Loads a resource whose reconciliation outcome is about to be recorded,
    /// checking the expectations in the order a reconciliation outcome is
    /// discarded: a deletion request, then a newer generation, then another
    /// status writer
    async fn load_reconcilable(
        &self,
        id: &ResourceID,
        expected: ResourceExpectedVersion,
    ) -> TransitionResult<R, R> {
        let resource = self.load(id).await?;

        if resource.headers().deletion_requested_at.is_some() {
            return Err(ResourceStaleExpectationError::DeletionRequested(
                ResourceDeletionInProgressError { id: *id },
            )
            .into());
        }

        let generation = resource.headers().generation;
        if generation != expected.generation {
            return Err(ResourceStaleExpectationError::GenerationChanged(
                ResourceGenerationConflictError {
                    expected: expected.generation,
                    actual: generation,
                },
            )
            .into());
        }

        ensure_status_generation(&resource, expected.status_generation)?;

        Ok(resource)
    }

    async fn load(&self, id: &ResourceID) -> TransitionResult<R, R> {
        self.resource_aggregate_loader
            .load(id)
            .await
            .map_err(|err| ResourceLoadError(err).into())
    }

    /// Loads the resources in the order of `ids`
    async fn load_many(&self, ids: &[ResourceID]) -> TransitionResult<Vec<R>, R> {
        self.resource_aggregate_loader
            .load_many(ids)
            .await
            .map_err(ErrorIntoInternal::int_err)?
            .into_iter()
            .zip(ids)
            .map(|(result, id)| {
                result.map_err(|err| {
                    tracing::error!(resource_id = %id, error = %err, "Failed to load a resource");
                    ResourceLoadError(err).into()
                })
            })
            .collect()
    }

    async fn post(&self, message: ResourceLifecycleMessage) -> TransitionResult<(), R> {
        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE, message)
            .await
            .map_err(ResourceLifecycleTransitionError::Internal)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why one resource of a batch was not transitioned
enum TransitionStep<R: EventSourcedResource> {
    Skip(ResourceTransitionSkipReason),
    Fail(R::LifecycleError),
}

impl<R: EventSourcedResource> TransitionStep<R> {
    fn from_lifecycle_error(err: R::LifecycleError) -> Self {
        match err.as_status_conflict() {
            Some(conflict) => Self::Skip(ResourceTransitionSkipReason::StatusConflict(*conflict)),
            None => Self::Fail(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn status_conflict<R: EventSourcedResource>(
    resource: &R,
    expected: u64,
) -> Option<ResourceStatusConflictError> {
    let actual = resource.headers().status_generation;
    (actual != expected).then_some(ResourceStatusConflictError { expected, actual })
}

fn ensure_status_generation<R: EventSourcedResource>(
    resource: &R,
    expected: u64,
) -> Result<(), ResourceStaleExpectationError> {
    match status_conflict(resource, expected) {
        Some(conflict) => Err(ResourceStaleExpectationError::StatusGenerationChanged(
            conflict,
        )),
        None => Ok(()),
    }
}

fn condition_update_lifecycle_error<R: EventSourcedResource>(
    err: R::LifecycleError,
) -> ResourceConditionUpdateError<R> {
    match err.as_status_conflict() {
        Some(conflict) => ResourceConditionUpdateError::StatusGenerationChanged(*conflict),
        None => ResourceConditionUpdateError::Lifecycle(err),
    }
}

fn lifecycle_error<R: EventSourcedResource>(
    err: R::LifecycleError,
) -> ResourceLifecycleTransitionError<R> {
    match err.as_status_conflict() {
        Some(conflict) => ResourceStaleExpectationError::StatusGenerationChanged(*conflict).into(),
        None => ResourceLifecycleTransitionError::Lifecycle(err),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_lifecycle_primitives {
    (
        primitives = $primitives:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceLifecyclePrimitives<$resource>)]
        pub struct $primitives {
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
            outbox: std::sync::Arc<dyn $crate::messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
            extension_schema_registry: std::sync::Arc<$crate::ResourceExtensionSchemaRegistry>,
        }

        impl $primitives {
            fn helper(&self) -> $crate::ResourceLifecyclePrimitivesHelper<'_, $resource> {
                $crate::ResourceLifecyclePrimitivesHelper::new(
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.outbox.as_ref(),
                    self.time_source.as_ref(),
                    self.extension_schema_registry.as_ref(),
                )
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceLifecyclePrimitives<$resource> for $primitives {
            async fn begin_reconciliation(
                &self,
                id: &kamu_resources::ResourceID,
                expected_status_generation: Option<u64>,
            ) -> Result<
                Option<$resource>,
                kamu_resources::ResourceLifecycleTransitionError<$resource>,
            > {
                self.helper()
                    .begin_reconciliation(id, expected_status_generation)
                    .await
            }

            async fn complete_reconciliation(
                &self,
                id: &kamu_resources::ResourceID,
                expected: kamu_resources::ResourceExpectedVersion,
            ) -> Result<$resource, kamu_resources::ResourceLifecycleTransitionError<$resource>>
            {
                self.helper().complete_reconciliation(id, expected).await
            }

            async fn fail_reconciliation(
                &self,
                id: &kamu_resources::ResourceID,
                expected: kamu_resources::ResourceExpectedVersion,
                error: &<$resource as kamu_resources::ManagedResource>::ReconcileError,
            ) -> Result<$resource, kamu_resources::ResourceLifecycleTransitionError<$resource>>
            {
                self.helper().fail_reconciliation(id, expected, error).await
            }

            async fn request_deletion(
                &self,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<Vec<$resource>, kamu_resources::ResourceLifecycleTransitionError<$resource>>
            {
                self.helper().request_deletion(ids).await
            }

            async fn complete_deletion(
                &self,
                completions: &[kamu_resources::ResourceDeletionCompletion],
            ) -> Result<
                kamu_resources::ResourceBatchTransition<$resource>,
                kamu_resources::ResourceLifecycleTransitionError<$resource>,
            > {
                self.helper().complete_deletion(completions).await
            }

            async fn record_cleanup_failure(
                &self,
                failures: &[kamu_resources::ResourceCleanupFailure],
            ) -> Result<
                kamu_resources::ResourceBatchTransition<$resource>,
                kamu_resources::ResourceLifecycleTransitionError<$resource>,
            > {
                self.helper().record_cleanup_failure(failures).await
            }

            async fn update_conditions(
                &self,
                id: &kamu_resources::ResourceID,
                expected_status_generation: u64,
                patch: &kamu_resources::ResourceConditionsPatch,
            ) -> Result<$resource, kamu_resources::ResourceConditionUpdateError<$resource>> {
                self.helper()
                    .update_conditions(id, expected_status_generation, patch)
                    .await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
