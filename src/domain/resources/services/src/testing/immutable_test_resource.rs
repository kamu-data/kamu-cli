// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use event_sourcing::*;
use internal_error::InternalError;
use kamu_resources::*;
use serde::{Deserialize, Serialize};

use crate::testing::TestResourceReconcileError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourceSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImmutableTestResourceSpec {
    pub target: TestTargetRef,

    /// Stored as [`Self::DEFAULT_MODE`] when omitted
    #[serde(default)]
    pub mode: Option<String>,
}

kamu_resources::declare_identity_resource_spec_from_input!(ImmutableTestResourceSpec);

impl ImmutableTestResourceSpec {
    pub const DEFAULT_MODE: &'static str = "auto";
    pub const INVALID_MODE: &'static str = "invalid";
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TestTargetRef {
    #[serde(default)]
    pub id: Option<String>,

    #[serde(default)]
    pub name: Option<String>,
}

impl TestTargetRef {
    fn has_id_of(&self, other: &Self) -> bool {
        self.id.is_some() && self.id == other.id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ImmutableTestResourceSpecValidationError {
    #[error("mode is invalid")]
    InvalidMode,
}

impl ResourceValidateSpec for ImmutableTestResourceSpec {
    type ValidationError = ImmutableTestResourceSpecValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.mode.as_deref() == Some(Self::INVALID_MODE) {
            return Err(ImmutableTestResourceSpecValidationError::InvalidMode);
        }
        Ok(())
    }
}

impl ResourceLinterSpec for ImmutableTestResourceSpec {
    fn lint_warnings(&self) -> Vec<ResourceWarning> {
        Vec::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ImmutableTestResourceStateModel;

impl ResourceStateModel for ImmutableTestResourceStateModel {
    type Spec = ImmutableTestResourceSpec;
    type SpecInput = ImmutableTestResourceSpec;
    type State = ImmutableTestResourceState;

    const SPEC_MUTABILITY: SpecMutability = SpecMutability::Immutable;
}

pub type ImmutableTestResourceState = EventSourcedResourceState<ImmutableTestResourceStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourceEventStore
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ImmutableTestResourceEventStore:
    EventStore<ImmutableTestResourceState> + 'static
{
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourceLifecycleError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum ImmutableTestResourceLifecycleError {
    #[error(transparent)]
    HeadersValidation(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    SpecValidation(#[from] ImmutableTestResourceSpecValidationError),

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<ImmutableTestResourceState>>),

    #[error(transparent)]
    StatusConflict(#[from] ResourceStatusConflictError),

    #[error(transparent)]
    DeletionInProgress(#[from] ResourceDeletionInProgressError),

    #[error(transparent)]
    SpecImmutable(#[from] ResourceSpecImmutableError),
}

kamu_resources::impl_resource_lifecycle_error!(
    ImmutableTestResourceLifecycleError,
    ImmutableTestResourceState
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A type whose spec is immutable and refers to a [`TestTargets`] entry, as
/// the spec of a task refers to a dataset
#[derive(Aggregate, Debug)]
pub struct ImmutableTestResource(
    pub Aggregate<ImmutableTestResourceState, dyn ImmutableTestResourceEventStore>,
);

impl ImmutableTestResource {
    pub const SCHEMA_STR: &'static str =
        "https://test.kamu.dev/schemas/test/v1/ImmutableTestResource";

    kamu_resources::declare_resource_selector_constants!("immutabletestresources", []);
}

impl ResourceSchemaProvider for ImmutableTestResource {
    fn type_uri() -> &'static TypeUri {
        static IMMUTABLE_TEST_RESOURCE_SCHEMA: LazyLock<TypeUri> =
            LazyLock::new(|| TypeUri::new_unchecked(ImmutableTestResource::SCHEMA_STR));
        &IMMUTABLE_TEST_RESOURCE_SCHEMA
    }
}

impl DeclarativeResource for ImmutableTestResource {
    type Spec = ImmutableTestResourceSpec;
    type SpecInput = ImmutableTestResourceSpec;
    type ResourceState = ImmutableTestResourceState;
}

impl ResourceStatusRules for ImmutableTestResource {}

impl ResourcePresentation for ImmutableTestResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::CANONICAL_SELECTOR_NAME,
        Self::SELECTOR_ALIASES,
        &[],
    );

    fn list_column_values(_state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        Vec::new()
    }
}

kamu_resources::impl_event_sourced_resource!(
    resource = ImmutableTestResource,
    reconcile_error = TestResourceReconcileError,
    lifecycle_error = ImmutableTestResourceLifecycleError,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestTargets
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What [`ImmutableTestResource`]s refer to: names by ID
pub struct TestTargets {
    names_by_id: Mutex<BTreeMap<String, String>>,
}

impl TestTargets {
    /// Exists from the start
    pub const ID: &'static str = "target-1";
    pub const NAME: &'static str = "alpha";

    /// Adds a target or renames an existing one
    pub fn put(&self, id: &str, name: &str) {
        self.names_by_id
            .lock()
            .unwrap()
            .insert(id.to_string(), name.to_string());
    }

    pub fn remove(&self, id: &str) {
        self.names_by_id.lock().unwrap().remove(id);
    }

    fn resolve(&self, target: &TestTargetRef) -> Option<TestTargetRef> {
        self.names_by_id
            .lock()
            .unwrap()
            .iter()
            .find(|(id, name)| {
                target
                    .id
                    .as_ref()
                    .map_or(target.name.as_ref() == Some(*name), |target_id| {
                        target_id == *id
                    })
            })
            .map(|(id, name)| TestTargetRef {
                id: Some(id.clone()),
                name: Some(name.clone()),
            })
    }
}

impl Default for TestTargets {
    fn default() -> Self {
        Self {
            names_by_id: Mutex::new([(Self::ID.to_string(), Self::NAME.to_string())].into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourcePreApply
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Fills the default mode and resolves the target
#[dill::component]
#[dill::interface(dyn ResourceSpecPreApply<ImmutableTestResource>)]
struct ImmutableTestResourcePreApply {
    targets: Arc<TestTargets>,
}

#[async_trait::async_trait]
impl ResourceSpecPreApply<ImmutableTestResource> for ImmutableTestResourcePreApply {
    async fn pre_apply(
        &self,
        new_spec: ImmutableTestResourceSpec,
        context: ResourceSpecPreApplyContext<'_, ImmutableTestResource>,
    ) -> Result<ResourceSpecPreApplyOutcome<ImmutableTestResource>, InternalError> {
        let maybe_current_target = context.maybe_current_spec.map(|spec| &spec.target);

        let target = if let Some(current_target) =
            maybe_current_target.filter(|current| new_spec.target.has_id_of(current))
        {
            current_target.clone()
        } else {
            let Some(resolved) = self.targets.resolve(&new_spec.target) else {
                return Ok(ResourceSpecPreApplyOutcome::Rejected(
                    ApplyResourceRejection {
                        category: ApplyResourceRejectionCategory::ReferencedObjectMissing,
                        message: format!("target not found: {:?}", new_spec.target),
                    },
                ));
            };
            maybe_current_target
                .filter(|current| resolved.has_id_of(current))
                .cloned()
                .unwrap_or(resolved)
        };

        Ok(ResourceSpecPreApplyOutcome::Prepared(
            ImmutableTestResourceSpec {
                target,
                mode: Some(
                    new_spec
                        .mode
                        .unwrap_or_else(|| ImmutableTestResourceSpec::DEFAULT_MODE.to_string()),
                ),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ImmutableTestResourceReconciler
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SimpleResourceReconciler<ImmutableTestResource>)]
struct ImmutableTestResourceReconciler;

#[async_trait::async_trait]
impl SimpleResourceReconciler<ImmutableTestResource> for ImmutableTestResourceReconciler {
    async fn reconcile(
        &self,
        _resource: &ImmutableTestResource,
    ) -> Result<(), TestResourceReconcileError> {
        Ok(())
    }

    async fn cleanup(&self, _resource: &ImmutableTestResource) -> Result<(), ResourceCleanupError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service layer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

crate::declare_resource_service_layer!(
    domain = crate::testing,
    name = ImmutableTestResource,
    resource = ImmutableTestResource,
    state_model = ImmutableTestResourceStateModel,
    controller_name = "immutable-test-resource-controller",
    controller_revision = 1
);

declare_resource_crud_dispatcher!(
    dispatcher = ImmutableTestResourceCrudDispatcher,
    resource = ImmutableTestResource
);

declare_resource_presentation_dispatcher!(
    dispatcher = ImmutableTestResourcePresentationDispatcher,
    resource = ImmutableTestResource
);

/// Everything the type needs to be served beside the real ones, reconciled by
/// a stub
pub fn register_immutable_test_resource(catalog_builder: &mut dill::CatalogBuilder) {
    register_immutable_test_resource_resource_service_layer(catalog_builder);
    catalog_builder.add::<ImmutableTestResourceCrudDispatcher>();
    catalog_builder.add::<ImmutableTestResourcePresentationDispatcher>();
    catalog_builder.add::<ImmutableTestResourcePreApply>();
    catalog_builder.add::<ImmutableTestResourceReconciler>();
    catalog_builder.add_value(TestTargets::default());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
