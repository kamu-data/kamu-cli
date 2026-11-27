// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use event_sourcing::*;
use kamu_resources::*;
use serde::{Deserialize, Serialize};

use crate::testing::{
    TestResourceReconciler,
    register_test_condition_schema_dispatchers,
    test_stage_condition_key,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TestResourceSpec {
    pub value: String,
}

kamu_resources::declare_identity_resource_spec_from_input!(TestResourceSpec);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum TestResourceSpecValidationError {
    #[error("value must not be empty")]
    EmptyValue,
}

impl ResourceValidateSpec for TestResourceSpec {
    type ValidationError = TestResourceSpecValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        if self.value.is_empty() {
            return Err(TestResourceSpecValidationError::EmptyValue);
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceLinterSpec for TestResourceSpec {
    fn lint_warnings(&self) -> Vec<ResourceWarning> {
        let mut warnings = Vec::new();
        if self.value.len() > 64 {
            warnings.push(ResourceWarning {
                code: "value_too_long".to_string(),
                path: Some("spec.value".to_string()),
                message: "value exceeds recommended length".to_string(),
            });
        }
        warnings
    }
}

// TestResourceStateModel
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceStateModel;

impl ResourceStateModel for TestResourceStateModel {
    type Spec = TestResourceSpec;
    type SpecInput = TestResourceSpec;
    type State = TestResourceState;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TestResourceState = EventSourcedResourceState<TestResourceStateModel>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceEventStore
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TestResourceEventStore: EventStore<TestResourceState> + 'static {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceLifecycleError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum TestResourceLifecycleError {
    #[error(transparent)]
    HeadersValidation(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    SpecValidation(#[from] TestResourceSpecValidationError),

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<TestResourceState>>),

    #[error(transparent)]
    StatusConflict(#[from] ResourceStatusConflictError),

    #[error(transparent)]
    DeletionInProgress(#[from] ResourceDeletionInProgressError),

    #[error(transparent)]
    SpecImmutable(#[from] ResourceSpecImmutableError),
}

kamu_resources::impl_resource_lifecycle_error!(TestResourceLifecycleError, TestResourceState);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceReconcileError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum TestResourceReconcileError {
    #[error(transparent)]
    ConcurrentModification(event_sourcing::ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),

    /// Fails every reconciliation of the spec, so retrying does not help
    #[error("Spec rejected: {0}")]
    Rejected(String),
}

pub const TEST_RECONCILIATION_ERROR_CODE_REJECTED: &str = "test-rejected";

impl ResourceReconcileError for TestResourceReconcileError {
    fn code(&self) -> &'static str {
        match self {
            Self::ConcurrentModification(_) => RECONCILIATION_ERROR_CODE_CONCURRENT_MODIFICATION,
            Self::Internal(_) => RECONCILIATION_ERROR_CODE_INTERNAL_ERROR,
            Self::Rejected(_) => TEST_RECONCILIATION_ERROR_CODE_REJECTED,
        }
    }

    fn user_message(&self) -> String {
        match self {
            Self::ConcurrentModification(_) => {
                "Resource was modified concurrently during reconciliation.".to_owned()
            }
            Self::Internal(e) => format!("Internal error: {e}"),
            Self::Rejected(reason) => format!("Spec rejected: {reason}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            Self::ConcurrentModification(_) | Self::Internal(_) => true,
            Self::Rejected(_) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct TestResource(pub Aggregate<TestResourceState, dyn TestResourceEventStore>);

impl TestResource {
    pub const SCHEMA_STR: &'static str = "https://test.kamu.dev/schemas/test/v1/TestResource";

    kamu_resources::declare_resource_selector_constants!("testresources", []);

    /// A spec value `staged:<stage>` starts the resource with that
    /// `TestStage` condition
    const STAGED_VALUE_PREFIX: &'static str = "staged:";

    /// The `TestStage` a resource never leaves, as a task never leaves
    /// `Finished`
    pub const FINAL_STAGE: &'static str = "done";
}

impl ResourceSchemaProvider for TestResource {
    fn type_uri() -> &'static TypeUri {
        static TEST_RESOURCE_SCHEMA: LazyLock<TypeUri> =
            LazyLock::new(|| TypeUri::new_unchecked(TestResource::SCHEMA_STR));
        &TEST_RESOURCE_SCHEMA
    }
}

impl DeclarativeResource for TestResource {
    type Spec = TestResourceSpec;
    type SpecInput = TestResourceSpec;
    type ResourceState = TestResourceState;

    const GENERATED_NAME_PREFIX: Option<fn(&TestResourceSpec) -> String> = Some(|spec| {
        if spec.value.starts_with(Self::STAGED_VALUE_PREFIX) {
            "staged"
        } else {
            "plain"
        }
        .to_string()
    });
}

impl ResourceStatusRules for TestResource {
    fn initial_conditions(spec: &TestResourceSpec) -> ResourceConditions {
        let mut conditions = empty_resource_conditions();
        if let Some(stage) = spec.value.strip_prefix(Self::STAGED_VALUE_PREFIX) {
            conditions
                .entries
                .insert(test_stage_condition_key(), stage.into());
        }
        conditions
    }

    fn validate_status_change(
        old: &ResourceStatus,
        new: &ResourceStatus,
    ) -> Result<(), ResourceStatusChangeRejectedError> {
        let key = test_stage_condition_key();
        let old_stage = old.conditions.entries.get(&key);
        let final_stage = serde_json::Value::from(Self::FINAL_STAGE);

        if old_stage == Some(&final_stage) && new.conditions.entries.get(&key) != old_stage {
            return Err(ResourceStatusChangeRejectedError::new(format!(
                "stage '{}' is final",
                Self::FINAL_STAGE
            )));
        }
        Ok(())
    }
}

impl ResourcePresentation for TestResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        Self::CANONICAL_SELECTOR_NAME,
        Self::SELECTOR_ALIASES,
        &[ResourceListColumnDefinition {
            key: "value",
            header: "Value",
            data_type: ResourceListColumnDataType::String,
            visibility: ResourceListColumnVisibility::Default,
        }],
    );

    fn list_column_values(state: &Self::ResourceState) -> Vec<ResourceListColumnValueView> {
        vec![ResourceListColumnValueView {
            key: "value".to_string(),
            value: ResourceListColumnValue::String(state.spec().value.clone()),
        }]
    }
}

kamu_resources::impl_event_sourced_resource!(
    resource = TestResource,
    reconcile_error = TestResourceReconcileError,
    lifecycle_error = TestResourceLifecycleError,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceAppendingPreApply
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceAppendingPreApply {
    pub suffix: String,
}

#[async_trait::async_trait]
impl ResourceSpecPreApply<TestResource> for TestResourceAppendingPreApply {
    async fn pre_apply(
        &self,
        mut new_spec: TestResourceSpec,
        _context: ResourceSpecPreApplyContext<'_, TestResource>,
    ) -> Result<ResourceSpecPreApplyOutcome<TestResource>, internal_error::InternalError> {
        if !new_spec.value.ends_with(&self.suffix) {
            new_spec.value.push_str(&self.suffix);
        }
        Ok(ResourceSpecPreApplyOutcome::Prepared(new_spec))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceClearingPreApply
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceClearingPreApply;

#[async_trait::async_trait]
impl ResourceSpecPreApply<TestResource> for TestResourceClearingPreApply {
    async fn pre_apply(
        &self,
        mut new_spec: TestResourceSpec,
        _context: ResourceSpecPreApplyContext<'_, TestResource>,
    ) -> Result<ResourceSpecPreApplyOutcome<TestResource>, internal_error::InternalError> {
        new_spec.value.clear();
        Ok(ResourceSpecPreApplyOutcome::Prepared(new_spec))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service layer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const TEST_RESOURCE_SIMPLE_CONTROLLER_NAME: &str = "test-resource-simple-controller";

crate::declare_resource_service_layer!(
    domain = crate::testing,
    name = TestResource,
    resource = TestResource,
    state_model = TestResourceStateModel,
    controller_name = TEST_RESOURCE_SIMPLE_CONTROLLER_NAME,
    controller_revision = 1
);

declare_resource_crud_dispatcher!(
    dispatcher = TestResourceCrudDispatcher,
    resource = TestResource
);

declare_resource_presentation_dispatcher!(
    dispatcher = TestResourcePresentationDispatcher,
    resource = TestResource
);

pub fn register_test_resource_crud_dispatcher(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<TestResourceCrudDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Everything the type needs to be served beside the real ones, reconciled by
/// a stub
pub fn register_test_resource(catalog_builder: &mut dill::CatalogBuilder) {
    register_test_resource_resource_service_layer(catalog_builder);
    register_test_resource_crud_dispatcher(catalog_builder);
    catalog_builder.add::<TestResourcePresentationDispatcher>();
    register_test_condition_schema_dispatchers(catalog_builder);
    catalog_builder.add::<TestResourceReconciler>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
