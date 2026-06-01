// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;
use kamu_resources::*;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceSpec
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TestResourceSpec {
    pub value: String,
}

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceStatus
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResourceStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,
}

impl Default for TestResourceStatus {
    fn default() -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
        }
    }
}

impl ResourceStatusLike for TestResourceStatus {
    fn resource_status(&self) -> &ResourceStatus {
        &self.resource_status
    }

    fn resource_status_mut(&mut self) -> &mut ResourceStatus {
        &mut self.resource_status
    }
}

impl PendingStatusFromSpec<TestResourceSpec> for TestResourceStatus {
    fn pending_from_spec(_spec: &TestResourceSpec) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
        }
    }

    fn reset_pending_from_spec(&mut self, _spec: &TestResourceSpec) {
        self.resource_status = ResourceStatus::new_pending();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceStatusProjector
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceStatusProjector;

impl ReconcilableStatusProjector<TestResourceSpec, (), String> for TestResourceStatusProjector {
    type Status = TestResourceStatus;

    fn on_reconciliation_succeeded(status: &mut Self::Status, _success: ()) {
        status.resource_status.phase = ResourcePhase::Ready;
    }

    fn on_reconciliation_failed(status: &mut Self::Status, _details: String) {
        status.resource_status.phase = ResourcePhase::Failed;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceStateModel
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceStateModel;

impl ReconcilableStateModel for TestResourceStateModel {
    type Spec = TestResourceSpec;
    type Status = TestResourceStatus;
    type Success = ();
    type FailureDetails = String;
    type State = TestResourceState;
    type StatusProjector = TestResourceStatusProjector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceState
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type TestResourceState = ReconcilableResourceState<TestResourceStateModel>;

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
    MetadataValidation(#[from] ResourceMetadataValidationError),

    #[error(transparent)]
    SpecValidation(#[from] TestResourceSpecValidationError),

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<TestResourceState>>),
}

kamu_resources::impl_invariant_violation_lifecycle_error!(
    TestResourceLifecycleError,
    TestResourceState
);

impl IntoApplyResourceRejection for TestResourceLifecycleError {
    fn into_apply_resource_rejection(self) -> ApplyResourceLifecycleErrorHandling {
        match self {
            Self::MetadataValidation(err) => {
                ApplyResourceLifecycleErrorHandling::Rejected(ApplyResourceRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    message: err.to_string(),
                })
            }
            Self::SpecValidation(err) => {
                ApplyResourceLifecycleErrorHandling::Rejected(ApplyResourceRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    message: err.to_string(),
                })
            }
            Self::InvariantViolation(err) => {
                use internal_error::ErrorIntoInternal;
                ApplyResourceLifecycleErrorHandling::Technical(err.int_err())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceReconcileError
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum TestResourceReconcileError {
    #[error(transparent)]
    ConcurrentModification(event_sourcing::ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] internal_error::InternalError),
}

impl ResourceReconcileError for TestResourceReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::ConcurrentModification(_) => "concurrent_modification",
            Self::Internal(_) => "internal_error",
        }
    }

    fn user_message(&self) -> String {
        match self {
            Self::ConcurrentModification(_) => {
                "Resource was modified concurrently during reconciliation.".to_owned()
            }
            Self::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        true
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResource
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct TestResource(pub(crate) Aggregate<TestResourceState, dyn TestResourceEventStore>);

impl ResourceType for TestResource {
    const RESOURCE_TYPE: &'static str = "TestResource";
}

impl ResourceApiVersion for TestResource {
    const API_VERSION: &'static str = "test.kamu.dev/v1";
}

impl DeclarativeResource for TestResource {
    type Spec = TestResourceSpec;
    type Status = TestResourceStatus;
    type ResourceState = TestResourceState;
}

impl ResourcePresentation for TestResource {
    const PRESENTATION: ResourcePresentationDefinition = ResourcePresentationDefinition::new(
        "testresources",
        &[],
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

kamu_resources::impl_reconcilable_event_sourced_resource!(
    resource = TestResource,
    reconcile_success = (),
    reconcile_error = TestResourceReconcileError,
    reconcile_failure_details = String,
    lifecycle_error = TestResourceLifecycleError,
    reconcile_failure_details_fn = |error| { error.to_string() }
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TestResourceSpecSanitizer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceSpecSanitizer {
    pub suffix: String,
}

#[async_trait::async_trait]
impl ResourceSpecSanitizer<TestResource> for TestResourceSpecSanitizer {
    async fn sanitize_new_spec(
        &self,
        mut new_spec: TestResourceSpec,
        _maybe_current_spec: Option<&TestResourceSpec>,
    ) -> Result<TestResourceSpec, internal_error::InternalError> {
        new_spec.value.push_str(&self.suffix);
        Ok(new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestResourceSpecClearingSanitizer;

#[async_trait::async_trait]
impl ResourceSpecSanitizer<TestResource> for TestResourceSpecClearingSanitizer {
    async fn sanitize_new_spec(
        &self,
        mut new_spec: TestResourceSpec,
        _maybe_current_spec: Option<&TestResourceSpec>,
    ) -> Result<TestResourceSpec, internal_error::InternalError> {
        new_spec.value.clear();
        Ok(new_spec)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service layer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources_services::declare_resource_service_layer!(
    domain = crate::tests::utils,
    name = TestResource,
    resource = crate::tests::utils::TestResource,
    state_model = crate::tests::utils::TestResourceStateModel
);

kamu_resources_services::declare_resource_crud_dispatcher!(
    dispatcher = TestResourceCrudDispatcher,
    resource = crate::tests::utils::TestResource
);

pub fn register_test_resource_crud_dispatcher(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<TestResourceCrudDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
