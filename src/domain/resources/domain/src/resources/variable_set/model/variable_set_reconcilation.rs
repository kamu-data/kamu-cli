// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    AppliesTypedEvent,
    DeclarativeResource,
    ReconcilableResource,
    ReconcilableResourceEvent,
    ReconcilableResourceEventFactory,
    ResourceEventCreated,
    ResourceEventMetadataUpdated,
    ResourceEventReconciliationFailed,
    ResourceEventReconciliationStarted,
    ResourceEventReconciliationSucceeded,
    ResourceEventSpecUpdated,
    ResourceID,
    ResourceMetadataInput,
    ResourceReconcileError,
    VariableSetEvent,
    VariableSetFailureDetails,
    VariableSetLifecycleError,
    VariableSetResource,
    VariableSetStats,
    try_mark_resource_reconciliation_failed,
    try_mark_resource_reconciliation_started,
    try_mark_resource_reconciliation_succeeded,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for VariableSetResource {
    type ReconcileSuccess = VariableSetReconcileSuccess;
    type ReconcileError = VariableSetReconcileError;
    type LifecycleError = VariableSetLifecycleError;

    fn needs_reconciliation(&self) -> bool {
        self.status()
            .resource_status
            .needs_reconciliation(self.metadata().generation)
    }

    fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), Self::LifecycleError> {
        try_mark_resource_reconciliation_started(self, now)
    }

    fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Result<(), Self::LifecycleError> {
        try_mark_resource_reconciliation_succeeded(self, now, expected_generation, success)
    }

    fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> Result<(), Self::LifecycleError> {
        try_mark_resource_reconciliation_failed(self, now, expected_generation, error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResourceEventFactory for VariableSetResource {
    type Event = VariableSetEvent;

    fn created_event(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: Self::Spec,
    ) -> Self::Event {
        ReconcilableResourceEvent::Created(ResourceEventCreated {
            event_time: now,
            resource_id,
            metadata,
            spec,
        })
    }

    fn metadata_updated_event(
        &self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Self::Event {
        ReconcilableResourceEvent::MetadataUpdated(ResourceEventMetadataUpdated {
            event_time: now,
            resource_id: *self.resource_id(),
            new_metadata,
        })
    }

    fn spec_updated_event(
        &self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
        new_generation: u64,
    ) -> Self::Event {
        ReconcilableResourceEvent::SpecUpdated(ResourceEventSpecUpdated {
            event_time: now,
            resource_id: *self.resource_id(),
            new_spec,
            new_generation,
        })
    }

    fn reconciliation_started_event(&self, now: DateTime<Utc>) -> Self::Event {
        ReconcilableResourceEvent::ReconciliationStarted(ResourceEventReconciliationStarted {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: self.metadata().generation,
        })
    }

    fn reconciliation_succeeded_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Self::Event {
        ReconcilableResourceEvent::ReconciliationSucceeded(ResourceEventReconciliationSucceeded {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: expected_generation,
            success,
        })
    }

    fn reconciliation_failed_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> Self::Event {
        ReconcilableResourceEvent::ReconciliationFailed(ResourceEventReconciliationFailed {
            event_time: now,
            resource_id: *self.resource_id(),
            generation: expected_generation,
            reason: error.reason_code().to_string(),
            message: error.user_message(),
            details: VariableSetFailureDetails {
                stats: VariableSetStats::default(),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AppliesTypedEvent<VariableSetEvent> for VariableSetResource {
    type LifecycleError = VariableSetLifecycleError;

    fn apply_typed_event(&mut self, event: VariableSetEvent) -> Result<(), Self::LifecycleError> {
        self.apply(event)
            .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct VariableSetReconcileSuccess {
    pub stats: VariableSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetReconcileError {
    #[error("Reference missing: {name}")]
    ReferenceMissing { name: String },

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ResourceReconcileError for VariableSetReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            VariableSetReconcileError::ReferenceMissing { .. } => "reference_missing",
            VariableSetReconcileError::Internal(_) => "internal_error",
        }
    }

    fn user_message(&self) -> String {
        match self {
            VariableSetReconcileError::ReferenceMissing { name } => {
                format!("Referenced resource '{name}' is missing.")
            }
            VariableSetReconcileError::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            VariableSetReconcileError::ReferenceMissing { .. } => false,
            VariableSetReconcileError::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
