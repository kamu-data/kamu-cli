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
    SecretSetEvent,
    SecretSetFailureDetails,
    SecretSetLifecycleError,
    SecretSetResource,
    SecretSetStats,
    try_mark_resource_reconciliation_failed,
    try_mark_resource_reconciliation_started,
    try_mark_resource_reconciliation_succeeded,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for SecretSetResource {
    type ReconcileSuccess = SecretSetReconcileSuccess;
    type ReconcileError = SecretSetReconcileError;
    type LifecycleError = SecretSetLifecycleError;

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

impl ReconcilableResourceEventFactory for SecretSetResource {
    type Event = SecretSetEvent;

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
            details: SecretSetFailureDetails {
                stats: SecretSetStats::default(),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AppliesTypedEvent<SecretSetEvent> for SecretSetResource {
    type LifecycleError = SecretSetLifecycleError;

    fn apply_typed_event(&mut self, event: SecretSetEvent) -> Result<(), Self::LifecycleError> {
        self.apply(event)
            .map_err(|e| SecretSetLifecycleError::InvariantViolation(Box::new(e)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct SecretSetReconcileSuccess {
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SecretSetReconcileError {
    #[error("Reference missing: {name}")]
    ReferenceMissing { name: String },

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ResourceReconcileError for SecretSetReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            SecretSetReconcileError::ReferenceMissing { .. } => "reference_missing",
            SecretSetReconcileError::Internal(_) => "internal_error",
        }
    }

    fn user_message(&self) -> String {
        match self {
            SecretSetReconcileError::ReferenceMissing { name } => {
                format!("Referenced resource '{name}' is missing.")
            }
            SecretSetReconcileError::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            SecretSetReconcileError::ReferenceMissing { .. } => false,
            SecretSetReconcileError::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
