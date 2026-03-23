// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

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
    StorageEvent,
    StorageFailureDetails,
    StorageLifecycleError,
    StorageProviderKind,
    StorageReferenceStatus,
    StorageResource,
    try_mark_resource_reconciliation_failed,
    try_mark_resource_reconciliation_started,
    try_mark_resource_reconciliation_succeeded,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageReconcileSuccess {
    pub provider_kind: StorageProviderKind,
    pub references: StorageReferenceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum StorageReconcileError {
    #[error("referenced variable '{name}' not found")]
    MissingVariableRef { name: String },

    #[error("referenced secret '{name}' not found")]
    MissingSecretRef { name: String },

    #[error("storage provider configuration is invalid: {message}")]
    InvalidConfiguration { message: String },

    #[error("storage backend dependency unavailable: {message}")]
    DependencyUnavailable { message: String },

    #[error(transparent)]
    Internal(InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceReconcileError for StorageReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            Self::MissingVariableRef { .. } => "MissingVariableRef",
            Self::MissingSecretRef { .. } => "MissingSecretRef",
            Self::InvalidConfiguration { .. } => "InvalidConfiguration",
            Self::DependencyUnavailable { .. } => "DependencyUnavailable",
            Self::Internal(_) => "InternalError",
        }
    }

    fn user_message(&self) -> String {
        self.to_string()
    }

    fn is_transient(&self) -> bool {
        matches!(self, Self::DependencyUnavailable { .. } | Self::Internal(_))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for StorageResource {
    type ReconcileSuccess = StorageReconcileSuccess;
    type ReconcileError = StorageReconcileError;
    type LifecycleError = StorageLifecycleError;

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

impl ReconcilableResourceEventFactory for StorageResource {
    type Event = StorageEvent;

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
            details: StorageFailureDetails {
                references: StorageReferenceStatus::default(),
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl AppliesTypedEvent<StorageEvent> for StorageResource {
    type LifecycleError = StorageLifecycleError;

    fn apply_typed_event(&mut self, event: StorageEvent) -> Result<(), Self::LifecycleError> {
        self.apply(event)
            .map_err(|e| StorageLifecycleError::InvariantViolation(Box::new(e)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
