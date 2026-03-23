// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;

use crate::{
    DeclarativeResource,
    ResourceMetadata,
    ResourceMetadataInput,
    ResourcePhase,
    ResourceReconcileError,
    ResourceValidateMetadata,
    ResourceValidateSpec,
    StorageEvent,
    StorageEventCreated,
    StorageEventMetadataUpdated,
    StorageEventReconciliationFailed,
    StorageEventReconciliationStarted,
    StorageEventReconciliationSucceeded,
    StorageEventSpecUpdated,
    StorageEventStore,
    StorageID,
    StorageLifecycleError,
    StorageReconcileError,
    StorageReconcileSuccess,
    StorageReferenceStatus,
    StorageSpec,
    StorageState,
    StorageStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct StorageResource(Aggregate<StorageState, StorageEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type StorageEventStoreStatic = dyn StorageEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageResource {
    pub fn try_create(
        now: DateTime<Utc>,
        storage_id: StorageID,
        metadata: ResourceMetadataInput,
        spec: StorageSpec,
    ) -> Result<Self, StorageLifecycleError> {
        metadata.validate()?;
        spec.validate()?;

        let event = StorageEvent::Created(StorageEventCreated {
            event_time: now,
            storage_id,
            metadata,
            spec,
        });

        Aggregate::new(storage_id, event)
            .map(Self)
            .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }

    pub fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Result<(), StorageLifecycleError> {
        new_metadata.validate()?;

        if self.metadata().is_equivalent_to(&new_metadata) {
            return Ok(());
        }

        self.apply(StorageEvent::MetadataUpdated(StorageEventMetadataUpdated {
            event_time: now,
            storage_id: self.id,
            new_metadata,
        }))
        .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: StorageSpec,
    ) -> Result<(), StorageLifecycleError> {
        new_spec.validate()?;

        if self.spec() == &new_spec {
            return Ok(());
        }

        self.apply(StorageEvent::SpecUpdated(StorageEventSpecUpdated {
            event_time: now,
            storage_id: self.id,
            new_spec,
            new_generation: self.metadata().generation + 1,
        }))
        .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }

    pub fn needs_reconciliation(&self) -> bool {
        self.status()
            .resource_status
            .needs_reconciliation(self.metadata().generation)
    }

    pub fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), StorageLifecycleError> {
        if !self.needs_reconciliation() {
            return Ok(());
        }

        if self.status().resource_status.phase == ResourcePhase::Reconciling {
            return Ok(());
        }

        self.apply(StorageEvent::ReconciliationStarted(
            StorageEventReconciliationStarted {
                event_time: now,
                storage_id: self.id,
                generation: self.metadata().generation,
            },
        ))
        .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }

    pub fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: StorageReconcileSuccess,
    ) -> Result<(), StorageLifecycleError> {
        if self.metadata().generation != expected_generation {
            return Err(StorageLifecycleError::InvariantViolation(format!(
                "reconciliation success generation mismatch: expected {}, current {}",
                expected_generation,
                self.metadata().generation
            )));
        }

        self.apply(StorageEvent::ReconciliationSucceeded(
            StorageEventReconciliationSucceeded {
                event_time: now,
                storage_id: self.id,
                generation: expected_generation,
                success,
            },
        ))
        .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }

    pub fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &StorageReconcileError,
    ) -> Result<(), StorageLifecycleError> {
        if self.metadata().generation != expected_generation {
            return Err(StorageLifecycleError::InvariantViolation(format!(
                "reconciliation failure generation mismatch: expected {}, current {}",
                expected_generation,
                self.metadata().generation
            )));
        }

        self.apply(StorageEvent::ReconciliationFailed(
            StorageEventReconciliationFailed {
                event_time: now,
                storage_id: self.id,
                generation: expected_generation,
                reason: error.reason_code().to_string(),
                message: error.user_message(),
                references: StorageReferenceStatus::default(),
            },
        ))
        .map_err(|e| StorageLifecycleError::InvariantViolation(e.to_string()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for StorageResource {
    type Identity = StorageID;
    type Spec = StorageSpec;
    type Status = StorageStatus;

    fn id(&self) -> &Self::Identity {
        &self.as_ref().id
    }

    fn metadata(&self) -> &ResourceMetadata {
        &self.as_ref().metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.as_ref().spec
    }

    fn status(&self) -> &Self::Status {
        &self.as_ref().status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
