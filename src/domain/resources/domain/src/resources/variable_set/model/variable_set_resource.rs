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
    ResourceValidateMetadata,
    ResourceValidateSpec,
    VariableSetEvent,
    VariableSetEventCreated,
    VariableSetEventMetadataUpdated,
    VariableSetEventReconciliationFailed,
    VariableSetEventReconciliationStarted,
    VariableSetEventReconciliationSucceeded,
    VariableSetEventSpecUpdated,
    VariableSetEventStore,
    VariableSetID,
    VariableSetLifecycleError,
    VariableSetSpec,
    VariableSetState,
    VariableSetStats,
    VariableSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct VariableSetResource(Aggregate<VariableSetState, VariableSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type VariableSetEventStoreStatic = dyn VariableSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetResource {
    pub fn try_create(
        now: DateTime<Utc>,
        variable_set_id: VariableSetID,
        metadata: ResourceMetadataInput,
        spec: VariableSetSpec,
    ) -> Result<Self, VariableSetLifecycleError> {
        metadata.validate()?;
        spec.validate()?;

        Ok(Self(
            Aggregate::new(
                variable_set_id,
                VariableSetEventCreated {
                    event_time: now,
                    variable_set_id,
                    metadata,
                    spec,
                },
            )
            .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))?,
        ))
    }

    pub fn try_update_metadata(
        &mut self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Result<(), VariableSetLifecycleError> {
        if self.metadata.is_equivalent_to(&new_metadata) {
            return Ok(()); // No changes, skip update
        }

        new_metadata.validate()?;

        let event = VariableSetEvent::MetadataUpdated(VariableSetEventMetadataUpdated {
            event_time: now,
            variable_set_id: self.id,
            new_metadata,
        });

        self.apply(event)
            .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))?;

        Ok(())
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: VariableSetSpec,
    ) -> Result<(), VariableSetLifecycleError> {
        if self.spec == new_spec {
            return Ok(()); // No changes, skip update
        }

        new_spec.validate()?;

        let event = VariableSetEvent::SpecUpdated(VariableSetEventSpecUpdated {
            event_time: now,
            variable_set_id: self.id,
            new_spec,
            new_generation: self.metadata.generation + 1,
        });

        self.apply(event)
            .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))?;

        Ok(())
    }

    pub fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), VariableSetLifecycleError> {
        if self.status().resource_status.observed_generation == self.metadata().generation {
            return Ok(());
        }

        if self.status().resource_status.phase == ResourcePhase::Reconciling {
            return Ok(());
        }

        self.apply(VariableSetEvent::ReconciliationStarted(
            VariableSetEventReconciliationStarted {
                event_time: now,
                variable_set_id: self.id,
                generation: self.metadata().generation,
            },
        ))
        .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))
    }

    pub fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        stats: VariableSetStats,
    ) -> Result<(), VariableSetLifecycleError> {
        if self.metadata().generation != expected_generation {
            tracing::warn!(
                expected_generation,
                current_generation = self.metadata().generation,
                "Attempting to mark reconciliation succeeded for wrong resource generation.",
            );
            return Ok(()); // Skip update if generation doesn't match
        }

        self.apply(VariableSetEvent::ReconciliationSucceeded(
            VariableSetEventReconciliationSucceeded {
                event_time: now,
                variable_set_id: self.id,
                generation: self.metadata().generation,
                stats,
            },
        ))
        .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))
    }

    pub fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        reason: String,
        message: String,
        stats: VariableSetStats,
    ) -> Result<(), VariableSetLifecycleError> {
        if self.metadata().generation != expected_generation {
            tracing::warn!(
                expected_generation,
                current_generation = self.metadata().generation,
                "Attempting to mark reconciliation failed for wrong resource generation.",
            );
            return Ok(()); // Skip update if generation doesn't match
        }

        self.apply(VariableSetEvent::ReconciliationFailed(
            VariableSetEventReconciliationFailed {
                event_time: now,
                variable_set_id: self.id,
                generation: self.metadata().generation,
                reason,
                message,
                stats,
            },
        ))
        .map_err(|e| VariableSetLifecycleError::InvariantViolation(Box::new(e)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for VariableSetResource {
    type Identity = VariableSetID;
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;

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
