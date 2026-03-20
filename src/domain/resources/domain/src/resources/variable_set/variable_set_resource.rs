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
    ResourceValidateSpec,
    VariableSetEvent,
    VariableSetEventCreated,
    VariableSetEventMetadataUpdated,
    VariableSetEventSpecUpdated,
    VariableSetEventStore,
    VariableSetID,
    VariableSetLifecycleError,
    VariableSetSpec,
    VariableSetState,
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
            .unwrap(),
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
        new_spec.validate()?;

        if self.spec == new_spec {
            return Ok(()); // No changes, skip update
        }

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for VariableSetResource {
    type Spec = VariableSetSpec;
    type Status = VariableSetStatus;

    fn metadata(&self) -> &ResourceMetadata {
        &self.metadata
    }

    fn spec(&self) -> &Self::Spec {
        &self.spec
    }

    fn status(&self) -> &Self::Status {
        &self.status
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
