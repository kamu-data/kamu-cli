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
    ResourceID,
    ResourceMetadata,
    ResourceValidateSpec,
    VariableSetEvent,
    VariableSetEventCreated,
    VariableSetEventSpecUpdated,
    VariableSetEventStore,
    VariableSetLifecycleError,
    VariableSetSpec,
    VariableSetState,
    VariableSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct VariableSetResource(Aggregate<VariableSetState, VariableSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type VariableSetID = ResourceID;

type VariableSetEventStoreStatic = dyn VariableSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetResource {
    pub fn try_create(
        now: DateTime<Utc>,
        variable_set_id: VariableSetID,
        name: String,
        spec: VariableSetSpec,
    ) -> Result<Self, VariableSetLifecycleError> {
        spec.validate()?;

        Ok(Self(
            Aggregate::new(
                variable_set_id,
                VariableSetEventCreated {
                    event_time: now,
                    variable_set_id,
                    name,
                    spec,
                },
            )
            .unwrap(),
        ))
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: VariableSetSpec,
    ) -> Result<(), VariableSetLifecycleError> {
        new_spec.validate()?;

        if self.spec == new_spec {
            return Err(VariableSetLifecycleError::NoChanges);
        }

        let event = VariableSetEvent::SpecUpdated(VariableSetEventSpecUpdated {
            event_time: now,
            variable_set_id: self.metadata.uid,
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
