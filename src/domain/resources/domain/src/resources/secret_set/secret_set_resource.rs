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
    ResourceValidateMetadata,
    ResourceValidateSpec,
    SecretSetEvent,
    SecretSetEventCreated,
    SecretSetEventMetadataUpdated,
    SecretSetEventSpecUpdated,
    SecretSetEventStore,
    SecretSetID,
    SecretSetLifecycleError,
    SecretSetSpec,
    SecretSetState,
    SecretSetStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct SecretSetResource(Aggregate<SecretSetState, SecretSetEventStoreStatic>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SecretSetEventStoreStatic = dyn SecretSetEventStore + 'static;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetResource {
    pub fn try_create(
        now: DateTime<Utc>,
        secret_set_id: SecretSetID,
        metadata: ResourceMetadataInput,
        spec: SecretSetSpec,
    ) -> Result<Self, SecretSetLifecycleError> {
        metadata.validate()?;
        spec.validate()?;

        Ok(Self(
            Aggregate::new(
                secret_set_id,
                SecretSetEventCreated {
                    event_time: now,
                    secret_set_id,
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
    ) -> Result<(), SecretSetLifecycleError> {
        if self.metadata.is_equivalent_to(&new_metadata) {
            return Ok(()); // No changes, skip update
        }

        new_metadata.validate()?;

        let event = SecretSetEvent::MetadataUpdated(SecretSetEventMetadataUpdated {
            event_time: now,
            secret_set_id: self.id,
            new_metadata,
        });

        self.apply(event)
            .map_err(|e| SecretSetLifecycleError::InvariantViolation(Box::new(e)))?;

        Ok(())
    }

    pub fn try_update_spec(
        &mut self,
        now: DateTime<Utc>,
        new_spec: SecretSetSpec,
    ) -> Result<(), SecretSetLifecycleError> {
        if self.spec == new_spec {
            return Ok(()); // No changes, skip update
        }

        new_spec.validate()?;

        let event = SecretSetEvent::SpecUpdated(SecretSetEventSpecUpdated {
            event_time: now,
            secret_set_id: self.id,
            new_spec,
            new_generation: self.metadata.generation + 1,
        });

        self.apply(event)
            .map_err(|e| SecretSetLifecycleError::InvariantViolation(Box::new(e)))?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DeclarativeResource for SecretSetResource {
    type Spec = SecretSetSpec;
    type Status = SecretSetStatus;

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
