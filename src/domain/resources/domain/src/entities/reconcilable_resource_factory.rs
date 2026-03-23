// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{ReconcilableResource, ResourceID, ResourceMetadataInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ReconcilableResourceEventFactory: ReconcilableResource {
    type Event;

    fn created_event(
        now: DateTime<Utc>,
        resource_id: ResourceID,
        metadata: ResourceMetadataInput,
        spec: Self::Spec,
    ) -> Self::Event;

    fn metadata_updated_event(
        &self,
        now: DateTime<Utc>,
        new_metadata: ResourceMetadataInput,
    ) -> Self::Event;

    fn spec_updated_event(
        &self,
        now: DateTime<Utc>,
        new_spec: Self::Spec,
        new_generation: u64,
    ) -> Self::Event;

    fn reconciliation_started_event(&self, now: DateTime<Utc>) -> Self::Event;

    fn reconciliation_succeeded_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Self::Event;

    fn reconciliation_failed_event(
        &self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcileError,
    ) -> Self::Event;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait AppliesTypedEvent<E> {
    type LifecycleError;

    fn apply_typed_event(&mut self, event: E) -> Result<(), Self::LifecycleError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
