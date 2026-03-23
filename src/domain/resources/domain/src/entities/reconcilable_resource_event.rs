// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;
use serde::{Deserialize, Serialize};

use crate::{ResourceID, ResourceMetadataInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails> {
    Created(ResourceEventCreated<TSpec>),
    MetadataUpdated(ResourceEventMetadataUpdated),
    SpecUpdated(ResourceEventSpecUpdated<TSpec>),
    ReconciliationStarted(ResourceEventReconciliationStarted),
    ReconciliationSucceeded(ResourceEventReconciliationSucceeded<TSuccess>),
    ReconciliationFailed(ResourceEventReconciliationFailed<TFailureDetails>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventCreated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub metadata: ResourceMetadataInput,
    pub spec: TSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventMetadataUpdated {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub new_metadata: ResourceMetadataInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventSpecUpdated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub new_spec: TSpec,
    pub new_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationStarted {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationSucceeded<TSuccess> {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub generation: u64,
    pub success: TSuccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationFailed<TFailureDetails> {
    pub event_time: DateTime<Utc>,
    pub resource_id: ResourceID,
    pub generation: u64,
    pub reason: String,
    pub message: String,
    pub details: TFailureDetails,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TSuccess, TFailureDetails> ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails> {
    pub fn resource_id(&self) -> &ResourceID {
        match self {
            ReconcilableResourceEvent::Created(e) => &e.resource_id,
            ReconcilableResourceEvent::MetadataUpdated(e) => &e.resource_id,
            ReconcilableResourceEvent::SpecUpdated(e) => &e.resource_id,
            ReconcilableResourceEvent::ReconciliationStarted(e) => &e.resource_id,
            ReconcilableResourceEvent::ReconciliationSucceeded(e) => &e.resource_id,
            ReconcilableResourceEvent::ReconciliationFailed(e) => &e.resource_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>);
impl_enum_variant!(
    ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>::Created(
        ResourceEventCreated<TSpec>
    )
);
impl_enum_variant!(
    ReconcilableResourceEvent < TSpec,
    TSuccess,
    TFailureDetails > ::MetadataUpdated(ResourceEventMetadataUpdated)
);
impl_enum_variant!(
    ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>::SpecUpdated(
        ResourceEventSpecUpdated<TSpec>
    )
);
impl_enum_variant!(
    ReconcilableResourceEvent < TSpec,
    TSuccess,
    TFailureDetails > ::ReconciliationStarted(ResourceEventReconciliationStarted)
);
impl_enum_variant!(
    ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>::ReconciliationSucceeded(
        ResourceEventReconciliationSucceeded<TSuccess>
    )
);
impl_enum_variant!(
    ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>::ReconciliationFailed(
        ResourceEventReconciliationFailed<TFailureDetails>
    )
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
