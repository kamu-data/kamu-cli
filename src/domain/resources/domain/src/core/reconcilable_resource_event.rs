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
use event_sourcing::ProjectionEvent;
use serde::{Deserialize, Serialize};

use crate::{ResourceHeadersInput, ResourceID, ResourceName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails> {
    Created(ResourceEventCreated<TSpec>),
    HeadersUpdated(ResourceEventHeadersUpdated),
    SpecUpdated(ResourceEventSpecUpdated<TSpec>),
    Deleted(ResourceEventDeleted),
    ReconciliationStarted(ResourceEventReconciliationStarted),
    ReconciliationSucceeded(ResourceEventReconciliationSucceeded<TSuccess>),
    ReconciliationFailed(ResourceEventReconciliationFailed<TFailureDetails>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventCreated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub headers: ResourceHeadersInput,
    pub spec: TSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventHeadersUpdated {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub new_headers: ResourceHeadersInput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventSpecUpdated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub new_spec: TSpec,
    pub new_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventDeleted {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub tombstone_name: ResourceName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationStarted {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationSucceeded<TSuccess> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub generation: u64,
    pub success: TSuccess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventReconciliationFailed<TFailureDetails> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub generation: u64,
    pub reason: String,
    pub message: String,
    pub details: TFailureDetails,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TSuccess, TFailureDetails> ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails> {
    pub fn typename(&self) -> &'static str {
        match self {
            ReconcilableResourceEvent::Created(_) => "Created",
            ReconcilableResourceEvent::HeadersUpdated(_) => "HeadersUpdated",
            ReconcilableResourceEvent::SpecUpdated(_) => "SpecUpdated",
            ReconcilableResourceEvent::Deleted(_) => "Deleted",
            ReconcilableResourceEvent::ReconciliationStarted(_) => "ReconciliationStarted",
            ReconcilableResourceEvent::ReconciliationSucceeded(_) => "ReconciliationSucceeded",
            ReconcilableResourceEvent::ReconciliationFailed(_) => "ReconciliationFailed",
        }
    }

    pub fn id(&self) -> &ResourceID {
        match self {
            ReconcilableResourceEvent::Created(e) => &e.id,
            ReconcilableResourceEvent::HeadersUpdated(e) => &e.id,
            ReconcilableResourceEvent::SpecUpdated(e) => &e.id,
            ReconcilableResourceEvent::Deleted(e) => &e.id,
            ReconcilableResourceEvent::ReconciliationStarted(e) => &e.id,
            ReconcilableResourceEvent::ReconciliationSucceeded(e) => &e.id,
            ReconcilableResourceEvent::ReconciliationFailed(e) => &e.id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            ReconcilableResourceEvent::Created(e) => e.event_time,
            ReconcilableResourceEvent::HeadersUpdated(e) => e.event_time,
            ReconcilableResourceEvent::SpecUpdated(e) => e.event_time,
            ReconcilableResourceEvent::Deleted(e) => e.event_time,
            ReconcilableResourceEvent::ReconciliationStarted(e) => e.event_time,
            ReconcilableResourceEvent::ReconciliationSucceeded(e) => e.event_time,
            ReconcilableResourceEvent::ReconciliationFailed(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec, TSuccess, TFailureDetails> ProjectionEvent<ResourceID>
    for ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync + 'static,
    TSuccess: std::fmt::Debug + Clone + Send + Sync + 'static,
    TFailureDetails: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.id() == query
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
    TFailureDetails > ::HeadersUpdated(ResourceEventHeadersUpdated)
);
impl_enum_variant!(
    ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>::SpecUpdated(
        ResourceEventSpecUpdated<TSpec>
    )
);
impl_enum_variant!(
    ReconcilableResourceEvent < TSpec,
    TSuccess,
    TFailureDetails > ::Deleted(ResourceEventDeleted)
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
