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

use crate::{ResourceHeadersInput, ResourceID, ResourceName, ResourceStatus};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
pub enum ResourceEvent<TSpec> {
    Created(ResourceEventCreated<TSpec>),
    HeadersUpdated(ResourceEventHeadersUpdated),
    SpecUpdated(ResourceEventSpecUpdated<TSpec>),
    StatusUpdated(ResourceEventStatusUpdated),
    Deleted(ResourceEventDeleted),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventCreated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceHeadersInput")]
    pub headers: ResourceHeadersInput,
    pub spec: TSpec,
}

#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventHeadersUpdated {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceHeadersInput")]
    pub new_headers: ResourceHeadersInput,
    /// Absent in events stored before header updates bumped the generation;
    /// such events leave it unchanged.
    #[serde(default)]
    pub new_generation: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventSpecUpdated<TSpec> {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub new_spec: TSpec,
    pub new_generation: u64,
}

/// A controller replaced the status of the resource.
#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventStatusUpdated {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceStatus")]
    pub new_status: ResourceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEventDeleted {
    pub event_time: DateTime<Utc>,
    pub id: ResourceID,
    pub tombstone_name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec> ResourceEvent<TSpec> {
    pub fn typename(&self) -> &'static str {
        self.into()
    }

    pub fn id(&self) -> &ResourceID {
        match self {
            ResourceEvent::Created(e) => &e.id,
            ResourceEvent::HeadersUpdated(e) => &e.id,
            ResourceEvent::SpecUpdated(e) => &e.id,
            ResourceEvent::StatusUpdated(e) => &e.id,
            ResourceEvent::Deleted(e) => &e.id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            ResourceEvent::Created(e) => e.event_time,
            ResourceEvent::HeadersUpdated(e) => e.event_time,
            ResourceEvent::SpecUpdated(e) => e.event_time,
            ResourceEvent::StatusUpdated(e) => e.event_time,
            ResourceEvent::Deleted(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<TSpec> ProjectionEvent<ResourceID> for ResourceEvent<TSpec>
where
    TSpec: std::fmt::Debug + Clone + Send + Sync + 'static,
{
    fn matches_query(&self, query: &ResourceID) -> bool {
        self.id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(ResourceEvent<TSpec>);
impl_enum_variant!(ResourceEvent<TSpec>::Created(ResourceEventCreated<TSpec>));
impl_enum_variant!(ResourceEvent<TSpec>::HeadersUpdated(ResourceEventHeadersUpdated));
impl_enum_variant!(ResourceEvent<TSpec>::SpecUpdated(ResourceEventSpecUpdated<TSpec>));
impl_enum_variant!(ResourceEvent<TSpec>::StatusUpdated(ResourceEventStatusUpdated));
impl_enum_variant!(ResourceEvent<TSpec>::Deleted(ResourceEventDeleted));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
