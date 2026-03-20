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

use crate::{ResourceMetadataInput, VariableSetID, VariableSetSpec, VariableSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a variable set
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum VariableSetEvent {
    Created(VariableSetEventCreated),
    MetadataUpdated(VariableSetEventMetadataUpdated),
    SpecUpdated(VariableSetEventSpecUpdated),
    ReconciliationStarted(VariableSetEventReconciliationStarted),
    ReconciliationSucceeded(VariableSetEventReconciliationSucceeded),
    ReconciliationFailed(VariableSetEventReconciliationFailed),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventCreated {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub metadata: ResourceMetadataInput,
    pub spec: VariableSetSpec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventMetadataUpdated {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub new_metadata: ResourceMetadataInput,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventSpecUpdated {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub new_spec: VariableSetSpec,
    pub new_generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventReconciliationStarted {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventReconciliationSucceeded {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub generation: u64,
    pub stats: VariableSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetEventReconciliationFailed {
    pub event_time: DateTime<Utc>,
    pub variable_set_id: VariableSetID,
    pub generation: u64,
    pub reason: String,
    pub message: String,
    pub stats: VariableSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetEvent {
    pub fn variable_set_id(&self) -> &VariableSetID {
        match self {
            VariableSetEvent::Created(e) => &e.variable_set_id,
            VariableSetEvent::MetadataUpdated(e) => &e.variable_set_id,
            VariableSetEvent::SpecUpdated(e) => &e.variable_set_id,
            VariableSetEvent::ReconciliationStarted(e) => &e.variable_set_id,
            VariableSetEvent::ReconciliationSucceeded(e) => &e.variable_set_id,
            VariableSetEvent::ReconciliationFailed(e) => &e.variable_set_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(VariableSetEvent);
impl_enum_variant!(VariableSetEvent::Created(VariableSetEventCreated));
impl_enum_variant!(VariableSetEvent::MetadataUpdated(
    VariableSetEventMetadataUpdated
));
impl_enum_variant!(VariableSetEvent::SpecUpdated(VariableSetEventSpecUpdated));
impl_enum_variant!(VariableSetEvent::ReconciliationStarted(
    VariableSetEventReconciliationStarted
));
impl_enum_variant!(VariableSetEvent::ReconciliationSucceeded(
    VariableSetEventReconciliationSucceeded
));
impl_enum_variant!(VariableSetEvent::ReconciliationFailed(
    VariableSetEventReconciliationFailed
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
