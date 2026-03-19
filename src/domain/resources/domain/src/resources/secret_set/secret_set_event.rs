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

use crate::{ResourceName, SecretSetID, SecretSetSpec, SecretSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecretSetEvent {
    Created(SecretSetEventCreated),
    SpecUpdated(SecretSetEventSpecUpdated),
    ReconciliationStarted(SecretSetEventReconciliationStarted),
    ReconciliationSucceeded(SecretSetEventReconciliationSucceeded),
    ReconciliationFailed(SecretSetEventReconciliationFailed),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetEventCreated {
    pub event_time: DateTime<Utc>,
    pub secret_set_id: SecretSetID,
    pub name: ResourceName,
    pub spec: SecretSetSpec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetEventSpecUpdated {
    pub event_time: DateTime<Utc>,
    pub secret_set_id: SecretSetID,
    pub new_spec: SecretSetSpec,
    pub new_generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetEventReconciliationStarted {
    pub event_time: DateTime<Utc>,
    pub secret_set_id: SecretSetID,
    pub generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetEventReconciliationSucceeded {
    pub event_time: DateTime<Utc>,
    pub secret_set_id: SecretSetID,
    pub generation: u64,
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretSetEventReconciliationFailed {
    pub event_time: DateTime<Utc>,
    pub secret_set_id: SecretSetID,
    pub generation: u64,
    pub reason: String,
    pub message: String,
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////\

impl SecretSetEvent {
    pub fn secret_set_id(&self) -> &SecretSetID {
        match self {
            Self::Created(e) => &e.secret_set_id,
            Self::SpecUpdated(e) => &e.secret_set_id,
            Self::ReconciliationStarted(e) => &e.secret_set_id,
            Self::ReconciliationSucceeded(e) => &e.secret_set_id,
            Self::ReconciliationFailed(e) => &e.secret_set_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////\

impl_enum_with_variants!(SecretSetEvent);
impl_enum_variant!(SecretSetEvent::Created(SecretSetEventCreated));
impl_enum_variant!(SecretSetEvent::SpecUpdated(SecretSetEventSpecUpdated));
impl_enum_variant!(SecretSetEvent::ReconciliationStarted(
    SecretSetEventReconciliationStarted
));
impl_enum_variant!(SecretSetEvent::ReconciliationSucceeded(
    SecretSetEventReconciliationSucceeded
));
impl_enum_variant!(SecretSetEvent::ReconciliationFailed(
    SecretSetEventReconciliationFailed
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
