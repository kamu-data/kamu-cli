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

use crate::{
    ResourceMetadataInput,
    StorageID,
    StorageReconcileSuccess,
    StorageReferenceStatus,
    StorageSpec,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageEvent {
    Created(StorageEventCreated),
    MetadataUpdated(StorageEventMetadataUpdated),
    SpecUpdated(StorageEventSpecUpdated),
    ReconciliationStarted(StorageEventReconciliationStarted),
    ReconciliationSucceeded(StorageEventReconciliationSucceeded),
    ReconciliationFailed(StorageEventReconciliationFailed),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventCreated {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub metadata: ResourceMetadataInput,
    pub spec: StorageSpec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventMetadataUpdated {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub new_metadata: ResourceMetadataInput,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventSpecUpdated {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub new_spec: StorageSpec,
    pub new_generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventReconciliationStarted {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub generation: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventReconciliationSucceeded {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub generation: u64,
    pub success: StorageReconcileSuccess,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEventReconciliationFailed {
    pub event_time: DateTime<Utc>,
    pub storage_id: StorageID,
    pub generation: u64,
    pub reason: String,
    pub message: String,
    pub references: StorageReferenceStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageEvent {
    pub fn storage_id(&self) -> &StorageID {
        match self {
            Self::Created(e) => &e.storage_id,
            Self::MetadataUpdated(e) => &e.storage_id,
            Self::SpecUpdated(e) => &e.storage_id,
            Self::ReconciliationStarted(e) => &e.storage_id,
            Self::ReconciliationSucceeded(e) => &e.storage_id,
            Self::ReconciliationFailed(e) => &e.storage_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(StorageEvent);
impl_enum_variant!(StorageEvent::Created(StorageEventCreated));
impl_enum_variant!(StorageEvent::MetadataUpdated(StorageEventMetadataUpdated));
impl_enum_variant!(StorageEvent::SpecUpdated(StorageEventSpecUpdated));
impl_enum_variant!(StorageEvent::ReconciliationStarted(
    StorageEventReconciliationStarted
));
impl_enum_variant!(StorageEvent::ReconciliationSucceeded(
    StorageEventReconciliationSucceeded
));
impl_enum_variant!(StorageEvent::ReconciliationFailed(
    StorageEventReconciliationFailed
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
