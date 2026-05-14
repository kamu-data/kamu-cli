// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::ResourceSnapshot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_LIFECYCLE_OUTBOX_VERSION: u32 = 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceLifecycleMessage {
    Applied(ResourceLifecycleMessageApplied),
    ReconciliationSucceeded(ResourceLifecycleMessageReconciliationSucceeded),
    ReconciliationFailed(ResourceLifecycleMessageReconciliationFailed),
    Deleted(ResourceLifecycleMessageDeleted),
}

impl ResourceLifecycleMessage {
    pub fn applied(
        event_time: DateTime<Utc>,
        outcome: ResourceLifecycleMessageOutcome,
        resource: ResourceSnapshot,
    ) -> Self {
        Self::Applied(ResourceLifecycleMessageApplied {
            event_time,
            outcome,
            resource,
        })
    }

    pub fn reconciliation_succeeded(event_time: DateTime<Utc>, resource: ResourceSnapshot) -> Self {
        Self::ReconciliationSucceeded(ResourceLifecycleMessageReconciliationSucceeded {
            event_time,
            resource,
        })
    }

    pub fn reconciliation_failed(event_time: DateTime<Utc>, resource: ResourceSnapshot) -> Self {
        Self::ReconciliationFailed(ResourceLifecycleMessageReconciliationFailed {
            event_time,
            resource,
        })
    }

    pub fn deleted(event_time: DateTime<Utc>, resources: Vec<ResourceSnapshot>) -> Self {
        debug_assert!(
            !resources.is_empty(),
            "deleted message must contain at least one resource"
        );

        debug_assert!(
            ResourceSnapshot::check_homogeneous(&resources),
            "all resources in a deleted message must share the same kind and api_version"
        );

        Self::Deleted(ResourceLifecycleMessageDeleted {
            event_time,
            resources,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Message for ResourceLifecycleMessage {
    fn version() -> u32 {
        RESOURCE_LIFECYCLE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceLifecycleMessageOutcome {
    Created,
    Updated,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageApplied {
    pub event_time: DateTime<Utc>,
    pub outcome: ResourceLifecycleMessageOutcome,
    pub resource: ResourceSnapshot,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageReconciliationSucceeded {
    pub event_time: DateTime<Utc>,
    pub resource: ResourceSnapshot,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageReconciliationFailed {
    pub event_time: DateTime<Utc>,
    pub resource: ResourceSnapshot,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageDeleted {
    pub event_time: DateTime<Utc>,
    // all resources share the same `kind` and `api_version`
    pub resources: Vec<ResourceSnapshot>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
