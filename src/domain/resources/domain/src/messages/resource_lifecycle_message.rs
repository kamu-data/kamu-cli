// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::{
    ResourceHeaders,
    ResourceID,
    ResourcePhase,
    ResourceSnapshot,
    ResourceStatus,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const RESOURCE_LIFECYCLE_OUTBOX_VERSION: u32 = 4;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceLifecycleMessage {
    Applied(ResourceLifecycleMessageApplied),
    DeletionRequested(ResourceLifecycleMessageDeletionRequested),
    Deleted(ResourceLifecycleMessageDeleted),
    StatusUpdated(ResourceLifecycleMessageStatusUpdated),
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

    pub fn deletion_requested(event_time: DateTime<Utc>, resources: Vec<ResourceSnapshot>) -> Self {
        debug_assert!(
            !resources.is_empty(),
            "deletion requested message must contain at least one resource"
        );

        debug_assert!(
            ResourceSnapshot::check_homogeneous(&resources),
            "all resources in a deletion requested message must share the same schema"
        );

        Self::DeletionRequested(ResourceLifecycleMessageDeletionRequested {
            event_time,
            resources,
        })
    }

    pub fn deleted(event_time: DateTime<Utc>, resources: Vec<ResourceSnapshot>) -> Self {
        debug_assert!(
            !resources.is_empty(),
            "deleted message must contain at least one resource"
        );

        debug_assert!(
            ResourceSnapshot::check_homogeneous(&resources),
            "all resources in a deleted message must share the same schema"
        );

        Self::Deleted(ResourceLifecycleMessageDeleted {
            event_time,
            resources,
        })
    }

    pub fn status_updated(
        event_time: DateTime<Utc>,
        resource: ResourceLifecycleMessageResourceRef,
        status: &ResourceStatus,
        changed_conditions: BTreeMap<TypeUri, ResourceLifecycleMessageConditionChange>,
    ) -> Self {
        Self::StatusUpdated(ResourceLifecycleMessageStatusUpdated {
            event_time,
            resource,
            status: ResourceLifecycleMessageStatus::from(status),
            changed_conditions,
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

/// Deletion of the resources was requested, possibly again for resources
/// already being deleted. The resources are in the `Deleting` phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageDeletionRequested {
    pub event_time: DateTime<Utc>,
    // all resources share the same `schema`
    pub resources: Vec<ResourceSnapshot>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The controllers cleaned up and the deletion of the resources completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageDeleted {
    pub event_time: DateTime<Utc>,
    // all resources share the same `schema`
    pub resources: Vec<ResourceSnapshot>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The status of the resource changed: a reconciliation started, succeeded or
/// failed, a cleanup failed, or a writer patched conditions. The message names
/// the resource without its spec and conditions, so a large condition never
/// travels with it
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageStatusUpdated {
    pub event_time: DateTime<Utc>,
    pub resource: ResourceLifecycleMessageResourceRef,
    pub status: ResourceLifecycleMessageStatus,
    /// Every condition the update set or removed
    pub changed_conditions: BTreeMap<TypeUri, ResourceLifecycleMessageConditionChange>,
}

impl ResourceLifecycleMessageStatusUpdated {
    /// Whether this update recorded the reconciliation of the resource's
    /// current generation. A later update of a reconciled resource, such as a
    /// condition patch, keeps the earlier `reconciledAt` and does not count
    pub fn records_reconciliation_of_current_generation(&self) -> bool {
        self.status.phase == ResourcePhase::Ready
            && self.status.reconciled_generation == Some(self.resource.headers.generation)
            && self.status.reconciled_at == Some(self.event_time)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The status fields other than the conditions, under the keys the resource
/// status uses
#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLifecycleMessageStatus {
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourcePhase")]
    pub phase: ResourcePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciled_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciled_at: Option<DateTime<Utc>>,
}

impl From<&ResourceStatus> for ResourceLifecycleMessageStatus {
    fn from(status: &ResourceStatus) -> Self {
        Self {
            phase: status.phase,
            observed_generation: status.observed_generation,
            observed_at: status.observed_at,
            reconciled_generation: status.reconciled_generation,
            reconciled_at: status.reconciled_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A resource named by its identity and headers, including the
/// `statusGeneration` it reached
#[serde_with::serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceLifecycleMessageResourceRef {
    pub id: ResourceID,
    pub schema: TypeUri,
    #[serde_as(as = "odf::metadata::serde::yaml::resources::ResourceHeaders")]
    pub headers: ResourceHeaders,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceLifecycleMessageConditionChange {
    /// Set a condition whose schema is registered as carried; the value may be
    /// JSON `null`
    Set {
        value: serde_json::Value,
    },
    /// Set a condition whose value does not travel with the message
    SetEmpty,
    Removed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
