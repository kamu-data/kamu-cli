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

const RESOURCE_LIFECYCLE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceLifecycleMessage {
    Applied(ResourceLifecycleMessageApplied),
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
}

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
