// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    ResourceUpdate(FlowActivationCauseResourceUpdate),
}

impl FlowActivationCause {
    pub fn activation_time(&self) -> DateTime<Utc> {
        match self {
            Self::Manual(t) => t.activation_time,
            Self::AutoPolling(t) => t.activation_time,
            Self::ResourceUpdate(t) => t.activation_time,
        }
    }

    pub fn initiator_account_id(&self) -> Option<&odf::AccountID> {
        if let Self::Manual(manual) = self {
            Some(&manual.initiator_account_id)
        } else {
            None
        }
    }

    /// Checks if new cause is unique compared to the existing causes
    pub fn is_unique_vs(&self, existing_causes: &[FlowActivationCause]) -> bool {
        // Try finding a similar existing causes and abort early, when found
        for existing in existing_causes {
            match (self, existing) {
                // If both are manual, compare initiator accounts. One cause is enough per account
                (Self::Manual(this), Self::Manual(existing))
                    if this.initiator_account_id == existing.initiator_account_id =>
                {
                    return false;
                }
                // If both are auto-polling, they are the same, one cause is enough
                (Self::AutoPolling(_), Self::AutoPolling(_)) => return false,
                // If both are dataset updates, compare the structures, but any key attribute
                // difference means it's unique
                (Self::ResourceUpdate(this), Self::ResourceUpdate(existing)) => {
                    return this.details != existing.details || this.changes != existing.changes;
                }
                _ => { /* Continue comparing */ }
            }
        }

        // No similar cause was found, so it's a truly unique one
        true
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseManual {
    pub activation_time: DateTime<Utc>,
    pub initiator_account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type InitiatorIDStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<odf::AccountID, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseAutoPolling {
    pub activation_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseResourceUpdate {
    pub activation_time: DateTime<Utc>,
    pub changes: ResourceChanges,
    pub resource_type: String,
    pub details: serde_json::Value,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceChanges {
    NewData(ResourceDataChanges),
    Breaking,
}

impl ResourceChanges {
    pub fn is_empty(&self) -> bool {
        match self {
            ResourceChanges::NewData(changes) => changes.is_empty(),
            ResourceChanges::Breaking => false,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct ResourceDataChanges {
    pub blocks_added: u64,
    pub records_added: u64,
    pub new_watermark: Option<DateTime<Utc>>,
}

impl ResourceDataChanges {
    pub fn is_empty(&self) -> bool {
        self.blocks_added == 0 && self.records_added == 0
    }
}

impl std::ops::AddAssign for ResourceDataChanges {
    fn add_assign(&mut self, rhs: Self) {
        self.blocks_added += rhs.blocks_added;
        self.records_added += rhs.records_added;
        self.new_watermark = match self.new_watermark {
            None => rhs.new_watermark,
            Some(self_watermark) => match rhs.new_watermark {
                None => Some(self_watermark),
                Some(rhs_watermark) => Some(std::cmp::max(self_watermark, rhs_watermark)),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
