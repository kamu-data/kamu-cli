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

use crate::{FlowBinding, FlowConfigurationRule};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_CONFIGURATION_UPDATE_OUTBOX_VERSION: u32 = 3;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow's configuration has been updated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowConfigurationUpdatedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique key identifying the flow
    pub flow_binding: FlowBinding,

    /// The updated configuration rule for the flow
    pub rule: FlowConfigurationRule,
}

impl Message for FlowConfigurationUpdatedMessage {
    fn version() -> u32 {
        FLOW_CONFIGURATION_UPDATE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
