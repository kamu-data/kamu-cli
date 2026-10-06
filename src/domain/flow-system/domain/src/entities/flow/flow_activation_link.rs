// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::FlowID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An upstream flow whose result activated a downstream flow, which then
/// processed that activation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowActivationLink {
    pub upstream_flow_id: FlowID,
    pub downstream_flow_id: FlowID,
    /// When the activation reached the downstream flow
    pub activated_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Name under which the projector of activation links stores its position. The
/// migration that backfilled historical links seeded this position, so it must
/// not change.
pub const FLOW_ACTIVATION_LINK_PROJECTOR_NAME: &str =
    "dev.kamu.domain.flow-system.FlowActivationLinkProjector";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
