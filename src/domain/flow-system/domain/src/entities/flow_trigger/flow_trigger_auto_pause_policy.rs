// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FlowTriggerAutoPausePolicy {
    AfterConsecutiveFailures { failures_count: u32 },
    Never,
}

impl Default for FlowTriggerAutoPausePolicy {
    fn default() -> Self {
        Self::AfterConsecutiveFailures { failures_count: 1 }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
