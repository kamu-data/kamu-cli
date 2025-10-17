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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowTriggerStatus {
    Active,
    PausedByUser,
    StoppedAutomatically,
    ScopeRemoved,
}

impl FlowTriggerStatus {
    pub fn is_active(&self) -> bool {
        match self {
            Self::Active => true,
            Self::PausedByUser | Self::StoppedAutomatically | Self::ScopeRemoved => false,
        }
    }

    pub fn is_dead(&self) -> bool {
        *self == FlowTriggerStatus::ScopeRemoved
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
