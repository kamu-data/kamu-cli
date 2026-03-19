// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::ResourceStatus;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableSetStatus {
    #[serde(flatten)]
    pub resource_status: ResourceStatus,

    pub stats: VariableSetStats,
}

impl VariableSetStatus {
    pub fn new_pending(stats: VariableSetStats) -> Self {
        Self {
            resource_status: ResourceStatus::new_pending(),
            stats,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VariableSetStats {
    pub total_variables: usize,
    pub valid_variables: usize,
    pub invalid_variables: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
