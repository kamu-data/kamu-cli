// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{ResourceCondition, ResourcePhase, VariableSetSpec};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct VariableSetStatus {
    pub phase: ResourcePhase,
    pub observed_generation: u64,
    pub conditions: Vec<ResourceCondition>,
    pub stats: VariableSetStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VariableSetStats {
    pub total_variables: usize,
    pub valid_variables: usize,
    pub invalid_variables: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VariableSetStatus {
    pub fn from_spec(spec: &VariableSetSpec) -> Self {
        let total = spec.variables.len();

        Self {
            phase: ResourcePhase::Pending,
            observed_generation: 0,
            conditions: Vec::new(),
            stats: VariableSetStats {
                total_variables: total,
                valid_variables: total,
                invalid_variables: 0,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
