// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::BatchingRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReactiveRule {
    pub for_new_data: BatchingRule,
    pub for_breaking_change: BreakingChangeRule,
}

impl ReactiveRule {
    pub fn new(for_new_data: BatchingRule, for_breaking_change: BreakingChangeRule) -> Self {
        Self {
            for_new_data,
            for_breaking_change,
        }
    }

    pub fn empty() -> Self {
        Self {
            for_new_data: BatchingRule::immediate(),
            for_breaking_change: BreakingChangeRule::NoAction,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BreakingChangeRule {
    NoAction,
    Recover,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
