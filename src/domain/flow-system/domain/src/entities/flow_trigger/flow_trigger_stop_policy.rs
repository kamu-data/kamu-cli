// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use nutype::nutype;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FlowTriggerStopPolicy {
    AfterConsecutiveFailures {
        failures_count: ConsecutiveFailuresCount,
    },
    Never,
}

impl Default for FlowTriggerStopPolicy {
    fn default() -> Self {
        Self::AfterConsecutiveFailures {
            failures_count: ConsecutiveFailuresCount::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MAX_CONSECUTIVE_FAILURES_COUNT: u32 = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(
    validate(greater_or_equal = 1, less_or_equal = MAX_CONSECUTIVE_FAILURES_COUNT),
    derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize, AsRef, Into)
)]
pub struct ConsecutiveFailuresCount(u32);

impl Default for ConsecutiveFailuresCount {
    fn default() -> Self {
        // Safe unwrap because 1 is within our valid range [1..=10]
        Self::try_new(1).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consecutive_failures_count_validation() {
        // Valid values should work
        assert!(ConsecutiveFailuresCount::try_new(1).is_ok());
        assert!(ConsecutiveFailuresCount::try_new(5).is_ok());
        assert!(ConsecutiveFailuresCount::try_new(10).is_ok());

        // Invalid values should fail
        assert!(ConsecutiveFailuresCount::try_new(0).is_err());
        assert!(ConsecutiveFailuresCount::try_new(11).is_err());
        assert!(ConsecutiveFailuresCount::try_new(100).is_err());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
