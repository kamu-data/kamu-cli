// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactingRule {
    max_slice_size: u64,
    max_slice_records: u64,
}

impl CompactingRule {
    pub fn new_checked(
        max_slice_size: u64,
        max_slice_records: u64,
    ) -> Result<Self, CompactingRuleValidationError> {
        if max_slice_size == 0 {
            return Err(CompactingRuleValidationError::MaxSliceSizeNotPositive);
        }
        if max_slice_records == 0 {
            return Err(CompactingRuleValidationError::MaxSliceRecordsNotPositive);
        }

        Ok(Self {
            max_slice_size,
            max_slice_records,
        })
    }

    #[inline]
    pub fn max_slice_size(&self) -> u64 {
        self.max_slice_size
    }

    #[inline]
    pub fn max_slice_records(&self) -> u64 {
        self.max_slice_records
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CompactingRuleValidationError {
    #[error("Maximum slice records must be a positive number")]
    MaxSliceRecordsNotPositive,

    #[error("Maximum slice size must be a positive number")]
    MaxSliceSizeNotPositive,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::{CompactingRule, CompactingRuleValidationError};

    #[test]
    fn test_valid_compacting_rule() {
        assert_matches!(CompactingRule::new_checked(1, 1), Ok(_));
        assert_matches!(CompactingRule::new_checked(1_000_000, 1_000_000), Ok(_));
        assert_matches!(CompactingRule::new_checked(1, 20), Ok(_));
    }

    #[test]
    fn test_non_positive_max_slice_records() {
        assert_matches!(
            CompactingRule::new_checked(100, 0),
            Err(CompactingRuleValidationError::MaxSliceRecordsNotPositive)
        );
    }

    #[test]
    fn test_non_positive_max_slice_size() {
        assert_matches!(
            CompactingRule::new_checked(0, 100),
            Err(CompactingRuleValidationError::MaxSliceSizeNotPositive)
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
