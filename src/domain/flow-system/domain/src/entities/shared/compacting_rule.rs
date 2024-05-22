// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactingRule {
    Full(CompactingRuleFull),
    MetadataOnly(CompactingRuleMetadataOnly),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactingRuleFull {
    max_slice_size: u64,
    max_slice_records: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactingRuleMetadataOnly {
    pub recursive: bool,
}

impl CompactingRuleFull {
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

impl CompactingRule {
    #[inline]
    pub fn max_slice_size(&self) -> Option<u64> {
        match self {
            Self::Full(compacting_rule) => Some(compacting_rule.max_slice_size()),
            _ => None,
        }
    }

    #[inline]
    pub fn max_slice_records(&self) -> Option<u64> {
        match self {
            Self::Full(compacting_rule) => Some(compacting_rule.max_slice_records()),
            _ => None,
        }
    }

    #[inline]
    pub fn recursive(&self) -> bool {
        match self {
            Self::MetadataOnly(compacting_rule) => compacting_rule.recursive,
            _ => false,
        }
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

    use crate::{CompactingRuleFull, CompactingRuleValidationError};

    #[test]
    fn test_valid_compacting_rule() {
        assert_matches!(CompactingRuleFull::new_checked(1, 1), Ok(_));
        assert_matches!(CompactingRuleFull::new_checked(1_000_000, 1_000_000), Ok(_));
        assert_matches!(CompactingRuleFull::new_checked(1, 20), Ok(_));
    }

    #[test]
    fn test_non_positive_max_slice_records() {
        assert_matches!(
            CompactingRuleFull::new_checked(100, 0),
            Err(CompactingRuleValidationError::MaxSliceRecordsNotPositive)
        );
    }

    #[test]
    fn test_non_positive_max_slice_size() {
        assert_matches!(
            CompactingRuleFull::new_checked(0, 100),
            Err(CompactingRuleValidationError::MaxSliceSizeNotPositive)
        );
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
