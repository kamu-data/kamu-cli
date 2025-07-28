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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_flow_system::flow_config_enum! {
    pub enum FlowConfigRuleCompact {
        Full(FlowConfigRuleCompactFull),
        MetadataOnly { recursive: bool },
    }
    => "CompactionRule"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigRuleCompactFull {
    max_slice_size: u64,
    max_slice_records: u64,
    recursive: bool,
}

impl FlowConfigRuleCompactFull {
    pub fn new_checked(
        max_slice_size: u64,
        max_slice_records: u64,
        recursive: bool,
    ) -> Result<Self, FlowConfigRuleCompactValidationError> {
        if max_slice_size == 0 {
            return Err(FlowConfigRuleCompactValidationError::MaxSliceSizeNotPositive);
        }
        if max_slice_records == 0 {
            return Err(FlowConfigRuleCompactValidationError::MaxSliceRecordsNotPositive);
        }

        Ok(Self {
            max_slice_size,
            max_slice_records,
            recursive,
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

    #[inline]
    pub fn recursive(&self) -> bool {
        self.recursive
    }
}

impl FlowConfigRuleCompact {
    #[inline]
    pub fn max_slice_size(&self) -> Option<u64> {
        match self {
            Self::Full(compaction_rule) => Some(compaction_rule.max_slice_size()),
            _ => None,
        }
    }

    #[inline]
    pub fn max_slice_records(&self) -> Option<u64> {
        match self {
            Self::Full(compaction_rule) => Some(compaction_rule.max_slice_records()),
            _ => None,
        }
    }

    #[inline]
    pub fn recursive(&self) -> bool {
        match self {
            Self::MetadataOnly { recursive } => *recursive,
            Self::Full(full_compaction_rule) => full_compaction_rule.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowConfigRuleCompactValidationError {
    #[error("Maximum slice records must be a positive number")]
    MaxSliceRecordsNotPositive,

    #[error("Maximum slice size must be a positive number")]
    MaxSliceSizeNotPositive,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::{FlowConfigRuleCompactFull, FlowConfigRuleCompactValidationError};

    #[test]
    fn test_valid_compaction_rule() {
        assert_matches!(FlowConfigRuleCompactFull::new_checked(1, 1, false), Ok(_));
        assert_matches!(
            FlowConfigRuleCompactFull::new_checked(1_000_000, 1_000_000, false),
            Ok(_)
        );
        assert_matches!(FlowConfigRuleCompactFull::new_checked(1, 20, false), Ok(_));
    }

    #[test]
    fn test_non_positive_max_slice_records() {
        assert_matches!(
            FlowConfigRuleCompactFull::new_checked(100, 0, false),
            Err(FlowConfigRuleCompactValidationError::MaxSliceRecordsNotPositive)
        );
    }

    #[test]
    fn test_non_positive_max_slice_size() {
        assert_matches!(
            FlowConfigRuleCompactFull::new_checked(0, 100, false),
            Err(FlowConfigRuleCompactValidationError::MaxSliceSizeNotPositive)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
