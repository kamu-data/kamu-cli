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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionRule {
    Full(CompactionRuleFull),
    MetadataOnly(CompactionRuleMetadataOnly),
}

impl From<CompactionRuleFull> for CompactionRule {
    fn from(value: CompactionRuleFull) -> Self {
        Self::Full(value)
    }
}

impl From<CompactionRuleMetadataOnly> for CompactionRule {
    fn from(value: CompactionRuleMetadataOnly) -> Self {
        Self::MetadataOnly(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRuleFull {
    max_slice_size: u64,
    max_slice_records: u64,
    recursive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionRuleMetadataOnly {
    pub recursive: bool,
}

impl CompactionRuleFull {
    pub fn new_checked(
        max_slice_size: u64,
        max_slice_records: u64,
        recursive: bool,
    ) -> Result<Self, CompactionRuleValidationError> {
        if max_slice_size == 0 {
            return Err(CompactionRuleValidationError::MaxSliceSizeNotPositive);
        }
        if max_slice_records == 0 {
            return Err(CompactionRuleValidationError::MaxSliceRecordsNotPositive);
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

impl CompactionRule {
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
            Self::MetadataOnly(compaction_rule) => compaction_rule.recursive,
            Self::Full(full_compaction_rule) => full_compaction_rule.recursive,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CompactionRuleValidationError {
    #[error("Maximum slice records must be a positive number")]
    MaxSliceRecordsNotPositive,

    #[error("Maximum slice size must be a positive number")]
    MaxSliceSizeNotPositive,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::{CompactionRuleFull, CompactionRuleValidationError};

    #[test]
    fn test_valid_compaction_rule() {
        assert_matches!(CompactionRuleFull::new_checked(1, 1, false), Ok(_));
        assert_matches!(
            CompactionRuleFull::new_checked(1_000_000, 1_000_000, false),
            Ok(_)
        );
        assert_matches!(CompactionRuleFull::new_checked(1, 20, false), Ok(_));
    }

    #[test]
    fn test_non_positive_max_slice_records() {
        assert_matches!(
            CompactionRuleFull::new_checked(100, 0, false),
            Err(CompactionRuleValidationError::MaxSliceRecordsNotPositive)
        );
    }

    #[test]
    fn test_non_positive_max_slice_size() {
        assert_matches!(
            CompactionRuleFull::new_checked(0, 100, false),
            Err(CompactionRuleValidationError::MaxSliceSizeNotPositive)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
