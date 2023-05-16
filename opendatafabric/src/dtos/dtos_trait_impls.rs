// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use super::DatasetVocabulary;
use crate::{
    ExecuteQueryResponseInternalError,
    ExecuteQueryResponseInvalidQuery,
    ReadStepCsv,
    SetVocab,
};

impl Default for DatasetVocabulary {
    fn default() -> Self {
        DatasetVocabulary {
            offset_column: None,
            system_time_column: None,
            event_time_column: None,
        }
    }
}

impl From<SetVocab> for DatasetVocabulary {
    fn from(v: SetVocab) -> Self {
        Self {
            offset_column: v.offset_column,
            system_time_column: v.system_time_column,
            event_time_column: v.event_time_column,
        }
    }
}

impl Default for ReadStepCsv {
    fn default() -> Self {
        Self {
            schema: None,
            separator: None,
            encoding: None,
            quote: None,
            escape: None,
            comment: None,
            header: None,
            enforce_schema: None,
            infer_schema: None,
            ignore_leading_white_space: None,
            ignore_trailing_white_space: None,
            null_value: None,
            empty_value: None,
            nan_value: None,
            positive_inf: None,
            negative_inf: None,
            date_format: None,
            timestamp_format: None,
            multi_line: None,
        }
    }
}

impl Display for ExecuteQueryResponseInvalidQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl Display for ExecuteQueryResponseInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.message)?;
        if let Some(bt) = &self.backtrace {
            write!(f, "\n\n--- Backtrace ---\n{}", bt)?;
        }
        Ok(())
    }
}
