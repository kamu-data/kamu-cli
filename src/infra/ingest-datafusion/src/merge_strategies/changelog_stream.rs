// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::logical_expr::SortExpr;
use datafusion::prelude::*;
use internal_error::*;

use crate::*;

/// Changelog stream merge strategy.
///
/// See [`odf_metadata::MergeStrategyChangelogStream`] for details.
pub struct MergeStrategyChangelogStream {
    vocab: odf::metadata::DatasetVocabulary,
    primary_key: Vec<String>,
}

impl MergeStrategyChangelogStream {
    pub fn new(
        vocab: odf::metadata::DatasetVocabulary,
        cfg: odf::metadata::MergeStrategyChangelogStream,
    ) -> Self {
        Self {
            vocab,
            primary_key: cfg.primary_key,
        }
    }

    fn validate_input(&self, new: DataFrame) -> Result<(), MergeError> {
        // Validate PK and Op columns exist
        new.select(
            self.primary_key
                .iter()
                .chain([&self.vocab.operation_type_column])
                .map(|name| col(Column::from_name(name)))
                .collect(),
        )
        .int_err()?;

        Ok(())
    }
}

// TODO: Validate op codes and ordering
// TODO: Compact inter-batch changes?
impl MergeStrategy for MergeStrategyChangelogStream {
    fn merge(&self, _prev: Option<DataFrame>, new: DataFrame) -> Result<DataFrame, MergeError> {
        self.validate_input(new.clone())?;
        Ok(new)
    }

    fn sort_order(&self) -> Vec<SortExpr> {
        // Main goal here is to establish correct order of -C / +C corrections, so we
        // sort records by primary key and then by operation type
        self.primary_key
            .iter()
            .map(|c| col(Column::from_name(c)).sort(true, true))
            .chain(std::iter::once(
                col(Column::from_name(&self.vocab.operation_type_column)).sort(true, true),
            ))
            .collect()
    }
}
