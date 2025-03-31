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
use odf::utils::data::DataFrameExt;

use crate::*;

/// Ledger merge strategy.
///
/// See [`odf_metadata::MergeStrategyLedger`] for details.
pub struct MergeStrategyLedger {
    vocab: odf::metadata::DatasetVocabulary,
    primary_key: Vec<String>,
}

impl MergeStrategyLedger {
    pub fn new(
        vocab: odf::metadata::DatasetVocabulary,
        cfg: odf::metadata::MergeStrategyLedger,
    ) -> Self {
        Self {
            vocab,
            primary_key: cfg.primary_key,
        }
    }
}

impl MergeStrategy for MergeStrategyLedger {
    // TODO: Ideas to add more comparison modes such as:
    // - input must be a full superset of prev
    //   - otherwise - raise error
    //   - otherwise - emit retractions
    // - input will only contain new events
    // - input will contain new events and a subset of prev events
    //   - dedupe against entire prev
    //   - seen events should be a tail of the prev (less work to dedupe)
    // - input can contain duplicates
    fn merge(
        &self,
        prev: Option<DataFrameExt>,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, MergeError> {
        let new_records = if prev.is_none() {
            // Validate PK columns exist
            new.clone()
                .select(
                    self.primary_key
                        .iter()
                        .map(|name| col(Column::from_name(name)))
                        .collect(),
                )
                .int_err()?;

            new
        } else {
            let cols: Vec<_> = self.primary_key.iter().map(String::as_str).collect();

            new.join(prev.unwrap(), JoinType::LeftAnti, &cols, &cols, None)
                .int_err()?
        };

        let res = new_records
            .with_column(
                &self.vocab.operation_type_column,
                // TODO: Cast to `u8` after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                lit(odf::metadata::OperationType::Append as i32),
            )
            .int_err()?
            .columns_to_front(&[&self.vocab.operation_type_column])
            .int_err()?;

        Ok(res)
    }

    fn sort_order(&self) -> Vec<SortExpr> {
        vec![col(Column::from_name(&self.vocab.event_time_column)).sort(true, true)]
    }
}
