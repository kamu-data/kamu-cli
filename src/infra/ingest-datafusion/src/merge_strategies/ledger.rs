// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::*;
use internal_error::*;
use kamu_data_utils::data::dataframe_ext::DataFrameExt;
use opendatafabric as odf;

use crate::*;

/// Ledger merge strategy.
///
/// See [opendatafabric::MergeStrategyLedger] for details.
pub struct MergeStrategyLedger {
    vocab: odf::DatasetVocabulary,
    primary_key: Vec<String>,
}

impl MergeStrategyLedger {
    pub fn new(vocab: odf::DatasetVocabulary, cfg: opendatafabric::MergeStrategyLedger) -> Self {
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
    fn merge(&self, prev: Option<DataFrame>, new: DataFrame) -> Result<DataFrame, MergeError> {
        let new_records = if prev.is_none() {
            // Validate PK columns exist
            new.clone()
                .select(self.primary_key.iter().map(|c| col(c)).collect())
                .int_err()?;

            new
        } else {
            let cols: Vec<_> = self.primary_key.iter().map(|s| s.as_str()).collect();

            new.join(prev.unwrap(), JoinType::LeftAnti, &cols, &cols, None)
                .int_err()?
        };

        let res = new_records
            .with_column(
                &self.vocab.operation_type_column,
                lit(odf::OperationType::Append as u8),
            )
            .int_err()?
            .columns_to_front(&[&self.vocab.operation_type_column])
            .int_err()?;

        Ok(res)
    }

    fn sort_order(&self) -> Vec<Expr> {
        vec![col(&self.vocab.event_time_column).sort(true, true)]
    }
}
