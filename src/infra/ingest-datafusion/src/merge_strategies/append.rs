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

/// Append merge strategy.
///
/// See [opendatafabric::MergeStrategy] for details.
pub struct MergeStrategyAppend {
    vocab: odf::DatasetVocabulary,
}

impl MergeStrategyAppend {
    pub fn new(vocab: odf::DatasetVocabulary) -> Self {
        Self { vocab }
    }
}

impl MergeStrategy for MergeStrategyAppend {
    fn merge(&self, _prev: Option<DataFrame>, new: DataFrame) -> Result<DataFrame, MergeError> {
        let df = new
            .with_column(
                &self.vocab.operation_type_column,
                // TODO: Cast to `u8` after Spark is updated
                // See: https://github.com/kamu-data/kamu-cli/issues/445
                lit(odf::OperationType::Append as i32),
            )
            .int_err()?
            .columns_to_front(&[&self.vocab.operation_type_column])
            .int_err()?;

        Ok(df)
    }

    fn sort_order(&self) -> Vec<Expr> {
        vec![col(&self.vocab.event_time_column).sort(true, true)]
    }
}
