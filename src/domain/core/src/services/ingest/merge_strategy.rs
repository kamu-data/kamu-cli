// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::logical_expr::SortExpr;
use internal_error::InternalError;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Open Data Fabric by design stores all data in the append-only event log
/// format, always preserving the entire history. Unfortunately, a lot of data
/// in the world is not stored or exposed this way. Some organizations may
/// expose their data in the form of periodic database dumps, while some choose
/// to provide it as a log of changes between current and the previous export.
/// Merge strategies define how to combine the newly-ingested data with the
/// existing one.
///
/// Contract:
/// - Previous data is received in its original form, including all system
///   columns
/// - New data is received in its original form, except event time column, which
///   in case of a snapshot data may be populated with NULL values
/// - Resulting data:
///   - must include operation type and event time columns
///   - event time column may still contain null values if input did not have
///     this column
/// - Sort order after [`MergeStrategy::merge`] is arbitrary and must be
///   restored using [`MergeStrategy::sort_order`] at the end of processing
pub trait MergeStrategy: Send + Sync {
    /// Reduces newly seen data `new` to a minimal update to previously
    /// ledgerized `prev` data.
    fn merge(
        &self,
        prev: Option<DataFrameExt>,
        new: DataFrameExt,
    ) -> Result<DataFrameExt, MergeError>;

    /// Returns the sort expression best suited for the output of this strategy
    /// to perform before writing the final result.
    fn sort_order(&self) -> Vec<SortExpr>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MergeError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}
