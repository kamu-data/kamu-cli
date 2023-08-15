// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::DataFrame;
use internal_error::InternalError;

///////////////////////////////////////////////////////////////////////////////

/// Open Data Fabric by design stores all data in the append-only event log
/// format, always preserving the entire history. Unfortunately, a lot of data
/// in the world is not stored or exposed this way. Some organizations may
/// expose their data in the form of periodic database dumps, while some choose
/// to provide it as a log of changes between current and the previous export.
/// Merge strategies define how to combine the newly-ingested data with the
/// existing one.
pub trait MergeStrategy {
    /// Reduces newly seen data `new` to a minimal update to previously
    /// ledgerized data `prev`.
    fn merge(&self, prev: Option<DataFrame>, new: DataFrame) -> Result<DataFrame, MergeError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MergeError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}
