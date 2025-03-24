// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod append;
mod changelog_stream;
mod ledger;
mod snapshot;
mod upsert_stream;

pub use append::*;
pub use changelog_stream::*;
use datafusion::error::DataFusionError;
use internal_error::*;
pub use ledger::*;
pub use snapshot::*;
pub use upsert_stream::*;

/// Helps us capture backtraces as close to the point as possible
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct DataFusionErrorWrapped(InternalError);

impl From<DataFusionError> for DataFusionErrorWrapped {
    fn from(value: DataFusionError) -> Self {
        Self(value.int_err())
    }
}

impl From<DataFusionErrorWrapped> for kamu_core::MergeError {
    fn from(value: DataFusionErrorWrapped) -> Self {
        Self::Internal(value.0)
    }
}
