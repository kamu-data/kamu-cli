// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use internal_error::*;
use opendatafabric as odf;

use super::MergeError;
use crate::CommitError;

///////////////////////////////////////////////////////////////////////////////

/// Auxiliary interface for appending data to root datasets.
/// Writers perform necessary transformations and merge strategies
/// to commit data into a dataset in bitemporal ledger form.
#[async_trait::async_trait]
pub trait DataWriter {
    async fn write(
        &mut self,
        new_data: DataFrame,
        opts: WriteDataOpts,
    ) -> Result<WriteDataResult, WriteDataError>;
}

///////////////////////////////////////////////////////////////////////////////

pub struct WriteDataOpts {
    /// Will be used for system time data column and metadata block timestamp
    pub system_time: DateTime<Utc>,
    /// If data does not contain event time column already this value will be
    /// used to populate it
    pub source_event_time: DateTime<Utc>,
    /// Data source state to store in the commit
    pub source_state: Option<odf::SourceState>,
    // TODO: Find a better way to deal with temporary files
    /// Local FS path to which data slice will be written before commiting it
    /// into the data object store of a dataset
    pub data_staging_path: PathBuf,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteDataResult {
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub new_block: odf::MetadataBlockTyped<odf::AddData>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum WriteDataError {
    #[error(transparent)]
    MergeError(#[from] MergeError),

    #[error(transparent)]
    CommitError(#[from] CommitError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}
