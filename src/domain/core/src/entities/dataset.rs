// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opendatafabric::*;
use thiserror::Error;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait Dataset: Send + Sync {
    /// Helper function to append a generic event to metadata chain.
    ///
    /// Warning: Don't use when synchronizing blocks from another dataset.
    async fn commit_event(
        &self,
        event: MetadataEvent,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError>;

    /// Helper function to commit AddData event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_add_data(
        &self,
        input_checkpoint: Option<Multihash>,
        data_interval: Option<OffsetInterval>,
        movable_data_file: Option<&Path>,
        movable_checkpoint_file: Option<&Path>,
        output_watermark: Option<DateTime<Utc>>,
        source_state: Option<SourceState>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError>;

    fn as_metadata_chain(&self) -> &dyn MetadataChain;
    fn as_data_repo(&self) -> &dyn ObjectRepository;
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository;
    fn as_info_repo(&self) -> &dyn NamedObjectRepository;

    /// Returns a brief summary of the dataset
    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct GetSummaryOpts {
    pub update_if_stale: bool,
}

impl Default for GetSummaryOpts {
    fn default() -> Self {
        Self {
            update_if_stale: true,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitOpts<'a> {
    /// Which reference to advance upon commit
    pub block_ref: &'a BlockRef,
    /// Override system time of the new block
    pub system_time: Option<DateTime<Utc>>,
    /// Compare-and-swap semantics to ensure there were no concurrent updates
    pub prev_block_hash: Option<Option<&'a Multihash>>,
    /// Whether to check for presence of linked objects like data and checkpoins
    /// in the respective repos
    pub check_object_refs: bool,
}

impl<'a> Default for CommitOpts<'a> {
    fn default() -> Self {
        Self {
            block_ref: &BlockRef::Head,
            system_time: None,
            prev_block_hash: None,
            check_object_refs: true,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Expected object of type {expected} but got {actual}")]
pub struct InvalidObjectKind {
    pub expected: String,
    pub actual: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetSummaryError {
    #[error("Dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommitError {
    #[error(transparent)]
    ObjectNotFound(#[from] ObjectNotFoundError),
    #[error(transparent)]
    MetadataAppendError(#[from] AppendError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}
