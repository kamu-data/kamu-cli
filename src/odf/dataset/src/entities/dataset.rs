// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use ::serde::{Deserialize, Serialize};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use file_utils::OwnedFile;
use internal_error::*;
use odf_metadata::*;
use odf_storage::*;
use thiserror::Error;
use url::Url;

use crate::{AppendError, BlockRef, DatasetSummary, MetadataChain};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    /// Helper function to commit [AddData] event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_add_data(
        &self,
        add_data: AddDataParams,
        data: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError>;

    /// Helper function to commit [ExecuteTransform] event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_execute_transform(
        &self,
        execute_transform: ExecuteTransformParams,
        data: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError>;

    /// Helper function to prepare [ExecuteTransform] event without committing
    /// it.
    async fn prepare_execute_transform(
        &self,
        execute_transform: ExecuteTransformParams,
        data: Option<&OwnedFile>,
        checkpoint: Option<&CheckpointRef>,
    ) -> Result<ExecuteTransform, InternalError>;

    fn get_storage_internal_url(&self) -> &Url;

    fn as_metadata_chain(&self) -> &dyn MetadataChain;

    fn as_data_repo(&self) -> &dyn ObjectRepository;
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository;
    fn as_info_repo(&self) -> &dyn NamedObjectRepository;

    /// Returns a brief summary of the dataset
    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitOpts<'a> {
    /// Which reference to advance upon commit
    pub block_ref: &'a BlockRef,
    /// Override system time of the new block
    pub system_time: Option<DateTime<Utc>>,
    /// Compare-and-swap semantics to ensure there were no concurrent updates
    pub prev_block_hash: Option<Option<&'a Multihash>>,
    /// Whether to check for the presence of linked objects like data and
    /// checkpoints in the respective repos
    pub check_object_refs: bool,
    // Whether to reset head to new committed block
    pub update_block_ref: bool,
}

impl Default for CommitOpts<'_> {
    fn default() -> Self {
        Self {
            block_ref: &BlockRef::Head,
            system_time: None,
            prev_block_hash: None,
            check_object_refs: true,
            update_block_ref: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub old_head: Option<Multihash>,
    pub new_head: Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Expected object of type {expected} but got {actual}")]
pub struct InvalidObjectKind {
    pub expected: String,
    pub actual: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommitError {
    #[error(transparent)]
    ObjectNotFound(#[from] ObjectNotFoundError),
    #[error(transparent)]
    MetadataAppendError(#[from] AppendError),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Commit helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Replicates [`AddData`] event prior to hashing of data and checkpoint
#[derive(Debug, Clone)]
pub struct AddDataParams {
    /// Hash of the checkpoint file used to restore ingestion state, if any.
    pub prev_checkpoint: Option<Multihash>,
    /// Last offset of the previous data slice, if any. Must be equal to the
    /// last non-empty `newData.offsetInterval.end`.
    pub prev_offset: Option<u64>,
    /// Offset interval of the output data written during this transaction, if
    /// any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Last watermark of the output data stream, if any. Initial blocks may not
    /// have watermarks, but once watermark is set - all subsequent blocks
    /// should either carry the same watermark or specify a new (greater) one.
    /// Thus, watermarks are monotonically non-decreasing.
    pub new_watermark: Option<DateTime<Utc>>,
    /// The state of the source the data was added from to allow fast resuming.
    /// If the state did not change but is still relevant for subsequent runs it
    /// should be carried, i.e. only the last state per source is considered
    /// when resuming.
    pub new_source_state: Option<SourceState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Replicates [`ExecuteTransform`] event prior to hashing of data and
/// checkpoint
#[derive(Debug, Clone)]
pub struct ExecuteTransformParams {
    /// Defines inputs used in this transaction. Slices corresponding to every
    /// input dataset must be present.
    pub query_inputs: Vec<ExecuteTransformInput>,
    /// Hash of the checkpoint file used to restore transformation state, if
    /// any.
    pub prev_checkpoint: Option<Multihash>,
    /// Last offset of the previous data slice, if any. Must be equal to the
    /// last non-empty `newData.offsetInterval.end`.
    pub prev_offset: Option<u64>,
    /// Offset interval of the output data written during this transaction, if
    /// any.
    pub new_offset_interval: Option<OffsetInterval>,
    /// Last watermark of the output data stream, if any. Initial blocks may not
    /// have watermarks, but once watermark is set - all subsequent blocks
    /// should either carry the same watermark or specify a new (greater) one.
    /// Thus, watermarks are monotonically non-decreasing.
    pub new_watermark: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum CheckpointRef {
    New(OwnedFile),
    Existed(Multihash),
}

impl From<CheckpointRef> for Option<OwnedFile> {
    fn from(val: CheckpointRef) -> Self {
        match val {
            CheckpointRef::Existed(_) => None,
            CheckpointRef::New(owned_file) => Some(owned_file),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Serialize, Deserialize, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum DatasetVisibility {
    #[default]
    #[serde(alias = "private")]
    Private,
    #[serde(alias = "public")]
    Public,
}

impl DatasetVisibility {
    pub fn is_private(&self) -> bool {
        match self {
            DatasetVisibility::Private => true,
            DatasetVisibility::Public => false,
        }
    }

    pub fn is_public(&self) -> bool {
        match self {
            DatasetVisibility::Private => false,
            DatasetVisibility::Public => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn read_dataset_alias(dataset: &dyn Dataset) -> Result<DatasetAlias, InternalError> {
    let bytes = dataset.as_info_repo().get("alias").await.int_err()?;
    let alias_str = std::str::from_utf8(&bytes[..]).int_err()?.trim();
    let alias = DatasetAlias::try_from(alias_str).int_err()?;
    Ok(alias)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn write_dataset_alias(
    dataset: &dyn Dataset,
    alias: &DatasetAlias,
) -> Result<(), InternalError> {
    dataset
        .as_info_repo()
        .set("alias", alias.to_string().as_bytes())
        .await
        .int_err()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
