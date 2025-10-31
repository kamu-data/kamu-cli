// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{BoxedError, InternalError};
use thiserror::Error;
use url::Url;

use crate::utils::metadata_chain_comparator::CompareChainsError;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SyncService: Send + Sync {
    async fn sync(
        &self,
        request: SyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError>;

    /// Adds dataset to IPFS and returns the root CID.
    /// Unlike `sync` it does not do IPNS resolution and publishing.
    async fn ipfs_add(&self, src: ResolvedDataset) -> Result<String, IpfsAddError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SyncRequest {
    pub src: SyncRef,
    pub dst: SyncRef,
}

#[derive(Debug, Clone)]
pub enum SyncRef {
    Local(ResolvedDataset),
    LocalNew(odf::DatasetAlias),
    Remote(SyncRefRemote),
}

impl SyncRef {
    pub fn is_local(&self) -> bool {
        match self {
            Self::Local(_) | Self::LocalNew(_) => true,
            Self::Remote(_) => false,
        }
    }

    // If remote, refers to resolved repository URL
    pub fn as_internal_any_ref(&self) -> odf::DatasetRefAny {
        match self {
            Self::Local(local_ref) => local_ref.get_handle().as_any_ref(),
            Self::LocalNew(alias) => alias.as_any_ref(),
            Self::Remote(remote_ref) => odf::DatasetRefAny::Url(remote_ref.url.clone()),
        }
    }

    // If remote, returns the original unresolved ref
    pub fn as_user_friendly_any_ref(&self) -> odf::DatasetRefAny {
        match self {
            Self::Local(local_ref) => local_ref.get_handle().as_any_ref(),
            Self::LocalNew(alias) => alias.as_any_ref(),
            Self::Remote(remote_ref) => remote_ref.original_remote_ref.as_any_ref(),
        }
    }

    pub fn detach_from_transaction(&self) {
        if let Self::Local(r) = self {
            r.detach_from_transaction();
        }
    }

    pub async fn refresh_dataset_from_registry(&mut self, dataset_registry: &dyn DatasetRegistry) {
        if let Self::Local(r) = self {
            *r = dataset_registry.get_dataset_by_handle(r.get_handle()).await;
        }
    }
}

#[derive(Clone)]
pub struct SyncRefRemote {
    pub url: Arc<Url>,
    pub dataset: Arc<dyn odf::Dataset>,
    pub original_remote_ref: odf::DatasetRefRemote,
}

impl std::fmt::Debug for SyncRefRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRefRemote")
            .field("url", &self.url)
            .field("original_remote_ref", &self.original_remote_ref)
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct SyncOptions {
    /// Whether the source of data can be assumed non-malicious to skip hash sum
    /// and other expensive checks. Defaults to `true` when the source is
    /// local workspace.
    pub trust_source: Option<bool>,

    /// Whether destination dataset should be created if it does not exist
    pub create_if_not_exists: bool,

    /// Force synchronization, even if revisions have diverged
    pub force: bool,

    /// Dataset visibility, in case of initial pushing or pulling
    pub dataset_visibility: odf::DatasetVisibility,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            trust_source: None,
            create_if_not_exists: true,
            force: false,
            dataset_visibility: odf::DatasetVisibility::Private,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SyncResult {
    UpToDate,
    Updated {
        old_head: Option<odf::Multihash>,
        new_head: odf::Multihash,
        num_blocks: u64,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Listener
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait SyncListener: Sync + Send {
    fn begin(&self) {}
    fn on_status(&self, _stage: SyncStage, _stats: &SyncStats) {}
    fn success(&self, _result: &SyncResult) {}
    fn error(&self, _error: &SyncError) {}
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SyncStage {
    ReadMetadata,
    TransferData,
    CommitBlocks,
}

#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// Statistics for the source party of the exchange
    pub src: SyncPartyStats,
    /// Estimated totals for the source party of the exchange
    pub src_estimated: SyncPartyStats,
    /// Statistics for the destination party of the exchange
    pub dst: SyncPartyStats,
    /// Estimated totals for the destination party of the exchange
    pub dst_estimated: SyncPartyStats,
}

#[derive(Debug, Clone, Default)]
pub struct SyncPartyStats {
    /// Number of metadata blocks read from the party
    pub metadata_blocks_read: u64,
    /// Number of metadata blocks written to the party
    pub metadata_blocks_written: u64,
    /// Number of checkpoint files read from the party
    pub checkpoints_read: u64,
    /// Number of checkpoint files written to the party
    pub checkpoints_written: u64,
    /// Number of data files read from the party
    pub data_slices_read: u64,
    /// Number of data files written to the party
    pub data_slices_written: u64,
    /// Number of data records read from the party
    pub data_records_read: u64,
    /// Number of data records written to the party
    pub data_records_written: u64,
    /// Number of bytes read from the party
    pub bytes_read: u64,
    /// Number of bytes written to the party
    pub bytes_written: u64,
}

pub struct NullSyncListener;
impl SyncListener for NullSyncListener {}

pub trait SyncMultiListener: Send + Sync {
    fn begin_sync(
        &self,
        _src: &odf::DatasetRefAny,
        _dst: &odf::DatasetRefAny,
    ) -> Option<Arc<dyn SyncListener>> {
        None
    }
}

pub struct NullSyncMultiListener;
impl SyncMultiListener for NullSyncMultiListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SyncError {
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetAnyRefUnresolvedError),
    #[error(transparent)]
    RefCollision(#[from] odf::dataset::RefCollisionError),
    #[error(transparent)]
    NameCollision(#[from] kamu_datasets::NameCollisionError),
    #[error(transparent)]
    CreateDatasetFailed(#[from] kamu_datasets::CreateDatasetError),
    #[error(transparent)]
    UnsupportedProtocol(#[from] odf::dataset::UnsupportedProtocolError),
    #[error(transparent)]
    UnsupportedIpfsStorageType(#[from] UnsupportedIpfsStorageTypeError),
    #[error(transparent)]
    RepositoryNotFound(
        #[from]
        #[backtrace]
        RepositoryNotFoundError,
    ),
    // TODO: Report divergence type:
    // Local dataset ({local_head}) and remote ({remote_head}) have diverged (remote is ahead
    // by {uncommon_blocks_in_remote} blocks, local is ahead by {uncommon_blocks_in_local})
    #[error(transparent)]
    DatasetsDiverged(#[from] DatasetsDivergedError),
    #[error(transparent)]
    DestinationAhead(#[from] DestinationAheadError),
    #[error(transparent)]
    InvalidInterval(#[from] odf::dataset::InvalidIntervalError),
    #[error(transparent)]
    OverwriteSeedBlock(#[from] OverwriteSeedBlockError),
    #[error(transparent)]
    Corrupted(#[from] CorruptedSourceError),
    #[error("Dataset was updated concurrently")]
    UpdatedConcurrently(#[source] BoxedError),
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::metadata::AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum IpfsAddError {
    #[error(transparent)]
    UnsupportedIpfsStorageType(#[from] UnsupportedIpfsStorageTypeError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Remove this error in favor of `odf::DatasetNotFoundError`
#[derive(Error, Clone, Eq, PartialEq, Debug)]
#[error("Dataset {dataset_ref} not found")]
pub struct DatasetAnyRefUnresolvedError {
    pub dataset_ref: odf::DatasetRefAny,
}

impl DatasetAnyRefUnresolvedError {
    pub fn new(r: impl Into<odf::DatasetRefAny>) -> Self {
        Self {
            dataset_ref: r.into(),
        }
    }
}

impl From<odf::DatasetNotFoundError> for DatasetAnyRefUnresolvedError {
    fn from(v: odf::DatasetNotFoundError) -> Self {
        Self {
            dataset_ref: v.dataset_ref.as_any_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, Eq, PartialEq, Debug)]
pub struct DatasetsDivergedError {
    pub src_head: odf::Multihash,
    pub dst_head: odf::Multihash,
    pub detail: Option<DatasetsDivergedErrorDetail>,
}

impl std::fmt::Display for DatasetsDivergedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(detail) = &self.detail {
            write!(
                f,
                "Source and destination datasets have diverged and have {} and {} different \
                 blocks respectively, source head is {}, destination head is {}",
                detail.uncommon_blocks_in_src,
                detail.uncommon_blocks_in_dst,
                self.src_head,
                self.dst_head,
            )
        } else {
            write!(
                f,
                "Source and destination datasets have diverged, source head is {}, destination \
                 head is {}",
                self.src_head, self.dst_head,
            )
        }
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct DatasetsDivergedErrorDetail {
    pub uncommon_blocks_in_src: u64,
    pub uncommon_blocks_in_dst: u64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, Eq, PartialEq, Debug)]
#[error(
    "Destination head {dst_head} is ahead of source head {src_head} by {dst_ahead_size} blocks"
)]
pub struct DestinationAheadError {
    pub src_head: odf::Multihash,
    pub dst_head: odf::Multihash,
    pub dst_ahead_size: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Repository appears to have corrupted data: {message}")]
pub struct CorruptedSourceError {
    pub message: String,
    #[source]
    pub source: Option<BoxedError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetRefUnresolvedError> for SyncError {
    fn from(v: odf::DatasetRefUnresolvedError) -> Self {
        match v {
            odf::DatasetRefUnresolvedError::NotFound(e) => {
                Self::DatasetNotFound(DatasetAnyRefUnresolvedError {
                    dataset_ref: e.dataset_ref.into(),
                })
            }
            odf::DatasetRefUnresolvedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<GetRepoError> for SyncError {
    fn from(v: GetRepoError) -> Self {
        match v {
            GetRepoError::NotFound(e) => Self::RepositoryNotFound(e),
            GetRepoError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<odf::dataset::BuildDatasetError> for SyncError {
    fn from(v: odf::dataset::BuildDatasetError) -> Self {
        match v {
            odf::dataset::BuildDatasetError::UnsupportedProtocol(e) => Self::UnsupportedProtocol(e),
            odf::dataset::BuildDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<CompareChainsError> for SyncError {
    fn from(v: CompareChainsError) -> Self {
        match v {
            CompareChainsError::Corrupted(e) => Self::Corrupted(e),
            CompareChainsError::Access(e) => Self::Access(e),
            CompareChainsError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<IpfsAddError> for SyncError {
    fn from(v: IpfsAddError) -> Self {
        match v {
            IpfsAddError::UnsupportedIpfsStorageType(e) => Self::UnsupportedIpfsStorageType(e),
            IpfsAddError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Dataset storage type '{}' is unsupported for IPFS operations", url.scheme())]
pub struct UnsupportedIpfsStorageTypeError {
    pub url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Overwriting dataset id or type is restricted")]
pub struct OverwriteSeedBlockError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
