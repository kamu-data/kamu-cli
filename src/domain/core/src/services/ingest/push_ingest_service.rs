// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::*;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::*;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushIngestService: Send + Sync {
    /// Uses push source defenition in metadata to ingest data from the
    /// specified source.
    ///
    /// See also [IngestMediaTypes].
    async fn ingest_from_url(
        &self,
        dataset_ref: &DatasetRef,
        url: url::Url,
        media_type: Option<&str>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError>;

    /// Uses push source defenition in metadata to ingest data passessed
    /// in-band as a file stream.
    ///
    /// See also [IngestMediaTypes].
    async fn ingest_from_file_stream(
        &self,
        dataset_ref: &DatasetRef,
        data: Box<dyn AsyncRead + Send + Unpin>,
        media_type: Option<&str>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError>;
}

/// Some media types of data formats acceptable by the ingest.
///
/// We use IANA types where we can:
///     https://www.iana.org/assignments/media-types/media-types.xhtml
pub struct IngestMediaTypes;
impl IngestMediaTypes {
    pub const CSV: &str = "text/csv";
    pub const JSON: &str = "application/json";
    /// Unofficial but used by several software projects
    pub const NDJSON: &str = "application/x-ndjson";
    pub const GEOJSON: &str = "application/geo+json";
    /// No standard found
    pub const NDGEOJSON: &str = "application/x-ndgeojson";
    /// See: https://issues.apache.org/jira/browse/PARQUET-1889
    pub const PARQUET: &str = "application/vnd.apache.parquet";
    /// No standard found
    pub const ESRISHAPEFILE: &str = "application/vnd.esri.shapefile";
}

#[derive(Debug)]
pub enum PushIngestResult {
    UpToDate,
    Updated {
        old_head: Multihash,
        new_head: Multihash,
        num_blocks: usize,
    },
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushIngestStage {
    CheckSource,
    Fetch,
    Read,
    Preprocess,
    Merge,
    Commit,
}

#[allow(unused_variables)]
pub trait PushIngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_stage_progress(&self, stage: PushIngestStage, _progress: u64, _out_of: TotalSteps) {}
    fn success(&self, result: &PushIngestResult) {}
    fn error(&self, error: &PushIngestError) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub struct NullPushIngestListener;
impl PushIngestListener for NullPushIngestListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

// TODO: Revisit error granularity
#[derive(Debug, Error)]
pub enum PushIngestError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),

    #[error(transparent)]
    SourceNotFound(
        #[from]
        #[backtrace]
        PushSourceNotFoundError,
    ),

    #[error(transparent)]
    UnsupportedMediaType(
        #[from]
        #[backtrace]
        UnsupportedMediaTypeError,
    ),

    #[error("Engine error")]
    EngineError(
        #[from]
        #[backtrace]
        crate::engine::EngineError,
    ),

    #[error(transparent)]
    ReadError(
        #[from]
        #[backtrace]
        ingest::ReadError,
    ),

    #[error(transparent)]
    MergeError(
        #[from]
        #[backtrace]
        ingest::MergeError,
    ),

    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        CommitError,
    ),

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

impl From<GetDatasetError> for PushIngestError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for PushIngestError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Dataset does not define a push source")]
pub struct PushSourceNotFoundError;

#[derive(Debug, Error)]
#[error("Unsupported media type {media_type}")]
pub struct UnsupportedMediaTypeError {
    pub media_type: String,
}

impl UnsupportedMediaTypeError {
    pub fn new(media_type: impl Into<String>) -> Self {
        Self {
            media_type: media_type.into(),
        }
    }
}
