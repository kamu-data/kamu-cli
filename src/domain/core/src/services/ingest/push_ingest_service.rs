// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushIngestService: Send + Sync {
    /// Uses or auto-creates push source definition in metadata to plan
    /// ingestion
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: PushIngestOpts,
    ) -> Result<PushIngestPlan, PushIngestPlanningError>;

    /// Uses push source definition in metadata to ingest data from the
    /// specified source.
    ///
    /// See also [MediaType].
    async fn ingest_from_url(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        url: url::Url,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError>;

    /// Uses push source definition in metadata to ingest data possessed
    /// in-band as a file stream.
    ///
    /// See also [MediaType].
    async fn ingest_from_file_stream(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        data: Box<dyn AsyncRead + Send + Unpin>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
pub struct PushIngestOpts {
    /// MIME type of the content
    pub media_type: Option<MediaType>,
    /// Event time to use if data does not contain such column itself
    pub source_event_time: Option<DateTime<Utc>>,
    /// Whether to automatically create a push source if it doesn't exist
    pub auto_create_push_source: bool,
    /// Schema inference configuration
    pub schema_inference: SchemaInferenceOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PushIngestPlan {
    pub args: PushIngestArgs,
    pub metadata_state: Box<DataWriterMetadataState>,
}

#[derive(Debug)]
pub struct PushIngestArgs {
    pub operation_id: String,
    pub operation_dir: PathBuf,
    pub system_time: DateTime<Utc>,
    pub opts: PushIngestOpts,
    pub push_source: AddPushSource,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum PushIngestResult {
    UpToDate,
    Updated {
        old_head: Multihash,
        new_head: Multihash,
        num_blocks: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Listener
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushIngestStage {
    Read,
    Preprocess,
    Merge,
    Commit,
}

pub trait PushIngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_stage_progress(&self, _stage: PushIngestStage, _progress: u64, _out_of: TotalSteps) {}
    fn success(&self, _result: &PushIngestResult) {}
    fn error(&self, _error: &PushIngestError) {}

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushIngestPlanningError {
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

    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        CommitError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Revisit error granularity
#[derive(Debug, Error)]
pub enum PushIngestError {
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
    BadInputSchema(
        #[from]
        #[backtrace]
        BadInputSchemaError,
    ),

    #[error(transparent)]
    IncompatibleSchema(
        #[from]
        #[backtrace]
        IncompatibleSchemaError,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error, Default)]
pub struct PushSourceNotFoundError {
    source_name: Option<String>,
}

impl PushSourceNotFoundError {
    pub fn new(source_name: Option<impl Into<String>>) -> Self {
        Self {
            source_name: source_name.map(Into::into),
        }
    }
}

impl std::fmt::Display for PushSourceNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source_name {
            None => write!(
                f,
                "Dataset does not define a default push source, consider specifying the source \
                 name"
            ),
            Some(s) => write!(f, "Dataset does not define a push source '{s}'"),
        }
    }
}

#[derive(Debug, Error)]
#[error("Unsupported media type {media_type}")]
pub struct UnsupportedMediaTypeError {
    pub media_type: MediaType,
}

impl UnsupportedMediaTypeError {
    pub fn new(media_type: MediaType) -> Self {
        Self { media_type }
    }
}
