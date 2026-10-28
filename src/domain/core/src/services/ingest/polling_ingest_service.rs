// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use container_runtime::ImagePullError;
use internal_error::{BoxedError, InternalError};
use kamu_datasets::{FindDatasetEnvVarError, ResolvedDataset};
use thiserror::Error;

use crate::engine::{EngineError, ProcessError, normalize_logs};
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait PollingIngestService: Send + Sync {
    /// Runs one ingest iteration of a plan from [`PollingIngestPlanner`]:
    /// fetches from the polling source and appends the new blocks without
    /// moving `HEAD`. The caller commits `HEAD` from `old_head` to `new_head`.
    ///
    /// Reports `begin` and progress to the listener; the caller reports the
    /// outcome, after its commit.
    ///
    /// `operation_id` names this attempt's working directory, which is removed
    /// after a successful iteration and kept after a failed one.
    async fn ingest(
        &self,
        target: ResolvedDataset,
        plan: PollingIngestPlan,
        options: PollingIngestOptions,
        env_vars: &IngestEnvVars,
        operation_id: &str,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResponse, PollingIngestError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct PollingIngestOptions {
    /// Fetch latest data from uncacheable data sources
    pub fetch_uncacheable: bool,
    /// Schema inference configuration
    pub schema_inference: SchemaInferenceOpts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SchemaInferenceOpts {
    /// Whether to auto-rename a column if it conflicts with one of the system
    /// columns
    pub rename_on_conflict_with_system_column: bool,
    /// Whether to attempt to coerce the event time column into a timestamp
    pub coerce_event_time_column_type: bool,
}

impl Default for SchemaInferenceOpts {
    fn default() -> Self {
        Self {
            rename_on_conflict_with_system_column: true,
            coerce_event_time_column_type: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PollingIngestResponse {
    pub result: PollingIngestResult,
    /// Writer state after the new blocks, to plan the next iteration from
    /// once `HEAD` is committed. Present when the result is `Updated`.
    pub metadata_state: Option<Box<DataWriterMetadataState>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollingIngestResult {
    UpToDate {
        /// The source has data but no source state, and fetching uncacheable
        /// sources was not requested
        uncacheable: bool,
    },
    Updated {
        old_head: odf::Multihash,
        new_head: odf::Multihash,
        /// The source has more data than this iteration fetched
        has_more: bool,
        uncacheable: bool,
        /// Blocks, records and watermark between `old_head` and `new_head`
        increment: odf::dataset::MetadataChainIncrementInterval,
        /// Source state recorded by the new blocks
        new_source_state: Option<odf::metadata::SourceState>,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Listener
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingIngestStage {
    CheckCache,
    Fetch,
    Prepare,
    Read,
    Preprocess,
    Merge,
    Commit,
}

/// Progress of a polling ingest. The ingest service reports `begin` and the
/// progress of each iteration; the pull reports `success` once an iteration
/// is committed, and ends with `error` or `no_polling_source` otherwise
pub trait PollingIngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_cache_hit(&self, _created_at: &DateTime<Utc>) {}
    fn on_stage_progress(&self, _stage: PollingIngestStage, _progress: u64, _out_of: TotalSteps) {}
    fn success(&self, _result: &PollingIngestResult) {}
    fn error(&self, _error: &PullError) {}
    /// The dataset defines no polling source, so there is nothing to ingest
    fn no_polling_source(&self) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub enum TotalSteps {
    Unknown,
    Exact(u64),
}

pub struct NullPollingIngestListener;
impl PollingIngestListener for NullPollingIngestListener {}

pub trait PollingIngestMultiListener: Send + Sync {
    fn begin_ingest(
        &self,
        _dataset: &odf::DatasetHandle,
    ) -> Option<Arc<dyn PollingIngestListener>> {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Environment variable {name} not set")]
pub struct IngestParameterNotFound {
    pub name: String,
}

impl IngestParameterNotFound {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Error, Debug)]
#[error("Invalid environment variable {name} format '{value}'")]
pub struct InvalidIngestParameterFormat {
    pub name: String,
    pub value: String,
}

impl InvalidIngestParameterFormat {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Error, Debug)]
pub enum TemplateError {
    #[error(transparent)]
    ValueNotFound(TemplateValueNotFoundError),

    #[error(transparent)]
    InvalidPattern(TemplateInvalidPatternError),
}

#[derive(Error, Debug)]
#[error("Missing values for variable(s): '{template}'")]
pub struct TemplateValueNotFoundError {
    pub template: String,
}

impl TemplateValueNotFoundError {
    pub fn new(template: impl Into<String>) -> Self {
        Self {
            template: template.into(),
        }
    }
}

#[derive(Error, Debug)]
#[error("Invalid pattern '{pattern}' encountered")]
pub struct TemplateInvalidPatternError {
    pub pattern: String,
}

impl TemplateInvalidPatternError {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

// TODO: Revisit error granularity
#[derive(Debug, Error)]
pub enum PollingIngestError {
    #[error("Source is unreachable at {path}")]
    Unreachable {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },

    #[error("Source not found at {path}")]
    NotFound {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },

    #[error(transparent)]
    ImagePull(
        #[from]
        #[backtrace]
        ImagePullError,
    ),

    #[error(transparent)]
    ParameterNotFound(
        #[from]
        #[backtrace]
        IngestParameterNotFound,
    ),

    #[error(transparent)]
    ProcessError(
        #[from]
        #[backtrace]
        ProcessError,
    ),

    #[error(transparent)]
    PipeError(
        #[from]
        #[backtrace]
        PipeError,
    ),

    #[error(transparent)]
    ReadError(
        #[from]
        #[backtrace]
        ReadError,
    ),

    #[error("Engine provisioning error")]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),

    #[error("Engine error")]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
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
        MergeError,
    ),

    #[error(transparent)]
    ExecutionError(
        #[from]
        #[backtrace]
        ExecutionError,
    ),

    #[error(transparent)]
    DataValidation(
        #[from]
        #[backtrace]
        DataValidationError,
    ),

    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        odf::dataset::CommitError,
    ),

    #[error(transparent)]
    InvalidParameterFormat(
        #[from]
        #[backtrace]
        InvalidIngestParameterFormat,
    ),

    #[error(transparent)]
    TemplateError(
        #[from]
        #[backtrace]
        TemplateError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<FindDatasetEnvVarError> for PollingIngestError {
    fn from(value: FindDatasetEnvVarError) -> Self {
        match value {
            FindDatasetEnvVarError::NotFound(e) => {
                Self::ParameterNotFound(IngestParameterNotFound::new(e.dataset_env_var_key))
            }
            FindDatasetEnvVarError::Internal(e) => Self::Internal(e),
        }
    }
}

impl PollingIngestError {
    /// Whether running the same ingest again could succeed. Errors caused by
    /// the source definition or by the fetched data fail the same way on a
    /// retry, which resumes from the same savepoint.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::EngineError(e) => e.is_recoverable(),
            Self::EngineProvisioningError(e) => e.is_recoverable(),

            Self::ParameterNotFound(_)
            | Self::InvalidParameterFormat(_)
            | Self::TemplateError(_)
            | Self::ReadError(_)
            | Self::BadInputSchema(_)
            | Self::IncompatibleSchema(_)
            | Self::MergeError(_)
            | Self::ExecutionError(_)
            | Self::DataValidation(_) => false,

            Self::Unreachable { .. }
            | Self::NotFound { .. }
            | Self::ImagePull(_)
            | Self::ProcessError(_)
            | Self::PipeError(_)
            | Self::CommitError(_)
            | Self::Internal(_) => true,
        }
    }

    pub fn unreachable(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        Self::Unreachable {
            path: path.into(),
            source,
        }
    }

    pub fn not_found(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        Self::NotFound {
            path: path.into(),
            source,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct PipeError {
    commands: Vec<String>,
    source: BoxedError,
    backtrace: Backtrace,
    log_files: Vec<PathBuf>,
}

impl PipeError {
    pub fn new(
        log_files: Vec<PathBuf>,
        commands: Vec<String>,
        e: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            commands,
            log_files: normalize_logs(log_files),
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for PipeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pipe error: ")?;
        writeln!(f, "Commands: {} failed ", self.commands.join(";"))?;
        write!(f, "with message: {}", self.source)?;

        if !self.log_files.is_empty() {
            writeln!(f, ", see log files for details:")?;
            for path in &self.log_files {
                writeln!(f, "- {}", path.display())?;
            }
        }

        Ok(())
    }
}
