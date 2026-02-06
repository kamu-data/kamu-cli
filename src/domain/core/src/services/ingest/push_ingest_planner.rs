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
use file_utils::MediaType;
use internal_error::InternalError;
use kamu_datasets::ResolvedDataset;
use thiserror::Error;

use crate::{DataWriterMetadataState, SchemaInferenceOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushIngestPlanner: Send + Sync {
    /// Uses or auto-creates push source definition in metadata to plan
    /// ingestion
    async fn plan_ingest(
        &self,
        target: ResolvedDataset,
        source_name: Option<&str>,
        opts: PushIngestOpts,
    ) -> Result<PushIngestPlan, PushIngestPlanningError>;
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
    /// Expected head block to prevent concurrent updates
    pub expected_head: Option<odf::Multihash>,
    /// Skip the account quota check after ingesting.
    pub ignore_quota_check: bool,
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
    pub push_source: odf::metadata::AddPushSource,
}

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
    HeadNotFound(
        #[from]
        #[backtrace]
        odf::storage::BlockNotFoundError,
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
        odf::dataset::CommitError,
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
#[error("{message}")]
pub struct PushSourceNotFoundError {
    source_name: Option<String>,
    message: String,
}

impl PushSourceNotFoundError {
    pub fn new(source_name: Option<impl Into<String>>) -> Self {
        match source_name {
            None => Self::new_with_messaage(
                source_name,
                "Dataset does not define a default push source, consider specifying the source \
                 name",
            ),
            Some(source_name) => {
                let source_name = source_name.into();
                let message = format!("Dataset does not define a push source '{source_name}'");
                Self::new_with_messaage(Some(source_name), message)
            }
        }
    }

    pub fn new_with_messaage(
        source_name: Option<impl Into<String>>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            source_name: source_name.map(Into::into),
            message: message.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
