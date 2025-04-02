// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::{
    DataSource,
    MediaType,
    PushIngestError,
    PushIngestListener,
    PushIngestPlanningError,
    PushIngestResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait IngestDataUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
        data_source: DataSource,
        options: IngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, IngestDataError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct IngestDataUseCaseOptions {
    pub source_name: Option<String>,
    pub source_event_time: Option<DateTime<Utc>>,
    pub is_ingest_from_upload: bool,
    pub media_type: Option<MediaType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum IngestDataUseCaseDataSource {
    Stream(Box<dyn AsyncRead + Send + Unpin>),
    Url(url::Url),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum IngestDataError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Planning(
        #[from]
        #[backtrace]
        PushIngestPlanningError,
    ),

    #[error(transparent)]
    Execution(
        #[from]
        #[backtrace]
        PushIngestError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
