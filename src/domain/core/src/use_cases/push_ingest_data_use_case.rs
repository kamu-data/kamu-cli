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
use file_utils::MediaType;
use internal_error::InternalError;
use kamu_datasets::ResolvedDataset;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::{
    DataSource,
    PushIngestError,
    PushIngestListener,
    PushIngestPlanningError,
    PushIngestResult,
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushIngestDataUseCase: Send + Sync {
    async fn execute(
        &self,
        target: ResolvedDataset,
        data_source: DataSource,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError>;

    async fn execute_multi(
        &self,
        target: ResolvedDataset,
        data_sources: Vec<DataSource>,
        options: PushIngestDataUseCaseOptions,
        listener_maybe: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestDataError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct PushIngestDataUseCaseOptions {
    pub source_name: Option<String>,
    pub source_event_time: Option<DateTime<Utc>>,
    pub is_ingest_from_upload: bool,
    pub media_type: Option<MediaType>,
    pub expected_head: Option<odf::Multihash>,
    pub skip_quota_check: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum PushIngestDataUseCaseDataSource {
    Stream(Box<dyn AsyncRead + Send + Unpin>),
    Url(url::Url),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushIngestDataError {
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
