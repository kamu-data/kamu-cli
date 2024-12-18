// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PushIngestExecutor: Send + Sync {
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
    async fn ingest_from_stream(
        &self,
        target: ResolvedDataset,
        plan: PushIngestPlan,
        data: Box<dyn AsyncRead + Send + Unpin>,
        listener: Option<Arc<dyn PushIngestListener>>,
    ) -> Result<PushIngestResult, PushIngestError>;
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
