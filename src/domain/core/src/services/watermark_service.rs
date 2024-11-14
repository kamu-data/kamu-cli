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
use opendatafabric::Multihash;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;
use crate::{AccessError, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WatermarkService: Send + Sync {
    /// Attempt reading watermark that is currently associated with a dataset
    async fn try_get_current_watermark(
        &self,
        dataset: Arc<dyn Dataset>,
    ) -> Result<Option<DateTime<Utc>>, GetWatermarkError>;

    /// Manually advances the watermark of a root dataset
    async fn set_watermark(
        &self,
        dataset: Arc<dyn Dataset>,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum SetWatermarkResult {
    UpToDate,
    Updated {
        old_head: Option<Multihash>,
        new_head: Multihash,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetWatermarkError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SetWatermarkError {
    #[error("Attempting to set watermark on a derivative dataset")]
    IsDerivative,

    #[error("Attempting to set watermark on a remote dataset")]
    IsRemote,

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

impl From<DatasetActionUnauthorizedError> for SetWatermarkError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
