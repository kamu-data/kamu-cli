// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_datasets::ResolvedDataset;
use thiserror::Error;

use crate::DataWriterMetadataState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SetWatermarkPlanner: Send + Sync {
    async fn plan_set_watermark(
        &self,
        target: ResolvedDataset,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkPlan, SetWatermarkPlanningError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SetWatermarkPlan {
    pub system_time: DateTime<Utc>,
    pub new_watermark: DateTime<Utc>,
    pub metadata_state: Box<DataWriterMetadataState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SetWatermarkPlanningError {
    #[error("Attempting to set watermark on a derivative dataset")]
    IsDerivative,

    #[error("Attempting to set watermark on a remote dataset")]
    IsRemote,

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
