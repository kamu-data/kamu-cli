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
use opendatafabric::Multihash;

use super::SetWatermarkError;
use crate::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WatermarkService: Send + Sync {
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
