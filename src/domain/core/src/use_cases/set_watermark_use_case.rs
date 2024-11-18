// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::DatasetHandle;

use crate::{SetWatermarkError, SetWatermarkResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SetWatermarkUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
