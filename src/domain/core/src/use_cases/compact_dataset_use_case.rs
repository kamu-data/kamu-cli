// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::DatasetHandle;

use crate::{
    CompactionError,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionResponse,
    CompactionResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CompactDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError>;

    async fn execute_multi(
        &self,
        dataset_handles: Vec<DatasetHandle>,
        options: CompactionOptions,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Vec<CompactionResponse>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
