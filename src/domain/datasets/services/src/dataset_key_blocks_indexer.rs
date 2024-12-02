// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_core::DatasetRegistry;

use crate::{
    DatasetKeyBlocksServiceImpl,
    JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    JOB_KAMU_DATASETS_DATASET_KEY_BLOCKS_INDEXER,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyBlocksIndexer {
    _dataset_registry: Arc<dyn DatasetRegistry>,
    _dataset_key_blocks_service: Arc<DatasetKeyBlocksServiceImpl>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_KEY_BLOCKS_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: true,
})]
impl DatasetKeyBlocksIndexer {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_key_blocks_service: Arc<DatasetKeyBlocksServiceImpl>,
    ) -> Self {
        Self {
            _dataset_registry: dataset_registry,
            _dataset_key_blocks_service: dataset_key_blocks_service,
        }
    }

    #[allow(clippy::unused_async)]
    async fn was_indexed(&self) -> Result<bool, InternalError> {
        // TODO
        Ok(false)
    }

    #[allow(clippy::unused_async)]
    async fn index_key_blocks_from_storage(&self) -> Result<(), InternalError> {
        // TODO
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetKeyBlocksIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetKeyBlocksIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.was_indexed().await? {
            tracing::debug!("Skip initialization: dataset key blocks were already indexed");
        } else {
            self.index_key_blocks_from_storage().await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
