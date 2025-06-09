// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetStatisticsRepository,
    JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    JOB_KAMU_DATASETS_DATASET_STATISTICS_INDEXER,
};

use crate::compute_dataset_statistics_increment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStatisticsIndexer {
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
    dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_STATISTICS_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: true,
})]
impl DatasetStatisticsIndexer {
    pub fn new(
        dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
        dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>,
    ) -> Self {
        Self {
            dataset_storage_unit,
            dataset_statistics_repo,
        }
    }

    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        self.dataset_statistics_repo.has_any_stats().await
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetStatisticsIndexer::index_dataset_statistics"
    )]
    async fn index_dataset_statistics(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        let dataset_ids: Vec<_> = self
            .dataset_storage_unit
            .stored_dataset_ids()
            .try_collect()
            .await?;

        dbg!("!!!", &dataset_ids);

        for dataset_id in dataset_ids {
            let dataset = self
                .dataset_storage_unit
                .get_stored_dataset_by_id(&dataset_id)
                .await
                .int_err()?;

            let head = dataset
                .as_metadata_chain()
                .as_uncached_ref_repo()
                .get(odf::BlockRef::Head.as_str())
                .await
                .int_err()?;

            let increment =
                compute_dataset_statistics_increment(dataset.as_metadata_chain(), &head, None)
                    .await?;

            assert!(increment.seen_seed);
            self.dataset_statistics_repo
                .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, increment.statistics)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetStatisticsIndexer {
    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetStatisticsIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.has_any_stats().await? {
            tracing::debug!("Skip initialization: datasets statistics have already indexed");

            return Ok(());
        }

        self.index_dataset_statistics().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
