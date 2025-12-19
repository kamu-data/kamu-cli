// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::{transactional_method1, transactional_method3};
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetStatisticsRepository,
    GetDatasetEntryError,
    JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    JOB_KAMU_DATASETS_DATASET_STATISTICS_INDEXER,
};

use crate::compute_dataset_statistics_increment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetStatisticsIndexer {
    catalog: Catalog,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_STATISTICS_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: false,
})]
impl DatasetStatisticsIndexer {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    #[transactional_method1(dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>)]
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        dataset_statistics_repo.has_any_stats().await
    }

    #[transactional_method1(dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>)]
    async fn get_dataset_ids(&self) -> Result<Vec<odf::DatasetID>, InternalError> {
        use futures::TryStreamExt;

        dataset_storage_unit
            .stored_dataset_ids()
            .try_collect()
            .await
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetStatisticsIndexer::index_dataset_statistics"
    )]
    async fn index_dataset_statistics(&self) -> Result<(), InternalError> {
        use futures::stream::{self, TryStreamExt};

        let dataset_ids = self.get_dataset_ids().await?;

        // Process datasets in parallel with per-dataset transactions using all
        // available cores
        let parallelism = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(1);
        stream::iter(dataset_ids.into_iter().map(Ok::<_, InternalError>))
            .try_for_each_concurrent(parallelism, |dataset_id| async move {
                self.process_dataset(dataset_id).await
            })
            .await?;

        Ok(())
    }

    #[transactional_method3(
        dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
        dataset_entry_repo: Arc<dyn kamu_datasets::DatasetEntryRepository>,
        dataset_statistics_repo: Arc<dyn DatasetStatisticsRepository>
    )]
    async fn process_dataset(&self, dataset_id: odf::DatasetID) -> Result<(), InternalError> {
        let dataset = dataset_storage_unit
            .get_stored_dataset_by_id(&dataset_id)
            .await
            .int_err()?;

        match dataset_entry_repo.get_dataset_entry(&dataset_id).await {
            Ok(_) => {}
            Err(GetDatasetEntryError::NotFound(_)) => {
                tracing::warn!(
                    "Dataset entry for dataset {} not found, skipping statistics indexing",
                    dataset_id
                );
                return Ok(());
            }
            Err(e) => return Err(e.int_err()),
        }

        let head = dataset
            .as_metadata_chain()
            .as_uncached_ref_repo()
            .get(odf::BlockRef::Head.as_str())
            .await
            .int_err()?;

        let increment = compute_dataset_statistics_increment(
            dataset.as_metadata_chain().as_uncached_chain(),
            &head,
            None,
        )
        .await?;

        assert!(increment.seen_seed);
        dataset_statistics_repo
            .set_dataset_statistics(&dataset_id, &odf::BlockRef::Head, increment.statistics)
            .await
            .int_err()?;

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
