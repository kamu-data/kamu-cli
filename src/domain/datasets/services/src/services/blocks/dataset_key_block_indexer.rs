// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use database_common_macros::transactional_method2;
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::DatasetRegistry;
use kamu_datasets::{
    DatasetKeyBlockRepository,
    JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
};

use crate::DatasetKeyBlockIndexingJob;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyBlockIndexer {
    catalog: Catalog,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
    ],
    requires_transaction: false,
})]
impl DatasetKeyBlockIndexer {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[transactional_method2(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>
    )]
    async fn collect_datasets_to_index(
        &self,
    ) -> Result<Vec<(odf::DatasetHandle, odf::BlockRef)>, InternalError> {
        // Repository can build a report of unindexed dataset branches (id->blockRef)
        #[allow(clippy::zero_sized_map_values)]
        let unindexed_dataset_branches = dataset_key_block_repo
            .list_unindexed_dataset_branches()
            .await?
            .into_iter()
            .collect::<HashMap<odf::DatasetID, odf::BlockRef>>();

        // Resolve dataset handles for unindexed dataset IDs

        let dataset_id_cows = unindexed_dataset_branches
            .keys()
            .map(Cow::Borrowed)
            .collect::<Vec<_>>();

        let dataset_handles_map = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_id_cows)
            .await
            .int_err()?;

        assert!(dataset_handles_map.unresolved_datasets.is_empty());
        assert!(dataset_handles_map.resolved_handles.len() == unindexed_dataset_branches.len());

        // Combine handles with block refs

        let dataset_branches_2_index = dataset_handles_map
            .resolved_handles
            .into_iter()
            .map(|dataset_handle| {
                let block_ref = unindexed_dataset_branches
                    .get(&dataset_handle.id)
                    .cloned()
                    .expect("Dataset ID must exist in unindexed branches map");

                (dataset_handle, block_ref)
            })
            .collect::<Vec<_>>();

        tracing::debug!(
            unindexed_count = dataset_branches_2_index.len(),
            "Finished collecting datasets to index"
        );

        Ok(dataset_branches_2_index)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetKeyBlockIndexer {
    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetKeyBlocksIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Collect dataset handles that have no block index built yet
        let datasets_to_index = self.collect_datasets_to_index().await?;
        if datasets_to_index.is_empty() {
            tracing::debug!("No datasets to index. All datasets have been indexed.");
            return Ok(());
        }

        // Convert handles into jobs wrapped into tokio tasks
        let mut job_results = tokio::task::JoinSet::new();
        for (dataset_handle, block_ref) in datasets_to_index {
            let job =
                DatasetKeyBlockIndexingJob::new(&self.catalog, dataset_handle.clone(), block_ref);
            let job_span = tracing::info_span!("DatasetKeyBlockIndexingJob", %dataset_handle);

            use tracing::Instrument;
            job_results.spawn(
                async move { (dataset_handle, job.run().await) }.instrument(job_span.or_current()),
            );
        }

        // Execute jobs in parallel
        let results = job_results.join_all().await;

        // Report errors, if any
        for (dataset_handle, result) in results {
            if let Err(err) = result {
                tracing::error!(%dataset_handle, err = ?err, "Failed to index dataset key blocks");
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
