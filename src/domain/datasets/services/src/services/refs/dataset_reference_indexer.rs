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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetReferenceRepository,
    GetDatasetEntryError,
    JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_ENTRY_INDEXER,
    ],
    requires_transaction: true,
})]
pub struct DatasetReferenceIndexer {
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
    dataset_entry_repo: Arc<dyn kamu_datasets::DatasetEntryRepository>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
}

impl DatasetReferenceIndexer {
    async fn has_references_indexed(&self) -> Result<bool, InternalError> {
        self.dataset_reference_repo.has_any_references().await
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetReferenceIndexer::index_dataset_references"
    )]
    async fn index_dataset_references(&self) -> Result<(), InternalError> {
        use futures::TryStreamExt;

        let dataset_ids: Vec<_> = self
            .dataset_storage_unit
            .stored_dataset_ids()
            .try_collect()
            .await?;

        for dataset_id in dataset_ids {
            let dataset = self
                .dataset_storage_unit
                .get_stored_dataset_by_id(&dataset_id)
                .await
                .int_err()?;

            match self.dataset_entry_repo.get_dataset_entry(&dataset_id).await {
                Ok(_) => {}
                Err(GetDatasetEntryError::NotFound(_)) => {
                    tracing::warn!(
                        "Dataset entry for dataset {} not found, skipping reference indexing",
                        dataset_id
                    );
                    continue;
                }
                Err(e) => return Err(e.int_err()),
            }

            let head = dataset
                .as_metadata_chain()
                .as_uncached_ref_repo()
                .get(odf::BlockRef::Head.as_str())
                .await
                .int_err()?;

            self.dataset_reference_repo
                .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, &head)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetReferenceIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetReferenceIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.has_references_indexed().await? {
            tracing::debug!("Skip initialization: datasets references have already indexed");

            return Ok(());
        }

        self.index_dataset_references().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
