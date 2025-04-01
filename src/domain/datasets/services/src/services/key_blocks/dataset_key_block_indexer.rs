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
use futures::StreamExt;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::ResolvedDataset;
use kamu_datasets::{DatasetKeyBlock, DatasetKeyBlockRepository, MetadataEventType};
use odf::dataset::MetadataChainExt;

use crate::{
    JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyBlockIndexer {
    dataset_registry: Arc<dyn kamu_core::DatasetRegistry>,
    dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
    ],
    requires_transaction: true,
})]
impl DatasetKeyBlockIndexer {
    pub fn new(
        dataset_registry: Arc<dyn kamu_core::DatasetRegistry>,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_key_block_repo,
        }
    }

    async fn has_blocks(&self, dataset_id: &odf::DatasetID) -> Result<bool, InternalError> {
        self.dataset_key_block_repo
            .has_blocks(dataset_id, &odf::BlockRef::Head)
            .await
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(dataset_handle = ?target.get_handle())
    )]
    async fn index_dataset_key_blocks(&self, target: ResolvedDataset) -> Result<(), InternalError> {
        let blocks_stream = target.as_metadata_chain().iter_blocks();

        use futures::stream::TryStreamExt;
        use odf::metadata::MetadataEventExt;

        // Collect key metadata blocks
        let key_metadata_blocks: Vec<_> = blocks_stream
            .filter_map(|result| async move {
                match result {
                    Ok((block_hash, block)) if block.event.is_key_event() => {
                        Some(Ok((block_hash, block)))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .try_collect()
            .await
            .int_err()?;

        // Transform metadata blocks into key blocks
        let key_blocks: Vec<_> = key_metadata_blocks
            .into_iter()
            .map(|(block_hash, block)| {
                Ok(DatasetKeyBlock {
                    event_kind: MetadataEventType::from_metadata_event(&block.event),
                    sequence_number: block.sequence_number,
                    block_hash,
                    event_payload: serde_json::to_value(&block.event).int_err()?,
                    created_at: block.system_time,
                })
            })
            .collect::<Result<_, InternalError>>()?;

        // Save key blocks in a batch
        self.dataset_key_block_repo
            .save_blocks_batch(target.get_id(), &odf::BlockRef::Head, &key_blocks)
            .await
            .int_err()?;

        Ok(())
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
        // Scan all datasets and index their key blocks, unless indexed already.
        use futures::stream::TryStreamExt;
        let mut dataset_handles_stream = self.dataset_registry.all_dataset_handles();
        while let Some(dataset_handle) = dataset_handles_stream.try_next().await? {
            tracing::debug!(?dataset_handle, "Checking if dataset index exists");
            if self.has_blocks(&dataset_handle.id).await? {
                tracing::debug!(?dataset_handle, "Dataset key block index already exists");
                continue;
            }

            let target = self
                .dataset_registry
                .get_dataset_by_handle(&dataset_handle)
                .await;
            if let Err(err) = self.index_dataset_key_blocks(target).await {
                tracing::error!(?dataset_handle, error = ?err, "Failed to index dataset key blocks");
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
