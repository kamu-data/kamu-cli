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
use futures::TryStreamExt;
use kamu_core::{
    Dataset,
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRepository,
    GetDatasetError,
    GetIntervalIncrementError,
    InternalError,
    ResultIntoInternal,
    TryStreamExtExt,
};
use opendatafabric::{DataSlice, DatasetID, MetadataEvent, Multihash};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetChangesServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetChangesService)]
impl DatasetChangesServiceImpl {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }

    // Scan updated dataset to detect statistics for task system result
    async fn make_dataset_update_result_from_updated_pull(
        &self,
        dataset: Arc<dyn Dataset>,
        old_head: Option<&Multihash>,
        new_head: &Multihash,
    ) -> Result<DatasetIntervalIncrement, InternalError> {
        // Analysis outputs
        let mut num_blocks = 0;
        let mut num_records = 0;
        let mut updated_watermark = None;

        // The watermark seen nearest to new head
        let mut latest_watermark = None;

        // Scan blocks (from new head to old head)
        let mut block_stream = dataset
            .as_metadata_chain()
            .iter_blocks_interval(new_head, old_head, false);

        while let Some((_, block)) = block_stream.try_next().await.int_err()? {
            // Each block counts
            num_blocks += 1;

            // Count added records in data blocks
            num_records += match &block.event {
                MetadataEvent::AddData(add_data) => add_data
                    .new_data
                    .as_ref()
                    .map(DataSlice::num_records)
                    .unwrap_or_default(),
                MetadataEvent::ExecuteTransform(execute_transform) => execute_transform
                    .new_data
                    .as_ref()
                    .map(DataSlice::num_records)
                    .unwrap_or_default(),
                _ => 0,
            };

            // If we haven't decided on the updated watermark yet, analyze watermarks
            if updated_watermark.is_none() {
                // Extract watermark of this block, if present
                let block_watermark = match &block.event {
                    MetadataEvent::AddData(add_data) => add_data.new_watermark,
                    MetadataEvent::ExecuteTransform(execute_transform) => {
                        execute_transform.new_watermark
                    }
                    _ => None,
                };
                if let Some(block_watermark) = block_watermark {
                    // Did we have a watermark already since the start of scanning?
                    if let Some(latest_watermark_ref) = latest_watermark.as_ref() {
                        // Yes, so if we see a different watermark now, it means it was
                        // updated in this pull result
                        if block_watermark != *latest_watermark_ref {
                            updated_watermark = Some(*latest_watermark_ref);
                        }
                    } else {
                        // No, so remember the latest watermark
                        latest_watermark = Some(block_watermark);
                    }
                }
            }
        }

        // Drop stream to unborrow old_head/new_head references
        std::mem::drop(block_stream);

        // We have reach the end of pulled interval.
        // If we've seen some watermark, but not the previous one within the changed
        // interval, we need to look for the previous watermark earlier
        if updated_watermark.is_none()
            && let Some(latest_watermark_ref) = latest_watermark.as_ref()
        {
            // Did we have any head before?
            if let Some(old_head) = &old_head {
                // Yes, so try locating the previous watermark - earliest different
                // watermark
                let prev_different_watermark = dataset
                    .as_metadata_chain()
                    .iter_blocks_interval(old_head, None, false)
                    .filter_data_stream_blocks()
                    .filter_map_ok(|(_, b)| {
                        b.event.new_watermark.and_then(|new_watermark| {
                            if new_watermark != *latest_watermark_ref {
                                Some(new_watermark)
                            } else {
                                None
                            }
                        })
                    })
                    .try_first()
                    .await
                    .int_err()?;
                updated_watermark = prev_different_watermark.or(latest_watermark);
            } else {
                // It's a first pull, the latest watermark is an update
                updated_watermark = latest_watermark;
            }
        }

        Ok(DatasetIntervalIncrement {
            num_blocks,
            num_records,
            updated_watermark,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetChangesService for DatasetChangesServiceImpl {
    async fn get_interval_increment(
        &self,
        dataset_id: &DatasetID,
        old_head: Option<&Multihash>,
        new_head: &Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIntervalIncrementError> {
        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_id.as_local_ref())
            .await
            .map_err(|e| match e {
                GetDatasetError::NotFound(e) => GetIntervalIncrementError::DatasetNotFound(e),
                GetDatasetError::Internal(e) => GetIntervalIncrementError::Internal(e),
            })?;

        let increment = self
            .make_dataset_update_result_from_updated_pull(dataset, old_head, new_head)
            .await
            .map_err(GetIntervalIncrementError::Internal)?;

        Ok(increment)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
