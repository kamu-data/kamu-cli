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
use dill::*;
use kamu_core::{
    BlockRef,
    Dataset,
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRepository,
    GenericCallbackVisitor,
    GetDatasetError,
    GetIncrementError,
    GetRefError,
    InternalError,
    MetadataChainExt,
    MetadataVisitorDecision,
    SearchDataBlocksVisitor,
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

    async fn resolve_dataset_by_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Arc<dyn Dataset>, GetIncrementError> {
        self.dataset_repo
            .get_dataset(&dataset_id.as_local_ref())
            .await
            .map_err(|e| match e {
                GetDatasetError::NotFound(e) => GetIncrementError::DatasetNotFound(e),
                GetDatasetError::Internal(e) => GetIncrementError::Internal(e),
            })
    }

    async fn resolve_dataset_head(
        &self,
        dataset: &dyn Dataset,
    ) -> Result<Multihash, GetIncrementError> {
        dataset
            .as_metadata_chain()
            .as_reference_repo()
            .get(&BlockRef::Head)
            .await
            .map_err(|e| match e {
                GetRefError::Access(e) => GetIncrementError::Access(e),
                GetRefError::NotFound(e) => GetIncrementError::RefNotFound(e),
                GetRefError::Internal(e) => GetIncrementError::Internal(e),
            })
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    async fn make_increment_from_interval(
        &self,
        dataset: Arc<dyn Dataset>,
        old_head: Option<&Multihash>,
        new_head: &Multihash,
    ) -> Result<DatasetIntervalIncrement, InternalError> {
        #[derive(Default)]
        struct DataBlockAnalysisVisitorState {
            // Analysis outputs
            num_blocks: u64,
            num_records: u64,
            updated_watermark: Option<DateTime<Utc>>,
            // The watermark seen nearest to new head
            latest_watermark: Option<DateTime<Utc>>,
        }

        // Scan blocks (from new head to old head)
        let mut data_block_analysis_visitor = GenericCallbackVisitor::new(
            DataBlockAnalysisVisitorState::default(),
            MetadataVisitorDecision::Next,
            |state, (_, block)| {
                // Each block counts
                state.num_blocks += 1;

                // Count added records in data blocks
                state.num_records += match &block.event {
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
                if state.updated_watermark.is_none() {
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
                        if let Some(latest_watermark_ref) = state.latest_watermark.as_ref() {
                            // Yes, so if we see a different watermark now, it means it was
                            // updated in this pull result
                            if block_watermark != *latest_watermark_ref {
                                state.updated_watermark = Some(*latest_watermark_ref);
                            }
                        } else {
                            // No, so remember the latest watermark
                            state.latest_watermark = Some(block_watermark);
                        }
                    }
                }

                Ok(MetadataVisitorDecision::Next)
            },
        );

        dataset
            .as_metadata_chain()
            .accept_by_interval::<InternalError>(
                &mut [&mut data_block_analysis_visitor],
                Some(new_head),
                old_head,
            )
            .await?;

        let DataBlockAnalysisVisitorState {
            num_blocks,
            num_records,
            mut updated_watermark,
            latest_watermark,
        } = data_block_analysis_visitor.into_state();

        // We have reach the end of pulled interval.
        // If we've seen some watermark, but not the previous one within the changed
        // interval, we need to look for the previous watermark earlier
        if updated_watermark.is_none()
            && let Some(latest_watermark_ref) = latest_watermark.as_ref()
        {
            // Did we have any head before?
            if let Some(old_head) = &old_head {
                // Yes, so try locating the previous watermark containing node
                let mut visitor = <SearchDataBlocksVisitor>::next_filled_new_watermark();

                dataset
                    .as_metadata_chain()
                    .accept_by_hash(&mut [&mut visitor], old_head)
                    .await?;

                let previous_nearest_watermark = visitor.into_event().and_then(|e| e.new_watermark);

                // The "latest" watermark is only an update, if we can find a different
                // watermark before the searched interval, or if it's a first watermark
                updated_watermark = if let Some(previous_nearest_watermark) =
                    previous_nearest_watermark
                {
                    // There is previous watermark
                    if previous_nearest_watermark != *latest_watermark_ref {
                        // It's different from what we've found on the interval, so it's an update
                        latest_watermark
                    } else {
                        // It's the same as what we've found on the interval, so there is no update
                        None
                    }
                } else {
                    // There was no watermark before, so it's definitely an update
                    latest_watermark
                };
            } else {
                // It's a first pull, the latest watermark is an update, if earlier found
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
    async fn get_increment_between<'a>(
        &'a self,
        dataset_id: &'a DatasetID,
        old_head: Option<&'a Multihash>,
        new_head: &'a Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError> {
        let dataset = self.resolve_dataset_by_id(dataset_id).await?;

        let increment = self
            .make_increment_from_interval(dataset, old_head, new_head)
            .await
            .map_err(GetIncrementError::Internal)?;

        Ok(increment)
    }

    async fn get_increment_since<'a>(
        &'a self,
        dataset_id: &'a DatasetID,
        old_head: Option<&'a Multihash>,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError> {
        let dataset = self.resolve_dataset_by_id(dataset_id).await?;
        let current_head = self.resolve_dataset_head(dataset.as_ref()).await?;

        let increment = self
            .make_increment_from_interval(dataset, old_head, &current_head)
            .await
            .map_err(GetIncrementError::Internal)?;

        Ok(increment)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
