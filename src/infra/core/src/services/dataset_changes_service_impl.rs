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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRegistry,
    DatasetRegistryExt,
    GetIncrementError,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetChangesServiceImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetChangesService)]
impl DatasetChangesServiceImpl {
    pub fn new(dataset_registry: Arc<dyn DatasetRegistry>) -> Self {
        Self { dataset_registry }
    }

    async fn resolve_dataset_by_id(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<ResolvedDataset, GetIncrementError> {
        self.dataset_registry
            .get_dataset_by_ref(&dataset_id.as_local_ref())
            .await
            .map_err(|e| match e {
                odf::dataset::GetDatasetError::NotFound(e) => GetIncrementError::DatasetNotFound(e),
                odf::dataset::GetDatasetError::Internal(e) => GetIncrementError::Internal(e),
            })
    }

    async fn resolve_dataset_head(
        &self,
        resolved_dataset: &ResolvedDataset,
    ) -> Result<odf::Multihash, GetIncrementError> {
        resolved_dataset
            .as_metadata_chain()
            .as_reference_repo()
            .get(odf::BlockRef::Head.as_str())
            .await
            .map_err(|e| match e {
                odf::storage::GetRefError::Access(e) => GetIncrementError::Access(e),
                odf::storage::GetRefError::NotFound(e) => GetIncrementError::RefNotFound(e),
                odf::storage::GetRefError::Internal(e) => GetIncrementError::Internal(e),
            })
    }

    // TODO: PERF: Avoid multiple passes over metadata chain
    async fn make_increment_from_interval(
        &self,
        resolved_dataset: &ResolvedDataset,
        old_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<DatasetIntervalIncrement, InternalError> {
        // Analysis outputs
        let mut num_blocks = 0;
        let mut num_records = 0;
        let mut updated_watermark = None;

        // The watermark seen nearest to new head
        let mut latest_watermark = None;

        // Scan blocks (from new head to old head)
        let mut block_stream = resolved_dataset
            .as_metadata_chain()
            .iter_blocks_interval(new_head, old_head, false);

        while let Some((_, block)) = block_stream.try_next().await.int_err()? {
            // Each block counts
            num_blocks += 1;

            // Count added records in data blocks
            num_records += match &block.event {
                odf::MetadataEvent::AddData(add_data) => add_data
                    .new_data
                    .as_ref()
                    .map(odf::DataSlice::num_records)
                    .unwrap_or_default(),
                odf::MetadataEvent::ExecuteTransform(execute_transform) => execute_transform
                    .new_data
                    .as_ref()
                    .map(odf::DataSlice::num_records)
                    .unwrap_or_default(),
                _ => 0,
            };

            // If we haven't decided on the updated watermark yet, analyze watermarks
            if updated_watermark.is_none() {
                // Extract watermark of this block, if present
                let block_watermark = match &block.event {
                    odf::MetadataEvent::AddData(add_data) => add_data.new_watermark,
                    odf::MetadataEvent::ExecuteTransform(execute_transform) => {
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
        drop(block_stream);

        // We have reach the end of pulled interval.
        // If we've seen some watermark, but not the previous one within the changed
        // interval, we need to look for the previous watermark earlier
        if updated_watermark.is_none()
            && let Some(latest_watermark_ref) = latest_watermark.as_ref()
        {
            // Did we have any head before?
            if let Some(old_head) = &old_head {
                // Yes, so try locating the previous watermark containing node
                use odf::dataset::MetadataChainExt;
                let previous_nearest_watermark = resolved_dataset
                    .as_metadata_chain()
                    .accept_one_by_hash(
                        old_head,
                        odf::dataset::SearchSingleDataBlockVisitor::next(),
                    )
                    .await
                    .int_err()?
                    .into_event()
                    .and_then(|event| event.new_watermark);

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetChangesService for DatasetChangesServiceImpl {
    async fn get_increment_between<'a>(
        &'a self,
        dataset_id: &'a odf::DatasetID,
        old_head: Option<&'a odf::Multihash>,
        new_head: &'a odf::Multihash,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError> {
        let resolved_dataset = self.resolve_dataset_by_id(dataset_id).await?;

        let increment = self
            .make_increment_from_interval(&resolved_dataset, old_head, new_head)
            .await
            .map_err(GetIncrementError::Internal)?;

        Ok(increment)
    }

    async fn get_increment_since<'a>(
        &'a self,
        dataset_id: &'a odf::DatasetID,
        old_head: Option<&'a odf::Multihash>,
    ) -> Result<DatasetIntervalIncrement, GetIncrementError> {
        let resolved_dataset = self.resolve_dataset_by_id(dataset_id).await?;
        let current_head = self.resolve_dataset_head(&resolved_dataset).await?;

        let increment = self
            .make_increment_from_interval(&resolved_dataset, old_head, &current_head)
            .await
            .map_err(GetIncrementError::Internal)?;

        Ok(increment)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
