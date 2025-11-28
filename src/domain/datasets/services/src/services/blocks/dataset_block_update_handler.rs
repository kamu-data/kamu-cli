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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetBlock,
    DatasetDataBlockRepository,
    DatasetKeyBlockRepository,
    DatasetKeyBlocksMessage,
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    DatasetRegistry,
    DatasetRegistryExt,
    MESSAGE_CONSUMER_KAMU_DATASET_BLOCK_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ResolvedDataset,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_BLOCK_UPDATE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetBlockUpdateHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
    dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetBlockUpdateHandler {
    async fn handle_dataset_reference_updated_message(
        &self,
        message: &DatasetReferenceMessageUpdated,
    ) -> Result<(), InternalError> {
        let target = match self
            .dataset_registry
            .get_dataset_by_id(&message.dataset_id)
            .await
        {
            Ok(target) => Ok(target),
            Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
                tracing::error!(
                    %message.dataset_id, err = ?e,
                    "Updating dataset blocks skipped. Dataset not found."
                );
                return Ok(());
            }
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
        }?;

        let blocks_response = self.update_dataset_blocks(target, message).await?;

        // Send key blocks message if there are any key blocks
        if !blocks_response.key_blocks.is_empty() {
            let head_key_block = &blocks_response.key_blocks[0];
            let tail_key_block = &blocks_response.key_blocks[blocks_response.key_blocks.len() - 1];

            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
                    DatasetKeyBlocksMessage::appended(
                        &message.dataset_id,
                        &message.block_ref,
                        &tail_key_block.block_hash,
                        &head_key_block.block_hash,
                        blocks_response.key_event_flags,
                        blocks_response.divergence_detected,
                    ),
                )
                .await?;
        }

        Ok(())
    }

    async fn update_dataset_blocks(
        &self,
        target: ResolvedDataset,
        message: &DatasetReferenceMessageUpdated,
    ) -> Result<CollectBlocksResponse, InternalError> {
        // Perform a single scan to collect both key and data blocks
        match collect_dataset_blocks_in_range(
            target.clone(),
            &message.new_block_hash,
            message.maybe_prev_block_hash.as_ref(),
        )
        .await
        {
            // We succeeded!
            Ok(blocks_response) => {
                if blocks_response.divergence_detected {
                    tracing::warn!(
                        %message.dataset_id,
                        new_head = %message.new_block_hash,
                        maybe_prev_head = ?message.maybe_prev_block_hash,
                        "Detected the dataset has diverged. Wiping the old indexes entirely"
                    );

                    // Drop existing blocks, as they might conflict with the new results
                    self.dataset_key_block_repo
                        .delete_all_key_blocks_for_ref(&message.dataset_id, &message.block_ref)
                        .await
                        .int_err()?;

                    self.dataset_data_block_repo
                        .delete_all_data_blocks_for_ref(&message.dataset_id, &message.block_ref)
                        .await
                        .int_err()?;
                }

                // Save batch of the key blocks
                if !blocks_response.key_blocks.is_empty() {
                    self.dataset_key_block_repo
                        .save_key_blocks_batch(
                            &message.dataset_id,
                            &message.block_ref,
                            &blocks_response.key_blocks,
                        )
                        .await
                        .int_err()?;
                }

                // Save batch of the data blocks
                if !blocks_response.data_blocks.is_empty() {
                    self.dataset_data_block_repo
                        .save_data_blocks_batch(
                            &message.dataset_id,
                            &message.block_ref,
                            &blocks_response.data_blocks,
                        )
                        .await
                        .int_err()?;
                }

                tracing::debug!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    key_blocks_count = blocks_response.key_blocks.len(),
                    data_blocks_count = blocks_response.data_blocks.len(),
                    "Dataset blocks updated in single scan"
                );

                Ok(blocks_response)
            }

            // Unexpected indexing error
            Err(e) => {
                tracing::error!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    err = ?e,
                    "Failed to index update of dataset blocks"
                );
                Err(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetBlockUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetBlockUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetBlockUpdateHandler[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference update message");

        match message {
            DatasetReferenceMessage::Updated(message) => {
                self.handle_dataset_reference_updated_message(message).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Collects dataset blocks in a range with a single scan, returning both key
/// and data blocks
pub(crate) async fn collect_dataset_blocks_in_range(
    target: ResolvedDataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
) -> Result<CollectBlocksResponse, InternalError> {
    use futures::stream::TryStreamExt;

    // Resulting blocks and event flags for both types
    let mut key_blocks = Vec::new();
    let mut data_blocks = Vec::new();
    let mut key_event_flags = odf::metadata::MetadataEventTypeFlags::empty();
    let mut data_event_flags = odf::metadata::MetadataEventTypeFlags::empty();
    let mut divergence_detected = false;

    // Iterate over blocks in the dataset in the specified range.
    // Note: don't ignore missing tail, we want to detect InvalidInterval error.
    //       Therefore, we need to iterate through all blocks to perform an accurate
    // tail check.
    let mut blocks_stream = target
        .as_metadata_chain()
        .as_uncached_chain()
        .iter_blocks_interval(head.into(), tail.map(Into::into), false);

    loop {
        // Try reading next stream element
        let try_next_result = match blocks_stream.try_next().await {
            // Normal stream element
            Ok(maybe_hashed_block) => maybe_hashed_block,

            // Invalid interval: return so far collected result with divergence marker
            Err(odf::dataset::IterBlocksError::InvalidInterval(_)) => {
                divergence_detected = true;
                break;
            }

            // Other errors are internal
            Err(odf::IterBlocksError::Internal(e)) => return Err(e),
            Err(e) => return Err(e.int_err()),
        };

        // Check if we've reached the end of stream
        let Some((block_hash, block)) = try_next_result else {
            break;
        };

        let event_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);
        let block_entity = crate::make_dataset_block(block_hash, &block);

        if event_flags.has_data_flags() {
            // This is a data block (AddData, ExecuteTransform)
            data_blocks.push(block_entity);
            data_event_flags |= event_flags;
        } else {
            // This is a key block (everything else)
            key_blocks.push(block_entity);
            key_event_flags |= event_flags;
        }
    }

    Ok(CollectBlocksResponse {
        key_blocks,
        data_blocks,
        key_event_flags,
        _data_event_flags: data_event_flags,
        divergence_detected,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct CollectBlocksResponse {
    pub(crate) key_blocks: Vec<DatasetBlock>,
    pub(crate) data_blocks: Vec<DatasetBlock>,
    pub(crate) key_event_flags: odf::metadata::MetadataEventTypeFlags,
    pub(crate) _data_event_flags: odf::metadata::MetadataEventTypeFlags,
    pub(crate) divergence_detected: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
