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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use kamu_datasets::{
    DatasetKeyBlockRepository,
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    DatasetReferenceMessageUpdating,
    MESSAGE_CONSUMER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::*;

use crate::collect_dataset_key_blocks_in_range;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_KEY_BLOCK_UPDATE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetKeyBlockUpdateHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetKeyBlockUpdateHandler {
    async fn handle_dataset_reference_updating_message(
        &self,
        message: &DatasetReferenceMessageUpdating,
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
                    "Updating dataset key blocks skipped. Dataset not found."
                );
                return Ok(());
            }
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
        }?;

        self.update_dataset_key_blocks(target, message).await?;

        Ok(())
    }

    async fn update_dataset_key_blocks(
        &self,
        target: ResolvedDataset,
        message: &DatasetReferenceMessageUpdating,
    ) -> Result<(), InternalError> {
        // Try to index in the specified range of HEAD advancement
        match collect_dataset_key_blocks_in_range(
            target.clone(),
            &message.new_block_hash,
            message.maybe_prev_block_hash.as_ref(),
        )
        .await
        {
            // We succeeded!
            Ok(key_blocks_response) => {
                // Check if the dataset has diverged: i.e., it was reset or force pushed
                if key_blocks_response.divergence_detected {
                    // In this case we have already collected entire list of key blocks
                    //  from the Head to Seed events
                    tracing::warn!(
                        %message.dataset_id,
                        new_head = %message.new_block_hash,
                        maybe_prev_head = ?message.maybe_prev_block_hash,
                        "Detected the dataset has diverged. Wiping the old index entirely"
                    );

                    // Drop existing key blocks, as they might conflict with the new results
                    self.dataset_key_block_repo
                        .delete_all_for_ref(&message.dataset_id, &odf::BlockRef::Head)
                        .await
                        .int_err()?;
                }

                // Save batch of the key blocks
                self.dataset_key_block_repo
                    .save_blocks_batch(
                        &message.dataset_id,
                        &odf::BlockRef::Head,
                        &key_blocks_response.key_blocks,
                    )
                    .await
                    .int_err()?;

                tracing::debug!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    "Dataset key blocks updated"
                );
                Ok(())
            }

            // Unexpected indexing error
            Err(e) => {
                tracing::error!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    err = ?e,
                    "Failed to index update of dataset key blocks"
                );
                Err(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetKeyBlockUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetKeyBlockUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetKeyBlockUpdateHandler[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference update message");

        match message {
            DatasetReferenceMessage::Updating(message) => {
                self.handle_dataset_reference_updating_message(message)
                    .await
            }
            DatasetReferenceMessage::Updated(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
