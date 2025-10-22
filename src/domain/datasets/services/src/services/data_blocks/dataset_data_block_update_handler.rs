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
    DatasetDataBlockRepository,
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    MESSAGE_CONSUMER_KAMU_DATASET_DATA_BLOCK_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::*;

use crate::{CollectDataBlockResponse, collect_dataset_data_blocks_in_range};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_DATA_BLOCK_UPDATE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetDataBlockUpdateHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetDataBlockUpdateHandler {
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
                    "Updating dataset data blocks skipped. Dataset not found."
                );
                return Ok(());
            }
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
        }?;

        self.update_dataset_data_blocks(target, message).await?;

        Ok(())
    }

    async fn update_dataset_data_blocks(
        &self,
        target: ResolvedDataset,
        message: &DatasetReferenceMessageUpdated,
    ) -> Result<CollectDataBlockResponse, InternalError> {
        // Try to index in the specified range of HEAD advancement
        match collect_dataset_data_blocks_in_range(
            target.clone(),
            &message.new_block_hash,
            message.maybe_prev_block_hash.as_ref(),
        )
        .await
        {
            // We succeeded!
            Ok(data_blocks_response) => {
                // Check if the dataset has diverged: i.e., it was reset or force pushed
                if data_blocks_response.divergence_detected {
                    // In this case we have already collected entire list of data blocks
                    //  from the Head to Seed events
                    tracing::warn!(
                        %message.dataset_id,
                        new_head = %message.new_block_hash,
                        maybe_prev_head = ?message.maybe_prev_block_hash,
                        "Detected the dataset has diverged. Wiping the old index entirely"
                    );

                    // Drop existing data blocks, as they might conflict with the new results
                    self.dataset_data_block_repo
                        .delete_all_for_ref(&message.dataset_id, &message.block_ref)
                        .await
                        .int_err()?;
                }

                // Save batch of the data blocks
                self.dataset_data_block_repo
                    .save_blocks_batch(
                        &message.dataset_id,
                        &message.block_ref,
                        &data_blocks_response.data_blocks,
                    )
                    .await
                    .int_err()?;

                tracing::debug!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    "Dataset data blocks updated"
                );
                Ok(data_blocks_response)
            }

            // Unexpected indexing error
            Err(e) => {
                tracing::error!(
                    %message.dataset_id,
                    new_head = %message.new_block_hash,
                    maybe_prev_head = ?message.maybe_prev_block_hash,
                    err = ?e,
                    "Failed to index update of dataset data blocks"
                );
                Err(e)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetDataBlockUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetDataBlockUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetDataBlockUpdateHandler[DatasetReferenceMessage]"
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
