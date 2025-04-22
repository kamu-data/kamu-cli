// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, meta, Catalog};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetReferenceMessage,
    DatasetReferenceRepository,
    DatasetReferenceService,
    GetDatasetReferenceError,
    SetDatasetReferenceError,
    MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
    Outbox,
    OutboxExt,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetReferenceService)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
    feeding_producers: &[
        MESSAGE_CONSUMER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetReferenceServiceImpl {
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceService for DatasetReferenceServiceImpl {
    async fn get_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        self.dataset_reference_repo
            .get_dataset_reference(dataset_id, block_ref)
            .await
    }

    async fn set_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        tracing::debug!(
            %dataset_id,
            %block_ref,
            ?maybe_prev_block_hash,
            %new_block_hash,
            "Setting dataset reference in persistent storage"
        );

        // Try repository operation
        self.dataset_reference_repo
            .set_dataset_reference(dataset_id, block_ref, maybe_prev_block_hash, new_block_hash)
            .await
            .map_err(|e| match e {
                SetDatasetReferenceError::CASFailed(e) => e.int_err(),
                SetDatasetReferenceError::Internal(e) => e,
            })?;

        // Send outbox message
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
                DatasetReferenceMessage::updated(
                    dataset_id,
                    block_ref,
                    maybe_prev_block_hash,
                    new_block_hash,
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetReferenceServiceImpl {}

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetReferenceServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetReferenceServiceImpl[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        match message {
            DatasetReferenceMessage::Updated(updated_message) => {
                // Update reference at storage level
                self.dataset_storage_unit_writer
                    .write_dataset_reference(
                        &updated_message.dataset_id,
                        &updated_message.block_ref,
                        &updated_message.new_block_hash,
                    )
                    .await
                    .int_err()?;

                tracing::debug!(
                    dataset_id = %updated_message.dataset_id,
                    block_ref = %updated_message.block_ref,
                    new_block_hash = %updated_message.new_block_hash,
                    "Storage-level dataset references updated"
                );
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
