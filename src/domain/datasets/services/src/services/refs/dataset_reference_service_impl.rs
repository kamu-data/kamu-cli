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
use kamu_core::{DatasetRegistry, DatasetRegistryExt};
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
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
    Outbox,
    OutboxExt,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetReferenceServiceImpl {
    dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
    outbox: Arc<dyn Outbox>,
}

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
})]
impl DatasetReferenceServiceImpl {
    pub fn new(
        dataset_reference_repo: Arc<dyn DatasetReferenceRepository>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_reference_repo,
            outbox,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceService for DatasetReferenceServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(dataset_id, block_ref))]
    async fn get_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        self.dataset_reference_repo
            .get_dataset_reference(dataset_id, block_ref)
            .await
    }

    #[tracing::instrument(level = "info", skip_all, fields(dataset_id, block_ref))]
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
                DatasetReferenceMessage::new(
                    dataset_id.clone(),
                    block_ref.clone(),
                    maybe_prev_block_hash.cloned(),
                    new_block_hash.clone(),
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
        transactional_catalog: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        let dataset_registry = transactional_catalog
            .get_one::<dyn DatasetRegistry>()
            .unwrap();

        // Resolve dataset
        let dataset = dataset_registry
            .get_dataset_by_id(&message.dataset_id)
            .await
            .int_err()?;

        // Update reference at storage level
        dataset
            .as_metadata_chain()
            .as_uncached_ref_repo() // Access storage level directly!
            .set(message.block_ref.as_str(), &message.new_block_hash)
            .await
            .int_err()?;

        tracing::debug!(
            dataset_id = %message.dataset_id,
            block_ref = %message.block_ref,
            new_block_hash = %message.new_block_hash,
            "Storage-level dataset references updated"
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
