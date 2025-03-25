// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use kamu_datasets::{
    DatasetReferenceMessage,
    DatasetReferenceMessageUpdated,
    DatasetStatistics,
    DatasetStatisticsRepository,
    GetDatasetStatisticsError,
    MESSAGE_CONSUMER_KAMU_DATASET_STATISTICS_UPDATE_HANDLER,
    MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
};
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetReferenceMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_STATISTICS_UPDATE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
})]
#[scope(Singleton)]
pub struct DatasetStatisticsUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetStatisticsUpdateHandler {
    async fn update_dataset_statistics(
        &self,
        dataset_stats_repo: &dyn DatasetStatisticsRepository,
        target: ResolvedDataset,
        updated_message: &DatasetReferenceMessageUpdated,
    ) -> Result<(), InternalError> {
        use odf::dataset::MetadataChainExt;
        let mut block_stream = target.as_metadata_chain().iter_blocks_interval(
            &updated_message.new_block_hash,
            updated_message.maybe_prev_block_hash.as_ref(),
            true,
        );

        let mut increment = DatasetStatistics::default();
        let mut seen_seed = false;

        use tokio_stream::StreamExt;
        while let Some((_, block)) = block_stream.try_next().await.int_err()? {
            match block.event {
                odf::MetadataEvent::Seed(_) => {
                    seen_seed = true;
                }
                odf::MetadataEvent::AddData(add_data) => {
                    increment.last_pulled.get_or_insert(block.system_time);

                    if let Some(output_data) = add_data.new_data {
                        let iv = output_data.offset_interval;
                        increment.num_records += iv.end - iv.start + 1;

                        increment.data_size += output_data.size;
                    }

                    if let Some(checkpoint) = add_data.new_checkpoint {
                        increment.checkpoints_size += checkpoint.size;
                    }
                }
                odf::MetadataEvent::ExecuteTransform(execute_transform) => {
                    increment.last_pulled.get_or_insert(block.system_time);

                    if let Some(output_data) = execute_transform.new_data {
                        let iv = output_data.offset_interval;
                        increment.num_records += iv.end - iv.start + 1;

                        increment.data_size += output_data.size;
                    }

                    if let Some(checkpoint) = execute_transform.new_checkpoint {
                        increment.checkpoints_size += checkpoint.size;
                    }
                }
                odf::MetadataEvent::SetDataSchema(_)
                | odf::MetadataEvent::SetAttachments(_)
                | odf::MetadataEvent::SetInfo(_)
                | odf::MetadataEvent::SetLicense(_)
                | odf::MetadataEvent::SetVocab(_)
                | odf::MetadataEvent::SetTransform(_)
                | odf::MetadataEvent::SetPollingSource(_)
                | odf::MetadataEvent::DisablePollingSource(_)
                | odf::MetadataEvent::AddPushSource(_)
                | odf::MetadataEvent::DisablePushSource(_) => (),
            }
        }

        // If we have seen Seed, the dataset has diverged,
        // and the increment represents the updated situation
        if seen_seed {
            dataset_stats_repo
                .set_dataset_statistics(
                    &updated_message.dataset_id,
                    &updated_message.block_ref,
                    increment,
                )
                .await
                .int_err()
        } else {
            // Otheriwse, load previous stats
            let older_stats = match dataset_stats_repo
                .get_dataset_statistics(&updated_message.dataset_id, &updated_message.block_ref)
                .await
            {
                Ok(stats) => Ok(stats),
                Err(GetDatasetStatisticsError::NotFound(_)) => Ok(DatasetStatistics::default()),
                Err(GetDatasetStatisticsError::Internal(e)) => Err(e),
            }?;

            // Save old + increment as new stats
            let new_stats = older_stats.with_increment(&increment);
            dataset_stats_repo
                .set_dataset_statistics(
                    &updated_message.dataset_id,
                    &updated_message.block_ref,
                    new_stats,
                )
                .await
                .int_err()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for DatasetStatisticsUpdateHandler {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetReferenceMessage> for DatasetStatisticsUpdateHandler {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DatasetStatisticsUpdateHandler[DatasetReferenceMessage]"
    )]
    async fn consume_message(
        &self,
        transaction_catalog: &Catalog,
        message: &DatasetReferenceMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset reference message");

        match message {
            DatasetReferenceMessage::Updated(updated_message) => {
                let dataset_registry = transaction_catalog
                    .get_one::<dyn DatasetRegistry>()
                    .unwrap();

                let target = match dataset_registry
                    .get_dataset_by_id(&updated_message.dataset_id)
                    .await
                {
                    Ok(target) => Ok(target),
                    Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
                        tracing::error!(
                            %updated_message.dataset_id, err = ?e,
                            "Updating dataset statistics skipped. Dataset not found."
                        );
                        return Ok(());
                    }
                    Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
                }?;

                let dataset_stats_repo = transaction_catalog
                    .get_one::<dyn DatasetStatisticsRepository>()
                    .unwrap();

                self.update_dataset_statistics(dataset_stats_repo.as_ref(), target, updated_message)
                    .await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
