// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetExternallyChangedMessage,
    DatasetLifecycleMessage,
    GetIncrementError,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
};
use kamu_flow_system as fs;
use messaging_outbox::*;
use time_source::SystemTimeSource;

use crate::{FLOW_TYPE_DATASET_INGEST, MESSAGE_CONSUMER_KAMU_FLOW_DATASETS_EVENT_BRIDGE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[dill::interface(dyn MessageConsumerT<DatasetExternallyChangedMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_DATASETS_EVENT_BRIDGE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    ],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowDatasetsEventBridge {
    time_source: Arc<dyn SystemTimeSource>,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowDatasetsEventBridge {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for FlowDatasetsEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowDatasetsEventBridge[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        catalog: &dill::Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            // Dataset resource is removed,
            // so we need to wipe all the related data in the flow system
            DatasetLifecycleMessage::Deleted(deleted_message) => {
                let flow_scope = fs::FlowScope::for_dataset(deleted_message.dataset_id.clone());

                tracing::debug!(
                    ?flow_scope,
                    "Handling flow scope removal for deleted dataset"
                );
                let flow_scope_removal_handlers = catalog
                    .get::<dill::AllOf<dyn fs::FlowScopeRemovalHandler>>()
                    .unwrap();
                for handler in flow_scope_removal_handlers {
                    handler.handle_flow_scope_removal(&flow_scope).await?;
                }
            }

            DatasetLifecycleMessage::Created(_) | DatasetLifecycleMessage::Renamed(_) => {
                // No action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetExternallyChangedMessage> for FlowDatasetsEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowDatasetsEventBridge[DatasetExternallyChangedMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &DatasetExternallyChangedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset externally changed message");

        let dataset_increment_query_service = target_catalog
            .get_one::<dyn kamu_datasets::DatasetIncrementQueryService>()
            .unwrap();

        let (update_cause, dataset_id) = match message {
            DatasetExternallyChangedMessage::HttpIngest(ingest_message) => {
                let increment = dataset_increment_query_service
                    .get_increment_between(
                        &ingest_message.dataset_id,
                        ingest_message.maybe_prev_block_hash.as_ref(),
                        &ingest_message.new_block_hash,
                    )
                    .await
                    .int_err()?;
                (
                    fs::FlowActivationCauseDatasetUpdate {
                        activation_time: self.time_source.now(),
                        dataset_id: ingest_message.dataset_id.clone(),
                        source: fs::DatasetUpdateSource::HttpIngest {
                            source_name: ingest_message.maybe_source_name.clone(),
                        },
                        new_head: ingest_message.new_block_hash.clone(),
                        old_head_maybe: ingest_message.maybe_prev_block_hash.clone(),
                        changes: fs::DatasetChanges::NewData {
                            blocks_added: increment.num_blocks,
                            records_added: increment.num_records,
                            new_watermark: increment.updated_watermark,
                        },
                    },
                    &ingest_message.dataset_id,
                )
            }
            DatasetExternallyChangedMessage::SmartTransferProtocolSync(sync_message) => {
                match dataset_increment_query_service
                    .get_increment_between(
                        &sync_message.dataset_id,
                        sync_message.maybe_prev_block_hash.as_ref(),
                        &sync_message.new_block_hash,
                    )
                    .await
                {
                    Ok(increment) => (
                        fs::FlowActivationCauseDatasetUpdate {
                            activation_time: self.time_source.now(),
                            dataset_id: sync_message.dataset_id.clone(),
                            source: fs::DatasetUpdateSource::SmartProtocolPush {
                                account_name: sync_message.account_name.clone(),
                                is_force: sync_message.is_force,
                            },
                            new_head: sync_message.new_block_hash.clone(),
                            old_head_maybe: sync_message.maybe_prev_block_hash.clone(),
                            changes: fs::DatasetChanges::NewData {
                                blocks_added: increment.num_blocks,
                                records_added: increment.num_records,
                                new_watermark: increment.updated_watermark,
                            },
                        },
                        &sync_message.dataset_id,
                    ),
                    Err(GetIncrementError::InvalidInterval(e)) => {
                        tracing::warn!(
                            ?e,
                            "Invalid interval for dataset increment query, breaking change"
                        );
                        (
                            fs::FlowActivationCauseDatasetUpdate {
                                activation_time: self.time_source.now(),
                                dataset_id: sync_message.dataset_id.clone(),
                                source: fs::DatasetUpdateSource::SmartProtocolPush {
                                    account_name: sync_message.account_name.clone(),
                                    is_force: sync_message.is_force,
                                },
                                new_head: sync_message.new_block_hash.clone(),
                                old_head_maybe: sync_message.maybe_prev_block_hash.clone(),
                                changes: fs::DatasetChanges::Breaking,
                            },
                            &sync_message.dataset_id,
                        )
                    }
                    Err(e) => {
                        tracing::error!(
                            ?e,
                            "Failed to get dataset increment for SmartTransferProtocolSync message"
                        );
                        return Err(e.int_err());
                    }
                }
            }
        };

        if update_cause.changes.is_empty() {
            tracing::debug!(
                %dataset_id,
                "No new blocks added, skipping flow activation",
            );
            return Ok(());
        }

        let flow_binding =
            fs::FlowBinding::for_dataset(dataset_id.clone(), FLOW_TYPE_DATASET_INGEST);

        self.flow_sensor_dispatcher
            .dispatch_input_flow_success(
                target_catalog,
                &flow_binding,
                fs::FlowActivationCause::DatasetUpdate(update_cause),
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
