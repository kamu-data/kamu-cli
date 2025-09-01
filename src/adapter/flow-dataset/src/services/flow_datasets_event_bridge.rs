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
    DatasetDependenciesMessage,
    DatasetExternallyChangedMessage,
    DatasetLifecycleMessage,
    GetIncrementError,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
};
use kamu_flow_system as fs;
use messaging_outbox::*;
use time_source::SystemTimeSource;

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FlowScopeDataset,
    MESSAGE_CONSUMER_KAMU_FLOW_DATASETS_EVENT_BRIDGE,
    ingest_dataset_binding,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[dill::interface(dyn MessageConsumerT<DatasetDependenciesMessage>)]
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
                let flow_scope = FlowScopeDataset::make_scope(&deleted_message.dataset_id);

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
impl MessageConsumerT<DatasetDependenciesMessage> for FlowDatasetsEventBridge {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowDatasetsEventBridge[DatasetDependenciesMessage]"
    )]
    async fn consume_message(
        &self,
        catalog: &dill::Catalog,
        message: &DatasetDependenciesMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset dependencies message");

        match message {
            DatasetDependenciesMessage::Updated(updated_message) => {
                let flow_scope = FlowScopeDataset::make_scope(&updated_message.dataset_id);
                self.flow_sensor_dispatcher
                    .refresh_sensor_dependencies(&flow_scope, catalog)
                    .await
                    .int_err()?;
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
                    fs::FlowActivationCauseResourceUpdate {
                        activation_time: self.time_source.now(),
                        changes: fs::ResourceChanges::NewData(fs::ResourceDataChanges {
                            blocks_added: increment.num_blocks,
                            records_added: increment.num_records,
                            new_watermark: increment.updated_watermark,
                        }),
                        resource_type: DATASET_RESOURCE_TYPE.to_string(),
                        details: serde_json::to_value(DatasetResourceUpdateDetails {
                            dataset_id: ingest_message.dataset_id.clone(),
                            source: DatasetUpdateSource::HttpIngest {
                                source_name: ingest_message.maybe_source_name.clone(),
                            },
                            new_head: ingest_message.new_block_hash.clone(),
                            old_head_maybe: ingest_message.maybe_prev_block_hash.clone(),
                        })
                        .int_err()?,
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
                        fs::FlowActivationCauseResourceUpdate {
                            activation_time: self.time_source.now(),
                            changes: fs::ResourceChanges::NewData(fs::ResourceDataChanges {
                                blocks_added: increment.num_blocks,
                                records_added: increment.num_records,
                                new_watermark: increment.updated_watermark,
                            }),
                            resource_type: DATASET_RESOURCE_TYPE.to_string(),
                            details: serde_json::to_value(DatasetResourceUpdateDetails {
                                dataset_id: sync_message.dataset_id.clone(),
                                source: DatasetUpdateSource::SmartProtocolPush {
                                    account_name: sync_message.account_name.clone(),
                                    is_force: sync_message.is_force,
                                },
                                new_head: sync_message.new_block_hash.clone(),
                                old_head_maybe: sync_message.maybe_prev_block_hash.clone(),
                            })
                            .int_err()?,
                        },
                        &sync_message.dataset_id,
                    ),
                    Err(GetIncrementError::InvalidInterval(e)) => {
                        tracing::warn!(
                            ?e,
                            "Invalid interval for dataset increment query, breaking change"
                        );
                        (
                            fs::FlowActivationCauseResourceUpdate {
                                activation_time: self.time_source.now(),
                                changes: fs::ResourceChanges::Breaking,
                                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                                details: serde_json::to_value(DatasetResourceUpdateDetails {
                                    dataset_id: sync_message.dataset_id.clone(),
                                    source: DatasetUpdateSource::SmartProtocolPush {
                                        account_name: sync_message.account_name.clone(),
                                        is_force: sync_message.is_force,
                                    },
                                    new_head: sync_message.new_block_hash.clone(),
                                    old_head_maybe: sync_message.maybe_prev_block_hash.clone(),
                                })
                                .int_err()?,
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

        let flow_binding = ingest_dataset_binding(dataset_id);

        self.flow_sensor_dispatcher
            .dispatch_input_flow_success(
                target_catalog,
                &flow_binding,
                fs::FlowActivationCause::ResourceUpdate(update_cause),
            )
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
