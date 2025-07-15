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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetExternallyChangedMessage,
    DatasetIncrementQueryService,
    DependencyGraphService,
    MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
};
use messaging_outbox::*;
use time_source::SystemTimeSource;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_INGEST,
    FlowConfigRuleIngest,
    MESSAGE_CONSUMER_KAMU_FLOW_DISPATCHER_INGEST,
    create_activation_cause_from_upstream_flow,
    trigger_transform_flow_for_all_downstream_datasets,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_type: FLOW_TYPE_DATASET_INGEST,
})]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<DatasetExternallyChangedMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_DISPATCHER_INGEST,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_HTTP_ADAPTER,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct FlowDispatcherIngest {
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
    flow_run_service: Arc<dyn fs::FlowRunService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_increment_query_service: Arc<dyn DatasetIncrementQueryService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherIngest {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

        let mut fetch_uncacheable = false;
        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleIngest::TYPE_ID
        {
            let ingest_rule = FlowConfigRuleIngest::from_flow_config(config_snapshot)?;
            fetch_uncacheable = ingest_rule.fetch_uncacheable;
        }

        Ok(ats::LogicalPlanDatasetUpdate {
            dataset_id: dataset_id.clone(),
            fetch_uncacheable,
        }
        .into_logical_plan())
    }

    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let maybe_activation_cause = create_activation_cause_from_upstream_flow(
            self.dataset_increment_query_service.as_ref(),
            success_flow_state,
            task_result,
            finish_time,
        )
        .await?;

        if let Some(activation_cause) = maybe_activation_cause {
            trigger_transform_flow_for_all_downstream_datasets(
                self.dependency_graph_service.as_ref(),
                self.flow_trigger_service.as_ref(),
                self.flow_run_service.as_ref(),
                &success_flow_state.flow_binding,
                activation_cause,
            )
            .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowDispatcherIngest {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetExternallyChangedMessage> for FlowDispatcherIngest {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowDispatcherIngest[DatasetExternallyChangedMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &dill::Catalog,
        message: &DatasetExternallyChangedMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset externally changed message");

        let time_source = target_catalog.get_one::<dyn SystemTimeSource>().unwrap();
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
                        activation_time: time_source.now(),
                        dataset_id: ingest_message.dataset_id.clone(),
                        source: fs::DatasetUpdateSource::HttpIngest {
                            source_name: ingest_message.maybe_source_name.clone(),
                        },
                        new_head: ingest_message.new_block_hash.clone(),
                        old_head_maybe: ingest_message.maybe_prev_block_hash.clone(),
                        blocks_added: increment.num_blocks,
                        records_added: increment.num_records,
                        was_compacted: false,
                        new_watermark: increment.updated_watermark,
                    },
                    &ingest_message.dataset_id,
                )
            }
            DatasetExternallyChangedMessage::SmartTransferProtocolSync(sync_message) => {
                // TODO: check what happens when the sync is forced. It might fail
                let increment = dataset_increment_query_service
                    .get_increment_between(
                        &sync_message.dataset_id,
                        sync_message.maybe_prev_block_hash.as_ref(),
                        &sync_message.new_block_hash,
                    )
                    .await
                    .int_err()?;
                (
                    fs::FlowActivationCauseDatasetUpdate {
                        activation_time: time_source.now(),
                        dataset_id: sync_message.dataset_id.clone(),
                        source: fs::DatasetUpdateSource::SmartProtocolPush {
                            account_name: sync_message.account_name.clone(),
                            is_force: sync_message.is_force,
                        },
                        new_head: sync_message.new_block_hash.clone(),
                        old_head_maybe: sync_message.maybe_prev_block_hash.clone(),
                        blocks_added: increment.num_blocks,
                        records_added: increment.num_records,
                        was_compacted: false,
                        new_watermark: increment.updated_watermark,
                    },
                    &sync_message.dataset_id,
                )
            }
        };

        if update_cause.blocks_added == 0 {
            tracing::debug!(
                %dataset_id,
                "No new blocks added, skipping flow activation",
            );
            return Ok(());
        }

        let flow_binding =
            fs::FlowBinding::for_dataset(dataset_id.clone(), FLOW_TYPE_DATASET_INGEST);

        trigger_transform_flow_for_all_downstream_datasets(
            self.dependency_graph_service.as_ref(),
            self.flow_trigger_service.as_ref(),
            self.flow_run_service.as_ref(),
            &flow_binding,
            fs::FlowActivationCause::DatasetUpdate(update_cause),
        )
        .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
