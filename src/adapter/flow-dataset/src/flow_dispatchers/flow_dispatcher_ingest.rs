// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetExternallyChangedMessage,
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
        flow_binding: &fs::FlowBinding,
        activation_cause: fs::FlowActivationCause,
        _: Option<fs::FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        trigger_transform_flow_for_all_downstream_datasets(
            self.dependency_graph_service.as_ref(),
            self.flow_trigger_service.as_ref(),
            self.flow_run_service.as_ref(),
            flow_binding,
            activation_cause,
        )
        .await
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

        let (activation_cause, dataset_id) = match message {
            DatasetExternallyChangedMessage::HttpIngest(update_message) => (
                fs::FlowActivationCause::Push(fs::FlowActivationCausePush {
                    activation_time: time_source.now(),
                    source_name: None,
                    dataset_id: update_message.dataset_id.clone(),
                    result: fs::DatasetPushResult::HttpIngest(fs::DatasetPushHttpIngestResult {
                        old_head_maybe: update_message.maybe_prev_block_hash.clone(),
                        new_head: update_message.new_block_hash.clone(),
                    }),
                }),
                &update_message.dataset_id,
            ),
            DatasetExternallyChangedMessage::SmartTransferProtocolSync(update_message) => (
                fs::FlowActivationCause::Push(fs::FlowActivationCausePush {
                    activation_time: time_source.now(),
                    source_name: None,
                    dataset_id: update_message.dataset_id.clone(),
                    result: fs::DatasetPushResult::SmtpSync(fs::DatasetPushSmtpSyncResult {
                        old_head_maybe: update_message.maybe_prev_block_hash.clone(),
                        new_head: update_message.new_block_hash.clone(),
                        account_name_maybe: update_message.account_name.clone(),
                        is_force: update_message.is_force,
                    }),
                }),
                &update_message.dataset_id,
            ),
        };

        let flow_binding =
            fs::FlowBinding::for_dataset(dataset_id.clone(), FLOW_TYPE_DATASET_INGEST);

        use fs::FlowDispatcher;
        self.propagate_success(&flow_binding, activation_cause, None)
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
