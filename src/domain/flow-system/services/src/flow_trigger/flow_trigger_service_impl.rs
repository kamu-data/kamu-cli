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
use database_common::PaginationOpts;
use dill::*;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_flow_system::{FlowTriggerEventStore, *};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
    Outbox,
    OutboxExt,
};
use time_source::SystemTimeSource;

use crate::{
    MESSAGE_CONSUMER_KAMU_FLOW_TRIGGER_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowTriggerServiceImpl {
    event_store: Arc<dyn FlowTriggerEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowTriggerService)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_TRIGGER_SERVICE,
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_DATASET_SERVICE],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
impl FlowTriggerServiceImpl {
    pub fn new(
        event_store: Arc<dyn FlowTriggerEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            outbox,
        }
    }

    async fn publish_flow_trigger_modified(
        &self,
        state: &FlowTriggerState,
        request_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let message = FlowTriggerUpdatedMessage {
            event_time: request_time,
            flow_key: state.flow_key.clone(),
            rule: state.rule.clone(),
            paused: !state.is_active(),
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE, message)
            .await
    }

    fn get_dataset_flow_keys(
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Vec<FlowKey> {
        if let Some(dataset_flow_type) = maybe_dataset_flow_type {
            vec![FlowKey::Dataset(FlowKeyDataset {
                dataset_id: dataset_id.clone(),
                flow_type: dataset_flow_type,
            })]
        } else {
            DatasetFlowType::all()
                .iter()
                .map(|dft| {
                    FlowKey::Dataset(FlowKeyDataset {
                        dataset_id: dataset_id.clone(),
                        flow_type: *dft,
                    })
                })
                .collect()
        }
    }

    fn get_system_flow_keys(maybe_system_flow_type: Option<SystemFlowType>) -> Vec<FlowKey> {
        if let Some(system_flow_type) = maybe_system_flow_type {
            vec![FlowKey::System(FlowKeySystem {
                flow_type: system_flow_type,
            })]
        } else {
            SystemFlowType::all()
                .iter()
                .map(|sft| FlowKey::System(FlowKeySystem { flow_type: *sft }))
                .collect()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerService for FlowTriggerServiceImpl {
    /// Find current trigger of a certain type
    async fn find_trigger(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowTriggerState>, FindFlowTriggerError> {
        let maybe_flow_trigger = FlowTrigger::try_load(flow_key, self.event_store.as_ref()).await?;
        Ok(maybe_flow_trigger.map(Into::into))
    }

    /// Set or modify flow trigger
    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowTriggerRule,
    ) -> Result<FlowTriggerState, SetFlowTriggerError> {
        tracing::info!(
            flow_key = ?flow_key,
            rule = ?rule,
            "Setting flow trigger"
        );

        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_key.clone(), self.event_store.as_ref()).await?;

        let mut flow_trigger = match maybe_flow_trigger {
            // Modification
            Some(mut flow_trigger) => {
                flow_trigger
                    .modify_rule(self.time_source.now(), paused, rule)
                    .int_err()?;

                flow_trigger
            }
            // New trigger
            None => FlowTrigger::new(self.time_source.now(), flow_key.clone(), paused, rule),
        };

        flow_trigger
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        self.publish_flow_trigger_modified(&flow_trigger, request_time)
            .await?;

        Ok(flow_trigger.into())
    }

    /// Lists all flow triggers, which are currently enabled
    fn list_enabled_triggers(&self) -> FlowTriggerStateStream {
        // Note: terribly inefficient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            for system_flow_type in SystemFlowType::all() {
                let flow_key = (*system_flow_type).into();
                let maybe_flow_trigger = FlowTrigger::try_load(flow_key, self.event_store.as_ref()).await.int_err()?;

                if let Some(flow_trigger) = maybe_flow_trigger && flow_trigger.is_active() {
                    yield flow_trigger.into();
                }
            }

            let dataset_list_per_page = 10;
            let mut current_page = 0;
            let datasets_count = self.event_store.all_dataset_ids_count().await?;

            while datasets_count > current_page * dataset_list_per_page {
                let dataset_ids: Vec<_> = self.event_store.list_dataset_ids(
                    &PaginationOpts::from_page(current_page, dataset_list_per_page)
                ).await?;

                for dataset_id in dataset_ids {
                    for dataset_flow_type in DatasetFlowType::all() {
                        let maybe_flow_trigger = FlowTrigger::try_load(FlowKeyDataset::new(dataset_id.clone(), *dataset_flow_type).into(), self.event_store.as_ref()).await.int_err()?;
                        if let Some(flow_trigger) = maybe_flow_trigger && flow_trigger.is_active() {
                            yield flow_trigger.into();
                        }
                    }
                }
                current_page += 1;
            }
        })
    }

    /// Pauses particular flow trigger
    async fn pause_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger = FlowTrigger::try_load(flow_key.clone(), self.event_store.as_ref())
            .await
            .int_err()?;

        if let Some(mut flow_trigger) = maybe_flow_trigger {
            flow_trigger.pause(request_time).int_err()?;
            flow_trigger
                .save(self.event_store.as_ref())
                .await
                .int_err()?;

            self.publish_flow_trigger_modified(&flow_trigger, request_time)
                .await?;
        }

        Ok(())
    }

    /// Resumes particular flow trigger
    async fn resume_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger = FlowTrigger::try_load(flow_key.clone(), self.event_store.as_ref())
            .await
            .int_err()?;

        if let Some(mut flow_trigger) = maybe_flow_trigger {
            flow_trigger.resume(request_time).int_err()?;
            flow_trigger
                .save(self.event_store.as_ref())
                .await
                .int_err()?;

            self.publish_flow_trigger_modified(&flow_trigger, request_time)
                .await?;
        }

        Ok(())
    }

    /// Pauses dataset flows of given type for given dataset.
    /// If type is omitted, all possible dataset flow types are paused
    async fn pause_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_dataset_flow_keys(dataset_id, maybe_dataset_flow_type);

        for flow_key in flow_keys {
            self.pause_flow_trigger(request_time, flow_key).await?;
        }

        Ok(())
    }

    /// Pauses system flows of given type.
    /// If type is omitted, all possible system flow types are paused
    async fn pause_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_system_flow_keys(maybe_system_flow_type);

        for flow_key in flow_keys {
            self.pause_flow_trigger(request_time, flow_key).await?;
        }

        Ok(())
    }

    /// Resumes dataset flows of given type for given dataset.
    /// If type is omitted, all possible types are resumed (where configured)
    async fn resume_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_dataset_flow_keys(dataset_id, maybe_dataset_flow_type);

        for flow_key in flow_keys {
            self.resume_flow_trigger(request_time, flow_key).await?;
        }

        Ok(())
    }

    /// Resumes system flows of given type.
    /// If type is omitted, all possible system flow types are resumed (where
    /// configured)
    async fn resume_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_system_flow_keys(maybe_system_flow_type);

        for flow_key in flow_keys {
            self.resume_flow_trigger(request_time, flow_key).await?;
        }

        Ok(())
    }

    /// Find all triggers by datasets
    #[tracing::instrument(level = "info", skip_all, fields(?dataset_ids))]
    async fn find_triggers_by_datasets(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> FlowTriggerStateStream {
        Box::pin(async_stream::try_stream! {
            for dataset_flow_type in DatasetFlowType::all() {
                for dataset_id in &dataset_ids {
                    let maybe_flow_trigger =
                        FlowTrigger::try_load(
                            FlowKeyDataset::new(dataset_id.clone(), *dataset_flow_type).into(), self.event_store.as_ref()
                        )
                        .await
                        .int_err()?;
                    if let Some(flow_trigger) = maybe_flow_trigger {
                        yield flow_trigger.into();
                    }
                }
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowTriggerServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for FlowTriggerServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowTriggerServiceImpl[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Deleted(message) => {
                for flow_type in DatasetFlowType::all() {
                    let maybe_flow_trigger = FlowTrigger::try_load(
                        FlowKeyDataset::new(message.dataset_id.clone(), *flow_type).into(),
                        self.event_store.as_ref(),
                    )
                    .await
                    .int_err()?;

                    if let Some(mut flow_trigger) = maybe_flow_trigger {
                        flow_trigger
                            .notify_dataset_removed(self.time_source.now())
                            .int_err()?;

                        flow_trigger
                            .save(self.event_store.as_ref())
                            .await
                            .int_err()?;
                    }
                }
            }

            DatasetLifecycleMessage::Created(_) | DatasetLifecycleMessage::Renamed(_) => {
                // no action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
