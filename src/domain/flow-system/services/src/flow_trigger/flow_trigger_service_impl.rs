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
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
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
            flow_binding: state.flow_binding.clone(),
            rule: state.rule.clone(),
            paused: !state.is_active(),
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE, message)
            .await
    }

    async fn get_dataset_flow_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<&str>,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        if let Some(dataset_flow_type) = maybe_dataset_flow_type {
            Ok(vec![FlowBinding::for_dataset(
                dataset_id.clone(),
                dataset_flow_type,
            )])
        } else {
            self.event_store
                .all_trigger_bindings_for_dataset_flows(dataset_id)
                .await
        }
    }

    async fn get_system_flow_bindings(
        &self,
        maybe_system_flow_type: Option<&str>,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        if let Some(system_flow_type) = maybe_system_flow_type {
            Ok(vec![FlowBinding::for_system(system_flow_type)])
        } else {
            self.event_store
                .all_trigger_bindings_for_system_flows()
                .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerService for FlowTriggerServiceImpl {
    /// Find current trigger of a certain type
    async fn find_trigger(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowTriggerState>, FindFlowTriggerError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding.clone(), self.event_store.as_ref()).await?;
        Ok(maybe_flow_trigger.map(Into::into))
    }

    /// Set or modify flow trigger
    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        paused: bool,
        rule: FlowTriggerRule,
    ) -> Result<FlowTriggerState, SetFlowTriggerError> {
        tracing::info!(
            flow_binding = ?flow_binding,
            rule = ?rule,
            "Setting flow trigger"
        );

        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding.clone(), self.event_store.as_ref()).await?;

        let mut flow_trigger = match maybe_flow_trigger {
            // Modification
            Some(mut flow_trigger) => {
                flow_trigger
                    .modify_rule(self.time_source.now(), paused, rule)
                    .int_err()?;

                flow_trigger
            }
            // New trigger
            None => FlowTrigger::new(self.time_source.now(), flow_binding, paused, rule),
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
        Box::pin(async_stream::try_stream! {
            use futures::stream::TryStreamExt;
            let mut stream_flow_bindings = self.event_store.stream_all_active_flow_bindings();
            while let Some(flow_binding) = stream_flow_bindings.try_next().await? {
                let maybe_flow_trigger = FlowTrigger::try_load(flow_binding, self.event_store.as_ref()).await.int_err()?;
                if let Some(flow_trigger) = maybe_flow_trigger {
                    yield flow_trigger.into();
                }
            }
        })
    }

    /// Pauses particular flow trigger
    async fn pause_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding.clone(), self.event_store.as_ref())
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
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError> {
        let maybe_flow_trigger =
            FlowTrigger::try_load(flow_binding.clone(), self.event_store.as_ref())
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
        maybe_dataset_flow_type: Option<&str>,
    ) -> Result<(), InternalError> {
        let flow_bindings = self
            .get_dataset_flow_bindings(dataset_id, maybe_dataset_flow_type)
            .await?;

        for flow_binding in flow_bindings {
            self.pause_flow_trigger(request_time, &flow_binding).await?;
        }

        Ok(())
    }

    /// Pauses system flows of given type.
    /// If type is omitted, all possible system flow types are paused
    async fn pause_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<&str>,
    ) -> Result<(), InternalError> {
        let flow_bindings = self
            .get_system_flow_bindings(maybe_system_flow_type)
            .await?;

        for flow_binding in flow_bindings {
            self.pause_flow_trigger(request_time, &flow_binding).await?;
        }

        Ok(())
    }

    /// Resumes dataset flows of given type for given dataset.
    /// If type is omitted, all possible types are resumed (where configured)
    async fn resume_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<&str>,
    ) -> Result<(), InternalError> {
        let flow_bindings = self
            .get_dataset_flow_bindings(dataset_id, maybe_dataset_flow_type)
            .await?;

        for flow_binding in flow_bindings {
            self.resume_flow_trigger(request_time, &flow_binding)
                .await?;
        }

        Ok(())
    }

    /// Resumes system flows of given type.
    /// If type is omitted, all possible system flow types are resumed (where
    /// configured)
    async fn resume_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<&str>,
    ) -> Result<(), InternalError> {
        let flow_bindings = self
            .get_system_flow_bindings(maybe_system_flow_type)
            .await?;

        for flow_binding in flow_bindings {
            self.resume_flow_trigger(request_time, &flow_binding)
                .await?;
        }

        Ok(())
    }

    /// Find all triggers by datasets
    #[tracing::instrument(level = "info", skip_all, fields(?dataset_ids))]
    async fn has_active_triggers_for_datasets(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<bool, InternalError> {
        self.event_store
            .has_active_triggers_for_datasets(dataset_ids)
            .await
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
                let flow_bindings = self
                    .get_dataset_flow_bindings(&message.dataset_id, None)
                    .await
                    .int_err()?;

                for flow_binding in flow_bindings {
                    let maybe_flow_trigger =
                        FlowTrigger::try_load(flow_binding, self.event_store.as_ref())
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
