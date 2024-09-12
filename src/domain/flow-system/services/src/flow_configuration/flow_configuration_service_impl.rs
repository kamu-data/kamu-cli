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
use futures::TryStreamExt;
use kamu_core::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE};
use kamu_flow_system::*;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionDurability,
    Outbox,
    OutboxExt,
};
use opendatafabric::DatasetID;
use time_source::SystemTimeSource;

use crate::{
    MESSAGE_CONSUMER_KAMU_FLOW_CONFIGURATION_SERVICE,
    MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationServiceImpl {
    event_store: Arc<dyn FlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationService)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_FLOW_CONFIGURATION_SERVICE,
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE],
    durability: MessageConsumptionDurability::Durable,
})]
impl FlowConfigurationServiceImpl {
    pub fn new(
        event_store: Arc<dyn FlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            outbox,
        }
    }

    async fn publish_flow_configuration_modified(
        &self,
        state: &FlowConfigurationState,
        request_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let message = FlowConfigurationUpdatedMessage {
            event_time: request_time,
            flow_key: state.flow_key.clone(),
            paused: !state.is_active(),
            rule: state.rule.clone(),
        };

        self.outbox
            .post_message(MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE, message)
            .await
    }

    fn get_dataset_flow_keys(
        dataset_id: &DatasetID,
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
impl FlowConfigurationService for FlowConfigurationServiceImpl {
    /// Find the current schedule, which may or may not be associated with the
    /// given dataset
    #[tracing::instrument(level = "info", skip_all, fields(?flow_key))]
    async fn find_configuration(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError> {
        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_key, self.event_store.as_ref()).await?;
        Ok(maybe_flow_configuration.map(Into::into))
    }

    /// Find all configurations by datasets
    #[tracing::instrument(level = "info", skip_all, fields(?dataset_ids))]
    async fn find_configurations_by_datasets(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> FlowConfigurationStateStream {
        Box::pin(async_stream::try_stream! {
            for dataset_flow_type in DatasetFlowType::all() {
                for dataset_id in &dataset_ids {
                    let maybe_flow_configuration =
                        FlowConfiguration::try_load(
                            FlowKeyDataset::new(dataset_id.clone(), *dataset_flow_type).into(), self.event_store.as_ref()
                        )
                        .await
                        .int_err()?;
                    if let Some(flow_configuration) = maybe_flow_configuration {
                        yield flow_configuration.into();
                    }
                }
            }
        })
    }

    /// Set or modify dataset update schedule
    #[tracing::instrument(level = "info", skip_all, fields(?flow_key, %paused))]
    async fn set_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError> {
        tracing::info!(flow_key=?flow_key, paused=%paused, rule=?rule, "Setting flow configuration");

        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_key.clone(), self.event_store.as_ref()).await?;

        let mut flow_configuration = match maybe_flow_configuration {
            // Modification
            Some(mut flow_configuration) => {
                flow_configuration
                    .modify_configuration(self.time_source.now(), paused, rule)
                    .int_err()?;

                flow_configuration
            }
            // New configuration
            None => FlowConfiguration::new(self.time_source.now(), flow_key.clone(), paused, rule),
        };

        flow_configuration
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        self.publish_flow_configuration_modified(&flow_configuration, request_time)
            .await?;

        Ok(flow_configuration.into())
    }

    /// Lists all enabled configurations
    fn list_enabled_configurations(&self) -> FlowConfigurationStateStream {
        // Note: terribly inefficient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            for system_flow_type in SystemFlowType::all() {
                let flow_key = (*system_flow_type).into();
                let maybe_flow_configuration = FlowConfiguration::try_load(flow_key, self.event_store.as_ref()).await.int_err()?;

                if let Some(flow_configuration) = maybe_flow_configuration && flow_configuration.is_active() {
                    yield flow_configuration.into();
                }
            }

            let dataset_ids: Vec<_> = self.event_store.list_all_dataset_ids().try_collect().await?;

            for dataset_id in dataset_ids {
                for dataset_flow_type in DatasetFlowType::all() {
                    let maybe_flow_configuration = FlowConfiguration::try_load(FlowKeyDataset::new(dataset_id.clone(), *dataset_flow_type).into(), self.event_store.as_ref()).await.int_err()?;
                    if let Some(flow_configuration) = maybe_flow_configuration && flow_configuration.is_active() {
                        yield flow_configuration.into();
                    }
                }
            }
        })
    }

    /// Pauses particular flow configuration
    async fn pause_flow_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_key.clone(), self.event_store.as_ref())
                .await
                .int_err()?;

        if let Some(mut flow_configuration) = maybe_flow_configuration {
            flow_configuration.pause(request_time).int_err()?;
            flow_configuration
                .save(self.event_store.as_ref())
                .await
                .int_err()?;

            self.publish_flow_configuration_modified(&flow_configuration, request_time)
                .await?;
        }

        Ok(())
    }

    /// Resumes particular flow configuration
    async fn resume_flow_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError> {
        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_key.clone(), self.event_store.as_ref())
                .await
                .int_err()?;

        if let Some(mut flow_configuration) = maybe_flow_configuration {
            flow_configuration.resume(request_time).int_err()?;
            flow_configuration
                .save(self.event_store.as_ref())
                .await
                .int_err()?;

            self.publish_flow_configuration_modified(&flow_configuration, request_time)
                .await?;
        }

        Ok(())
    }

    /// Pauses dataset flows of a given type for given dataset.
    /// If the type is omitted, all possible dataset flow types are paused
    async fn pause_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_dataset_flow_keys(dataset_id, maybe_dataset_flow_type);

        for flow_key in flow_keys {
            self.pause_flow_configuration(request_time, flow_key)
                .await?;
        }

        Ok(())
    }

    /// Pauses system flows of a given type.
    /// If the type is omitted, all possible system flow types are paused
    async fn pause_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_system_flow_keys(maybe_system_flow_type);

        for flow_key in flow_keys {
            self.pause_flow_configuration(request_time, flow_key)
                .await?;
        }

        Ok(())
    }

    /// Resumes dataset flows of a given type for given dataset.
    /// If the type is omitted, all possible types are resumed (where
    /// configured)
    async fn resume_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_dataset_flow_keys(dataset_id, maybe_dataset_flow_type);

        for flow_key in flow_keys {
            self.resume_flow_configuration(request_time, flow_key)
                .await?;
        }

        Ok(())
    }

    /// Resumes system flows of a given type.
    /// If the type is omitted, all possible system flow types are resumed
    /// (where configured)
    async fn resume_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError> {
        let flow_keys = Self::get_system_flow_keys(maybe_system_flow_type);

        for flow_key in flow_keys {
            self.resume_flow_configuration(request_time, flow_key)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for FlowConfigurationServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for FlowConfigurationServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowConfigurationServiceImpl[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(message=?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Deleted(message) => {
                for flow_type in DatasetFlowType::all() {
                    let maybe_flow_configuration = FlowConfiguration::try_load(
                        FlowKeyDataset::new(message.dataset_id.clone(), *flow_type).into(),
                        self.event_store.as_ref(),
                    )
                    .await
                    .int_err()?;

                    if let Some(mut flow_configuration) = maybe_flow_configuration {
                        flow_configuration
                            .notify_dataset_removed(self.time_source.now())
                            .int_err()?;

                        flow_configuration
                            .save(self.event_store.as_ref())
                            .await
                            .int_err()?;
                    }
                }
            }

            DatasetLifecycleMessage::Created(_)
            | DatasetLifecycleMessage::DependenciesUpdated(_) => {
                // no action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
