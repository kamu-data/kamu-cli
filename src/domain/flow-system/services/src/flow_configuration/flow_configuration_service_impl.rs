// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use dill::*;
use kamu_core::DatasetLifecycleMessage;
use kamu_flow_system::*;
use messaging_outbox::{MessageConsumer, MessageConsumerT};
use opendatafabric::DatasetID;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationServiceImpl {
    event_store: Arc<dyn FlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationService)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
impl FlowConfigurationServiceImpl {
    pub fn new(
        event_store: Arc<dyn FlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            event_store,
            time_source,
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
    #[tracing::instrument(level = "info", skip_all, fields(?flow_key))]
    async fn set_configuration(
        &self,
        flow_key: FlowKey,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError> {
        tracing::info!(
            flow_key = ?flow_key,
            rule = ?rule,
            "Setting flow configuration"
        );

        let maybe_flow_configuration =
            FlowConfiguration::try_load(flow_key.clone(), self.event_store.as_ref()).await?;

        let mut flow_configuration = match maybe_flow_configuration {
            // Modification
            Some(mut flow_configuration) => {
                flow_configuration
                    .modify_configuration(self.time_source.now(), rule)
                    .int_err()?;

                flow_configuration
            }
            // New configuration
            None => FlowConfiguration::new(self.time_source.now(), flow_key.clone(), rule),
        };

        flow_configuration
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(flow_configuration.into())
    }

    /// Lists all active configurations
    fn list_active_configurations(&self) -> FlowConfigurationStateStream {
        // Note: terribly inefficient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            for system_flow_type in SystemFlowType::all() {
                let flow_key = (*system_flow_type).into();
                let maybe_flow_configuration = FlowConfiguration::try_load(flow_key, self.event_store.as_ref()).await.int_err()?;

                if let Some(flow_configuration) = maybe_flow_configuration && flow_configuration.is_active() {
                    yield flow_configuration.into();
                }
            }

            let dataset_list_per_page = 10;
            let mut current_page = 0;
            let datasets_count = self.event_store.all_dataset_ids_count().await?;

            while datasets_count > current_page * dataset_list_per_page {
                let dataset_ids: Vec<_> = self.event_store.list_dataset_ids(&PaginationOpts {
                    limit: dataset_list_per_page,
                    offset: current_page * dataset_list_per_page,
                }).await?;

                for dataset_id in dataset_ids {
                    for dataset_flow_type in DatasetFlowType::all() {
                        let maybe_flow_configuration = FlowConfiguration::try_load(FlowKeyDataset::new(dataset_id.clone(), *dataset_flow_type).into(), self.event_store.as_ref()).await.int_err()?;
                        if let Some(flow_configuration) = maybe_flow_configuration && flow_configuration.is_active() {
                            yield flow_configuration.into();
                        }
                    }
                }
                current_page += 1;
            }
        })
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
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

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
            | DatasetLifecycleMessage::DependenciesUpdated(_)
            | DatasetLifecycleMessage::Renamed(_) => {
                // no action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
