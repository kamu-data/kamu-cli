// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use event_bus::{AsyncEventHandler, EventBus};
use futures::StreamExt;
use kamu_core::events::DatasetEventDeleted;
use kamu_core::SystemTimeSource;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigurationServiceInMemory {
    event_store: Arc<dyn DatasetFlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    event_bus: Arc<EventBus>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFlowConfigurationService)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[scope(Singleton)]
impl DatasetFlowConfigurationServiceInMemory {
    pub fn new(
        event_store: Arc<dyn DatasetFlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            event_bus,
        }
    }

    async fn publish_dataset_flow_configuration_modified(
        &self,
        state: &DatasetFlowConfigurationState,
    ) -> Result<(), InternalError> {
        let event = FlowConfigurationEventModified::<DatasetFlowKey> {
            event_time: self.time_source.now(),
            flow_key: DatasetFlowKey::new(
                state.flow_key.dataset_id.clone(),
                state.flow_key.flow_type,
            ),
            paused: state.is_active(),
            rule: state.rule.clone(),
        };
        self.event_bus.dispatch_event(event).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetFlowConfigurationService for DatasetFlowConfigurationServiceInMemory {
    /// Lists update configurations, which are currently enabled
    fn list_enabled_configurations(
        &self,
        flow_type: DatasetFlowType,
    ) -> DatasetFlowConfigurationStateStream {
        // Note: terribly ineffecient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            let dataset_ids: Vec<_> = self.event_store.list_all_dataset_ids().collect().await;
            for dataset_id in dataset_ids {
                let maybe_update_configuration = DatasetFlowConfiguration::try_load(DatasetFlowKey::new(dataset_id, flow_type), self.event_store.as_ref()).await.int_err()?;
                if let Some(update_configuration) = maybe_update_configuration && update_configuration.is_active() {
                    yield update_configuration.into();
                }
            }
        })
    }

    /// Find current schedule, which may or may not be associated with the given
    /// dataset
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    async fn find_configuration(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<DatasetFlowConfigurationState>, FindDatasetFlowConfigurationError> {
        let maybe_update_configuration = DatasetFlowConfiguration::try_load(
            DatasetFlowKey::new(dataset_id.clone(), flow_type),
            self.event_store.as_ref(),
        )
        .await?;
        Ok(maybe_update_configuration.map(|us| us.into()))
    }

    /// Set or modify dataset update schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn set_configuration(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<DatasetFlowConfigurationState, SetDatasetFlowConfigurationError> {
        let maybe_update_configuration = DatasetFlowConfiguration::try_load(
            DatasetFlowKey::new(dataset_id.clone(), flow_type),
            self.event_store.as_ref(),
        )
        .await?;

        match maybe_update_configuration {
            // Modification
            Some(mut update_configuration) => {
                update_configuration
                    .modify_configuration(self.time_source.now(), paused, rule)
                    .int_err()?;

                update_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_dataset_flow_configuration_modified(&update_configuration)
                    .await?;

                Ok(update_configuration.into())
            }
            // New configuration
            None => {
                let mut update_configuration = DatasetFlowConfiguration::new(
                    self.time_source.now(),
                    DatasetFlowKey::new(dataset_id, flow_type),
                    paused,
                    rule,
                );

                update_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_dataset_flow_configuration_modified(&update_configuration)
                    .await?;

                Ok(update_configuration.into())
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for DatasetFlowConfigurationServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        for flow_type in DatasetFlowType::iterator() {
            let maybe_update_configuration = DatasetFlowConfiguration::try_load(
                DatasetFlowKey::new(event.dataset_id.clone(), flow_type),
                self.event_store.as_ref(),
            )
            .await
            .int_err()?;

            if let Some(mut update_configuration) = maybe_update_configuration {
                update_configuration
                    .notify_dataset_removed(self.time_source.now())
                    .int_err()?;

                update_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;
            }
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
