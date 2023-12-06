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
use event_bus::EventBus;
use kamu_core::SystemTimeSource;
use kamu_dataset_update_flow::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemFlowConfigurationServiceInMemory {
    event_store: Arc<dyn SystemFlowConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    event_bus: Arc<EventBus>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SystemFlowConfigurationService)]
#[scope(Singleton)]
impl SystemFlowConfigurationServiceInMemory {
    pub fn new(
        event_store: Arc<dyn SystemFlowConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            event_bus,
        }
    }

    async fn publish_system_flow_configuration_modified(
        &self,
        state: &SystemFlowConfigurationState,
    ) -> Result<(), InternalError> {
        let event = FlowConfigurationEventModified::<SystemFlowKey> {
            event_time: self.time_source.now(),
            flow_key: SystemFlowKey::new(state.flow_key.flow_type),
            paused: state.is_active(),
            rule: state.rule.clone(),
        };
        self.event_bus.dispatch_event(event).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SystemFlowConfigurationService for SystemFlowConfigurationServiceInMemory {
    /// Find current schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn find_configuration(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<SystemFlowConfigurationState>, FindSystemFlowConfigurationError> {
        let maybe_update_configuration = SystemFlowConfiguration::try_load(
            SystemFlowKey::new(flow_type),
            self.event_store.as_ref(),
        )
        .await?;
        Ok(maybe_update_configuration.map(|us| us.into()))
    }

    /// Set or modify dataset update schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn set_configuration(
        &self,
        flow_type: SystemFlowType,
        paused: bool,
        schedule: Schedule,
    ) -> Result<SystemFlowConfigurationState, SetSystemFlowConfigurationError> {
        let maybe_flow_configuration = SystemFlowConfiguration::try_load(
            SystemFlowKey::new(flow_type),
            self.event_store.as_ref(),
        )
        .await?;

        match maybe_flow_configuration {
            // Modification
            Some(mut flow_configuration) => {
                flow_configuration
                    .modify_configuration(self.time_source.now(), paused, schedule)
                    .int_err()?;

                flow_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_system_flow_configuration_modified(&flow_configuration)
                    .await?;

                Ok(flow_configuration.into())
            }
            // New configuration
            None => {
                let mut flow_configuration = SystemFlowConfiguration::new(
                    self.time_source.now(),
                    flow_type,
                    paused,
                    schedule,
                );

                flow_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_system_flow_configuration_modified(&flow_configuration)
                    .await?;

                Ok(flow_configuration.into())
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
