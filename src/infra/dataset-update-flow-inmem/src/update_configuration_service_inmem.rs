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

pub struct UpdateConfigurationServiceInMemory {
    event_store: Arc<dyn UpdateConfigurationEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    event_bus: Arc<EventBus>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UpdateConfigurationService)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[scope(Singleton)]
impl UpdateConfigurationServiceInMemory {
    pub fn new(
        event_store: Arc<dyn UpdateConfigurationEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            event_bus,
        }
    }

    async fn publish_update_configuration_modified(
        &self,
        update_configuration_state: &UpdateConfigurationState,
    ) -> Result<(), InternalError> {
        let event = UpdateConfigurationEventModified {
            event_time: self.time_source.now(),
            dataset_id: update_configuration_state.dataset_id.clone(),
            paused: update_configuration_state.is_active(),
            schedule: update_configuration_state.schedule.clone(),
        };
        self.event_bus.dispatch_event(event).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateConfigurationService for UpdateConfigurationServiceInMemory {
    /// Lists proactive update schedules, which are currently enabled
    fn list_enabled_proactive_configurations(&self) -> UpdateConfigurationStateStream {
        // Note: terribly ineffecient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            let dataset_ids: Vec<_> = self.event_store.list_all_dataset_ids().collect().await;
            for dataset_id in dataset_ids {
                let update_configuration = UpdateConfiguration::load(dataset_id, self.event_store.as_ref()).await.int_err()?;
                if update_configuration.schedule.is_proactive() && update_configuration.is_active() {
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
    ) -> Result<Option<UpdateConfigurationState>, FindConfigurationError> {
        let maybe_update_configuration =
            UpdateConfiguration::try_load(dataset_id.clone(), self.event_store.as_ref()).await?;
        Ok(maybe_update_configuration.map(|us| us.into()))
    }

    /// Set or modify dataset update schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn set_configuration(
        &self,
        dataset_id: DatasetID,
        paused: bool,
        schedule: Schedule,
    ) -> Result<UpdateConfigurationState, SetConfigurationError> {
        let maybe_update_configuration =
            UpdateConfiguration::try_load(dataset_id.clone(), self.event_store.as_ref()).await?;

        match maybe_update_configuration {
            // Modification
            Some(mut update_configuration) => {
                update_configuration
                    .modify_configuration(self.time_source.now(), paused, schedule)
                    .int_err()?;

                update_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_update_configuration_modified(&update_configuration)
                    .await?;

                Ok(update_configuration.into())
            }
            // New configuration
            None => {
                let mut update_configuration =
                    UpdateConfiguration::new(self.time_source.now(), dataset_id, paused, schedule);

                update_configuration
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_update_configuration_modified(&update_configuration)
                    .await?;

                Ok(update_configuration.into())
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for UpdateConfigurationServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut update_configuration =
            UpdateConfiguration::load(event.dataset_id.clone(), self.event_store.as_ref())
                .await
                .int_err()?;

        update_configuration
            .notify_dataset_removed(self.time_source.now())
            .int_err()?;

        update_configuration
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
