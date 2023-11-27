// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, scope, Singleton};
use event_bus::{AsyncEventHandler, EventBus};
use futures::StreamExt;
use kamu_core::events::DatasetDeleted;
use kamu_core::SystemTimeSource;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateScheduleServiceInMemory {
    event_store: Arc<dyn UpdateScheduleEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
    event_bus: Arc<EventBus>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
impl UpdateScheduleServiceInMemory {
    pub fn new(
        event_store: Arc<dyn UpdateScheduleEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            event_store,
            time_source,
            event_bus,
        }
    }

    async fn publish_update_schedule_modified(
        &self,
        update_schedule_state: &UpdateScheduleState,
    ) -> Result<(), InternalError> {
        let event = UpdateScheduleEventModified {
            event_time: self.time_source.now(),
            dataset_id: update_schedule_state.dataset_id.clone(),
            paused: update_schedule_state.paused(),
            schedule: update_schedule_state.schedule().clone(),
        };
        self.event_bus.dispatch_event(event).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateScheduleService for UpdateScheduleServiceInMemory {
    /// Lists update schedules, which are currently enabled
    fn list_enabled_schedules(&self) -> UpdateScheduleStateStream {
        // Note: terribly ineffecient - walks over events multiple times
        Box::pin(async_stream::try_stream! {
            let dataset_ids: Vec<_> = self.event_store.get_queries().collect().await;
            for dataset_id in dataset_ids {
                let update_schedule = UpdateSchedule::load(dataset_id, self.event_store.as_ref()).await.int_err()?;
                if !update_schedule.paused() {
                    yield update_schedule.into();
                }
            }
        })
    }

    /// Find current schedule, which may or may not be associated with the given
    /// dataset
    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    async fn find_schedule(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<Option<UpdateScheduleState>, FindScheduleError> {
        let maybe_update_schedule =
            UpdateSchedule::try_load(dataset_id.clone(), self.event_store.as_ref()).await?;
        Ok(maybe_update_schedule.map(|us| us.into()))
    }

    /// Set or modify dataset update schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn set_schedule(
        &self,
        dataset_id: DatasetID,
        schedule: Schedule,
    ) -> Result<UpdateScheduleState, SetScheduleError> {
        let maybe_update_schedule =
            UpdateSchedule::try_load(dataset_id.clone(), self.event_store.as_ref()).await?;

        match maybe_update_schedule {
            // Modification
            Some(mut update_schedule) => {
                update_schedule
                    .modify_schedule(self.time_source.now(), schedule)
                    .int_err()?;

                update_schedule
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_update_schedule_modified(&update_schedule)
                    .await?;

                Ok(update_schedule.into())
            }
            // New schedule
            None => {
                let mut update_schedule =
                    UpdateSchedule::new(self.time_source.now(), dataset_id, schedule);

                update_schedule
                    .save(self.event_store.as_ref())
                    .await
                    .int_err()?;

                self.publish_update_schedule_modified(&update_schedule)
                    .await?;

                Ok(update_schedule.into())
            }
        }
    }

    /// Pause dataset update schedule
    async fn pause_schedule(&self, dataset_id: &DatasetID) -> Result<(), PauseScheduleError> {
        let mut update_schedule =
            UpdateSchedule::load(dataset_id.clone(), self.event_store.as_ref()).await?;

        update_schedule.pause(self.time_source.now()).int_err()?;

        update_schedule
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        self.publish_update_schedule_modified(&update_schedule)
            .await?;

        Ok(())
    }

    /// Resume paused dataset update schedule
    async fn resume_schedule(&self, dataset_id: &DatasetID) -> Result<(), ResumeScheduleError> {
        let mut update_schedule =
            UpdateSchedule::load(dataset_id.clone(), self.event_store.as_ref()).await?;

        update_schedule.resume(self.time_source.now()).int_err()?;

        update_schedule
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        self.publish_update_schedule_modified(&update_schedule)
            .await?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetDeleted> for UpdateScheduleServiceInMemory {
    async fn handle(&self, event: DatasetDeleted) -> Result<(), InternalError> {
        let mut update_schedule = UpdateSchedule::load(event.dataset_id, self.event_store.as_ref())
            .await
            .int_err()?;

        update_schedule
            .notify_dataset_removed(self.time_source.now())
            .int_err()?;

        update_schedule
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
