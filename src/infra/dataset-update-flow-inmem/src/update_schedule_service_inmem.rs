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
use futures::StreamExt;
use kamu_core::SystemTimeSource;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateScheduleServiceInMemory {
    event_store: Arc<dyn UpdateScheduleEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
impl UpdateScheduleServiceInMemory {
    pub fn new(
        event_store: Arc<dyn UpdateScheduleEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            event_store,
            time_source,
        }
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
                if !update_schedule.paused {
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
        dataset_id: DatasetID,
    ) -> Result<Option<UpdateScheduleState>, FindScheduleError> {
        let maybe_update_schedule =
            UpdateSchedule::try_load(dataset_id, self.event_store.as_ref()).await?;
        Ok(maybe_update_schedule.map(|us| us.into()))
    }

    /// Set dataset update schedule
    #[tracing::instrument(level = "info", skip_all)]
    async fn set_schedule(
        &self,
        dataset_id: DatasetID,
        schedule: Schedule,
    ) -> Result<UpdateScheduleState, SetScheduleError> {
        let mut update_schedule = UpdateSchedule::new(self.time_source.now(), dataset_id, schedule);
        update_schedule
            .save(self.event_store.as_ref())
            .await
            .int_err()?;

        Ok(update_schedule.into())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
