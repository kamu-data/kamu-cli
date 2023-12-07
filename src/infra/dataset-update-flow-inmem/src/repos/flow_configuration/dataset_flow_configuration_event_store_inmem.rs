// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use dill::*;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigurationEventStoreInMem {
    inner: EventStoreInMemory<DatasetFlowConfigurationState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<DatasetFlowConfigurationEvent>,
    dataset_ids: HashSet<DatasetID>,
}

impl EventStoreState<DatasetFlowConfigurationState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[DatasetFlowConfigurationEvent] {
        &self.events
    }

    fn add_event(&mut self, event: DatasetFlowConfigurationEvent) {
        self.events.push(event);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFlowConfigurationEventStore)]
#[scope(Singleton)]
impl DatasetFlowConfigurationEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<DatasetFlowConfigurationState> for DatasetFlowConfigurationEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events<'a>(
        &'a self,
        query: &DatasetFlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<'a, DatasetFlowConfigurationEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &DatasetFlowKey,
        events: Vec<DatasetFlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetFlowConfigurationEventStore for DatasetFlowConfigurationEventStoreInMem {
    fn list_all_dataset_ids<'a>(&'a self) -> DatasetIDStream<'a> {
        // TODO: re-consider performance impact
        Box::pin(tokio_stream::iter(
            self.inner.as_state().lock().unwrap().dataset_ids.clone(),
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
