// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use dill::*;
use kamu_flow_system::*;
use opendatafabric::DatasetID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowTriggerEventStore {
    inner: InMemoryEventStore<FlowTriggerState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowTriggerEvent>,
    dataset_ids: Vec<DatasetID>,
}

impl EventStoreState<FlowTriggerState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowTriggerEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowTriggerEvent) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowTriggerEventStore)]
#[scope(Singleton)]
impl InMemoryFlowTriggerEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowTriggerState> for InMemoryFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, ?opts))]
    fn get_events(&self, query: &FlowKey, opts: GetEventsOpts) -> EventStream<FlowTriggerEvent> {
        self.inner.get_events(query, opts)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, num_events = events.len()))]
    async fn save_events(
        &self,
        query: &FlowKey,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowTriggerEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        if let FlowKey::Dataset(flow_key) = query {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            g.dataset_ids.push(flow_key.dataset_id.clone());
        }

        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerEventStore for InMemoryFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_dataset_ids(
        &self,
        pagination: &PaginationOpts,
    ) -> Result<Vec<DatasetID>, InternalError> {
        Ok(self
            .inner
            .as_state()
            .lock()
            .unwrap()
            .dataset_ids
            .iter()
            .skip(pagination.offset)
            .take(pagination.limit)
            .cloned()
            .collect())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn all_dataset_ids_count(&self) -> Result<usize, InternalError> {
        Ok(self.inner.as_state().lock().unwrap().dataset_ids.len())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
