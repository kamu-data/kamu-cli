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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowConfigurationEventStore {
    inner: InMemoryEventStore<FlowConfigurationState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowConfigurationEvent>,
    dataset_ids: Vec<odf::DatasetID>,
}

impl EventStoreState<FlowConfigurationState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowConfigurationEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowConfigurationEvent) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationEventStore)]
#[scope(Singleton)]
impl InMemoryFlowConfigurationEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for InMemoryFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, ?opts))]
    fn get_events(
        &self,
        query: &FlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<FlowConfigurationEvent> {
        self.inner.get_events(query, opts)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, num_events = events.len()))]
    async fn save_events(
        &self,
        query: &FlowKey,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowConfigurationEvent>,
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
impl FlowConfigurationEventStore for InMemoryFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn list_dataset_ids(
        &self,
        pagination: &PaginationOpts,
    ) -> Result<Vec<odf::DatasetID>, InternalError> {
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
