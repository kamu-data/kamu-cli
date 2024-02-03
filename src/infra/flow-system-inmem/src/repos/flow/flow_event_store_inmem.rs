// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use dill::*;
use kamu_flow_system::*;
use opendatafabric::{AccountName, DatasetID};

use crate::dataset_flow_key::BorrowedFlowKeyDataset;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowEventStoreInMem {
    inner: EventStoreInMemory<FlowState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowEvent>,
    typed_flows_by_dataset: HashMap<FlowKeyDataset, Vec<FlowID>>,
    all_flows_by_dataset: HashMap<DatasetID, Vec<FlowID>>,
    system_flows_by_type: HashMap<SystemFlowType, Vec<FlowID>>,
    all_system_flows: Vec<FlowID>,
    flow_search_index: HashMap<FlowID, FlowIndexEntry>,
    all_flows: Vec<FlowID>,
    last_flow_id: Option<FlowID>,
}

impl State {
    fn next_flow_id(&mut self) -> FlowID {
        let next_flow_id = if let Some(last_flow_id) = self.last_flow_id {
            let id: u64 = last_flow_id.into();
            FlowID::new(id + 1)
        } else {
            FlowID::new(0)
        };
        self.last_flow_id = Some(next_flow_id);
        next_flow_id
    }

    fn matches_dataset_flow(&self, flow_id: FlowID, filters: &DatasetFlowFilters) -> bool {
        if let Some(index_entry) = self.flow_search_index.get(&flow_id) {
            index_entry.matches_dataset_flow_filters(filters)
        } else {
            false
        }
    }

    fn matches_system_flow(&self, flow_id: FlowID, filters: &SystemFlowFilters) -> bool {
        if let Some(index_entry) = self.flow_search_index.get(&flow_id) {
            index_entry.matches_system_flow_filters(filters)
        } else {
            false
        }
    }
}

impl EventStoreState<FlowState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowEvent) {
        self.events.push(event);
    }
}

struct FlowIndexEntry {
    pub flow_type: AnyFlowType,
    pub flow_status: FlowStatus,
    pub initiator: Option<AccountName>,
}

enum AnyFlowType {
    Dataset(DatasetFlowType),
    System(SystemFlowType),
}

impl FlowIndexEntry {
    pub fn matches_dataset_flow_filters(&self, filters: &DatasetFlowFilters) -> bool {
        self.dataset_flow_type_matches(filters.by_flow_type)
            && self.flow_status_matches(filters.by_flow_status)
            && self.initiator_matches(filters.by_initiator.as_ref())
    }

    pub fn matches_system_flow_filters(&self, filters: &SystemFlowFilters) -> bool {
        self.system_flow_type_matches(filters.by_flow_type)
            && self.flow_status_matches(filters.by_flow_status)
            && self.initiator_matches(filters.by_initiator.as_ref())
    }

    fn dataset_flow_type_matches(
        &self,
        maybe_dataset_flow_type_filter: Option<DatasetFlowType>,
    ) -> bool {
        match self.flow_type {
            AnyFlowType::Dataset(dft) => match maybe_dataset_flow_type_filter {
                Some(flow_type_filter) => flow_type_filter == dft,
                None => true,
            },
            AnyFlowType::System(_) => false,
        }
    }

    fn system_flow_type_matches(
        &self,
        maybe_system_flow_type_filter: Option<SystemFlowType>,
    ) -> bool {
        match self.flow_type {
            AnyFlowType::Dataset(_) => false,
            AnyFlowType::System(sft) => match maybe_system_flow_type_filter {
                Some(flow_type_filter) => flow_type_filter == sft,
                None => true,
            },
        }
    }

    fn flow_status_matches(&self, maybe_flow_status_filter: Option<FlowStatus>) -> bool {
        if let Some(flow_status_filter) = maybe_flow_status_filter {
            flow_status_filter == self.flow_status
        } else {
            true
        }
    }

    fn initiator_matches(&self, maybe_initiator_filter: Option<&InitiatorFilter>) -> bool {
        match maybe_initiator_filter {
            None => true,
            Some(InitiatorFilter::System) => self.initiator.is_none(),
            Some(InitiatorFilter::Account(filter_initiator)) => {
                if let Some(initiator) = &self.initiator {
                    filter_initiator == initiator
                } else {
                    false
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowEventStore)]
#[scope(Singleton)]
impl FlowEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }

    fn update_index(state: &mut State, event: &FlowEvent) {
        if let FlowEvent::Initiated(e) = &event {
            match &e.flow_key {
                FlowKey::Dataset(flow_key) => {
                    let typed_entries = match state.typed_flows_by_dataset.entry(flow_key.clone()) {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    typed_entries.push(event.flow_id());

                    let all_dataset_entries = match state
                        .all_flows_by_dataset
                        .entry(flow_key.dataset_id.clone())
                    {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    all_dataset_entries.push(event.flow_id());

                    state.flow_search_index.insert(
                        event.flow_id(),
                        FlowIndexEntry {
                            flow_type: AnyFlowType::Dataset(flow_key.flow_type),
                            flow_status: FlowStatus::Waiting,
                            initiator: e.trigger.initiator_account_name().cloned(),
                        },
                    );
                }

                FlowKey::System(flow_key) => {
                    let typed_entries = match state.system_flows_by_type.entry(flow_key.flow_type) {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    typed_entries.push(event.flow_id());

                    state.all_system_flows.push(event.flow_id());

                    state.flow_search_index.insert(
                        event.flow_id(),
                        FlowIndexEntry {
                            flow_type: AnyFlowType::System(flow_key.flow_type),
                            flow_status: FlowStatus::Waiting,
                            initiator: e.trigger.initiator_account_name().cloned(),
                        },
                    );
                }
            }

            state.all_flows.push(event.flow_id());
        }
        /* Existing flow must have been indexed, update status */
        else if let Some(new_status) = event.new_status() {
            state
                .flow_search_index
                .get_mut(&event.flow_id())
                .expect("Previously unseen flow ID")
                .flow_status = new_status;
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for FlowEventStoreInMem {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%query, ?opts))]
    fn get_events(&self, query: &FlowID, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        self.inner.get_events(query, opts)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%query, num_events = events.len()))]
    async fn save_events(
        &self,
        query: &FlowID,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index(&mut g, event);
            }
        }

        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for FlowEventStoreInMem {
    #[tracing::instrument(level = "debug", skip_all)]
    fn new_flow_id(&self) -> FlowID {
        self.inner.as_state().lock().unwrap().next_flow_id()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?flow_type))]
    fn get_last_dataset_flow_id_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.typed_flows_by_dataset
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .and_then(|flows| flows.last().copied()))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_type))]
    fn get_last_system_flow_id_of_type(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.system_flows_by_type
            .get(&flow_type)
            .and_then(|flows| flows.last().copied()))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?filters, ?pagination))]
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_flows_by_dataset
                .get(dataset_id)
                .map(|dataset_flow_ids| {
                    dataset_flow_ids
                        .iter()
                        .rev()
                        .filter(|flow_id| g.matches_dataset_flow(**flow_id, &filters))
                        .skip(pagination.offset)
                        .take(pagination.limit)
                        .map(|flow_id| Ok(*flow_id))
                        .collect()
                })
                .unwrap_or_default()
        };

        Box::pin(futures::stream::iter(flow_ids_page))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?filters))]
    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(
            if let Some(dataset_flow_ids) = g.all_flows_by_dataset.get(dataset_id) {
                dataset_flow_ids
                    .iter()
                    .filter(|flow_id| g.matches_dataset_flow(**flow_id, filters))
                    .count()
            } else {
                0
            },
        )
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?filters, ?pagination))]
    fn get_all_system_flow_ids(
        &self,
        filters: SystemFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_system_flows
                .iter()
                .rev()
                .filter(|flow_id| g.matches_system_flow(**flow_id, &filters))
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|flow_id| Ok(*flow_id))
                .collect()
        };
        Box::pin(futures::stream::iter(flow_ids_page))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?filters))]
    async fn get_count_system_flows(
        &self,
        filters: &SystemFlowFilters,
    ) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.all_system_flows
            .iter()
            .filter(|flow_id| g.matches_system_flow(**flow_id, filters))
            .count())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?pagination))]
    fn get_all_flow_ids(&self, pagination: FlowPaginationOpts) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_flows
                .iter()
                .rev()
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|flow_id| Ok(*flow_id))
                .collect()
        };

        Box::pin(futures::stream::iter(flow_ids_page))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_count_all_flows(&self) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.all_flows.len())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
