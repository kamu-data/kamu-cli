// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use database_common::PaginationOpts;
use dill::*;
use kamu_flow_system::{BorrowedFlowKeyDataset, *};
use opendatafabric::{AccountID, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowEventStore {
    inner: InMemoryEventStore<FlowState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowEvent>,
    last_flow_id: Option<FlowID>,
    all_flows_by_dataset: HashMap<DatasetID, Vec<FlowID>>,
    all_system_flows: Vec<FlowID>,
    all_flows: Vec<FlowID>,
    flow_search_index: HashMap<FlowID, FlowIndexEntry>,
    flow_key_by_flow_id: HashMap<FlowID, FlowKey>,
    dataset_flow_last_run_stats: HashMap<FlowKeyDataset, FlowRunStats>,
    system_flow_last_run_stats: HashMap<SystemFlowType, FlowRunStats>,
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

    fn matches_any_flow(&self, flow_id: FlowID, filters: &AllFlowFilters) -> bool {
        if let Some(index_entry) = self.flow_search_index.get(&flow_id) {
            index_entry.matches_all_flow_filters(filters)
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
    pub initiator: Option<AccountID>,
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

    pub fn matches_all_flow_filters(&self, filters: &AllFlowFilters) -> bool {
        self.flow_status_matches(filters.by_flow_status)
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
                    filter_initiator.contains(initiator)
                } else {
                    false
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowEventStore)]
#[scope(Singleton)]
impl InMemoryFlowEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }

    fn update_index(state: &mut State, event: &FlowEvent) {
        if let FlowEvent::Initiated(e) = &event {
            state
                .flow_key_by_flow_id
                .insert(e.flow_id, e.flow_key.clone());

            match &e.flow_key {
                FlowKey::Dataset(flow_key) => {
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
                            initiator: e.trigger.initiator_account_id().cloned(),
                        },
                    );
                }

                FlowKey::System(flow_key) => {
                    state.all_system_flows.push(event.flow_id());

                    state.flow_search_index.insert(
                        event.flow_id(),
                        FlowIndexEntry {
                            flow_type: AnyFlowType::System(flow_key.flow_type),
                            flow_status: FlowStatus::Waiting,
                            initiator: e.trigger.initiator_account_id().cloned(),
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

            // Record last attempted/succeeded flows
            if let FlowEvent::TaskFinished(e) = &event {
                let flow_key = state
                    .flow_key_by_flow_id
                    .get(&e.flow_id)
                    .expect("Previously unseen flow ID");

                let new_run_stats = FlowRunStats {
                    last_attempt_time: Some(e.event_time),
                    last_success_time: if e.task_outcome.is_success() {
                        Some(e.event_time)
                    } else {
                        None
                    },
                };

                match flow_key {
                    FlowKey::Dataset(flow_key) => state
                        .dataset_flow_last_run_stats
                        .entry(flow_key.clone())
                        .and_modify(|flow_run_stats_mut_ref| {
                            flow_run_stats_mut_ref.merge(new_run_stats);
                        })
                        .or_insert(new_run_stats),

                    FlowKey::System(flow_key) => state
                        .system_flow_last_run_stats
                        .entry(flow_key.flow_type)
                        .and_modify(|flow_run_stats_mut_ref| {
                            flow_run_stats_mut_ref.merge(new_run_stats);
                        })
                        .or_insert(new_run_stats),
                };
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for InMemoryFlowEventStore {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for InMemoryFlowEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn new_flow_id(&self) -> Result<FlowID, InternalError> {
        Ok(self.inner.as_state().lock().unwrap().next_flow_id())
    }

    async fn try_get_pending_flow(
        &self,
        flow_key: &FlowKey,
    ) -> Result<Option<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        Ok(match flow_key {
            FlowKey::Dataset(flow_key) => {
                let waiting_filter = DatasetFlowFilters {
                    by_flow_type: Some(flow_key.flow_type),
                    by_flow_status: Some(FlowStatus::Waiting),
                    by_initiator: None,
                };

                let running_filter = DatasetFlowFilters {
                    by_flow_type: Some(flow_key.flow_type),
                    by_flow_status: Some(FlowStatus::Running),
                    by_initiator: None,
                };

                g.all_flows_by_dataset
                    .get(&flow_key.dataset_id)
                    .map(|dataset_flow_ids| {
                        dataset_flow_ids.iter().rev().find(|flow_id| {
                            g.matches_dataset_flow(**flow_id, &waiting_filter)
                                || g.matches_dataset_flow(**flow_id, &running_filter)
                        })
                    })
                    .unwrap_or_default()
                    .copied()
            }
            FlowKey::System(flow_key) => {
                let waiting_filter = SystemFlowFilters {
                    by_flow_type: Some(flow_key.flow_type),
                    by_flow_status: Some(FlowStatus::Waiting),
                    by_initiator: None,
                };

                let running_filter = SystemFlowFilters {
                    by_flow_type: Some(flow_key.flow_type),
                    by_flow_status: Some(FlowStatus::Running),
                    by_initiator: None,
                };

                g.all_system_flows
                    .iter()
                    .rev()
                    .find(|flow_id| {
                        g.matches_system_flow(**flow_id, &waiting_filter)
                            || g.matches_system_flow(**flow_id, &running_filter)
                    })
                    .copied()
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?flow_type))]
    async fn get_dataset_flow_run_stats(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.dataset_flow_last_run_stats
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .copied()
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_type))]
    async fn get_system_flow_run_stats(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.system_flow_last_run_stats
            .get(&flow_type)
            .copied()
            .unwrap_or_default())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id, ?filters, ?pagination))]
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
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
                        .filter(|flow_id| g.matches_dataset_flow(**flow_id, filters))
                        .skip(pagination.offset)
                        .take(pagination.limit)
                        .map(|flow_id| Ok(*flow_id))
                        .collect()
                })
                .unwrap_or_default()
        };

        Box::pin(futures::stream::iter(flow_ids_page))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    fn get_unique_flow_initiator_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> InitiatorIDStream {
        let flow_initiators: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            let mut unique_initiators = HashSet::new();
            g.all_flows_by_dataset
                .get(dataset_id)
                .map(|dataset_flow_ids| {
                    dataset_flow_ids
                        .iter()
                        .filter_map(|flow_id| {
                            let search_index_maybe = g.flow_search_index.get(flow_id);
                            if let Some(search_index) = search_index_maybe
                                && let Some(initiator_id) = &search_index.initiator
                            {
                                let is_added = unique_initiators.insert(initiator_id);
                                if is_added {
                                    return Some(Ok(initiator_id.clone()));
                                }
                                return None;
                            }
                            None
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        Box::pin(futures::stream::iter(flow_initiators))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_ids, ?pagination))]
    fn get_all_flow_ids_by_datasets(
        &self,
        dataset_ids: HashSet<DatasetID>,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            let mut result: Vec<Result<FlowID, _>> = vec![];
            let mut total_count = 0;
            for flow_id in g.all_flows.iter().rev() {
                let flow_key = g.flow_key_by_flow_id.get(flow_id).unwrap();
                if let FlowKey::Dataset(flow_key_dataset) = flow_key {
                    if dataset_ids.contains(&flow_key_dataset.dataset_id)
                        && g.matches_dataset_flow(*flow_id, filters)
                    {
                        if result.len() >= pagination.limit {
                            break;
                        }
                        if total_count >= pagination.offset {
                            result.push(Ok(*flow_id));
                        }
                        total_count += 1;
                    }
                };
            }
            result
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
        filters: &SystemFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_system_flows
                .iter()
                .rev()
                .filter(|flow_id| g.matches_system_flow(**flow_id, filters))
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
    fn get_all_flow_ids(
        &self,
        filters: &AllFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_flows
                .iter()
                .rev()
                .filter(|flow_id| g.matches_any_flow(**flow_id, filters))
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|flow_id| Ok(*flow_id))
                .collect()
        };
        Box::pin(futures::stream::iter(flow_ids_page))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn get_count_all_flows(&self, filters: &AllFlowFilters) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.all_flows
            .iter()
            .filter(|flow_id| g.matches_any_flow(**flow_id, filters))
            .count())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
