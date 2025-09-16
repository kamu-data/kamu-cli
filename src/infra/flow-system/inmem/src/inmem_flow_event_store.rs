// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use dill::*;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowEventStore {
    inner: InMemoryEventStore<FlowState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowEvent>,
    last_flow_id: Option<FlowID>,
    all_flows_by_scope: HashMap<FlowScope, Vec<FlowID>>,
    all_flows: Vec<FlowID>,
    flow_search_index: HashMap<FlowID, FlowIndexEntry>,
    flow_binding_by_flow_id: HashMap<FlowID, FlowBinding>,
    flow_last_run_stats: HashMap<FlowBinding, FlowRunStats>,
    flows_by_scheduled_for_activation_time: BTreeMap<DateTime<Utc>, BTreeSet<FlowID>>,
    scheduled_for_activation_time_by_flow_id: HashMap<FlowID, DateTime<Utc>>,
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

    fn matches_flow(&self, flow_id: FlowID, filters: &FlowFilters) -> bool {
        if let Some(index_entry) = self.flow_search_index.get(&flow_id) {
            index_entry.matches_flow_filters(filters)
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
    pub flow_binding: FlowBinding,
    pub flow_status: FlowStatus,
    pub initiator: Option<odf::AccountID>,
}

impl FlowIndexEntry {
    pub fn matches_flow_filters(&self, filters: &FlowFilters) -> bool {
        self.flow_type_matches(filters.by_flow_types.as_deref())
            && self.flow_status_matches(filters.by_flow_status)
            && self.initiator_matches(filters.by_initiator.as_ref())
    }

    fn flow_type_matches(&self, maybe_flow_type_filter: Option<&[String]>) -> bool {
        match maybe_flow_type_filter {
            None => true,
            Some(flow_types) => flow_types.contains(&self.flow_binding.flow_type),
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
                .flow_binding_by_flow_id
                .insert(e.flow_id, e.flow_binding.clone());

            state
                .all_flows_by_scope
                .entry(e.flow_binding.scope.clone())
                .or_default()
                .push(e.flow_id);

            state.flow_search_index.insert(
                event.flow_id(),
                FlowIndexEntry {
                    flow_binding: e.flow_binding.clone(),
                    flow_status: FlowStatus::Waiting,
                    initiator: e.activation_cause.initiator_account_id().cloned(),
                },
            );

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
                let flow_binding = state
                    .flow_binding_by_flow_id
                    .get(&e.flow_id)
                    .expect("Previously unseen flow ID");

                let new_run_stats = FlowRunStats {
                    last_attempt_time: Some(e.event_time),
                    last_success_time: if e.task_outcome.is_success() {
                        Some(e.event_time)
                    } else {
                        None
                    },
                    last_failure_time: if e.task_outcome.is_failure() {
                        Some(e.event_time)
                    } else {
                        None
                    },
                };

                state
                    .flow_last_run_stats
                    .entry(flow_binding.clone())
                    .and_modify(|flow_run_stats_mut_ref| {
                        flow_run_stats_mut_ref.merge(new_run_stats);
                    })
                    .or_insert(new_run_stats);
            }
        }

        // Manage scheduled time changes- insertions
        if let FlowEvent::ScheduledForActivation(e) = &event {
            // Remove any possible previous enqueuing
            Self::remove_flow_scheduling_record(state, e.flow_id);
            // make new record
            Self::insert_flow_scheduling_record(state, e.flow_id, e.scheduled_for_activation_at);
        } else if let FlowEvent::TaskFinished(e) = &event {
            // Will there be a retry?
            if let Some(next_attempt_at) = e.next_attempt_at {
                // Remove any possible previous enqueuing
                Self::remove_flow_scheduling_record(state, e.flow_id);
                // make new record
                Self::insert_flow_scheduling_record(state, e.flow_id, next_attempt_at);
            } else {
                // If no next attempt is scheduled, remove the scheduling record
                Self::remove_flow_scheduling_record(state, e.flow_id);
            }
        }
        // and removals
        else if let FlowEvent::Aborted(_) | FlowEvent::TaskScheduled(_) = &event {
            let flow_id = event.flow_id();
            Self::remove_flow_scheduling_record(state, flow_id);
        }
    }

    fn insert_flow_scheduling_record(
        state: &mut State,
        flow_id: FlowID,
        scheduled_for_activation_at: DateTime<Utc>,
    ) {
        // Update direct lookup
        state
            .flows_by_scheduled_for_activation_time
            .entry(scheduled_for_activation_at)
            .and_modify(|flow_ids| {
                flow_ids.insert(flow_id);
            })
            .or_insert_with(|| BTreeSet::from([flow_id]));

        // Update reverse lookup
        state
            .scheduled_for_activation_time_by_flow_id
            .insert(flow_id, scheduled_for_activation_at);
    }

    fn remove_flow_scheduling_record(state: &mut State, flow_id: FlowID) {
        if let Some(scheduled_for_activation_at) = state
            .scheduled_for_activation_time_by_flow_id
            .remove(&flow_id)
        {
            let flow_ids = state
                .flows_by_scheduled_for_activation_time
                .get_mut(&scheduled_for_activation_at)
                .unwrap();
            flow_ids.remove(&flow_id);
            if flow_ids.is_empty() {
                state
                    .flows_by_scheduled_for_activation_time
                    .remove(&scheduled_for_activation_at);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for InMemoryFlowEventStore {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        self.inner.get_all_events(opts)
    }

    fn get_events(&self, query: &FlowID, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &FlowID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index(&mut g, event);
            }
        }

        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for InMemoryFlowEventStore {
    async fn new_flow_id(&self) -> Result<FlowID, InternalError> {
        Ok(self.inner.as_state().lock().unwrap().next_flow_id())
    }

    async fn try_get_pending_flow(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let waiting_filter = FlowFilters {
            by_flow_types: Some(vec![flow_binding.flow_type.clone()]),
            by_flow_status: Some(FlowStatus::Waiting),
            by_initiator: None,
        };

        let running_filter = FlowFilters {
            by_flow_types: Some(vec![flow_binding.flow_type.clone()]),
            by_flow_status: Some(FlowStatus::Running),
            by_initiator: None,
        };

        Ok(g.all_flows_by_scope
            .get(&flow_binding.scope)
            .map(|scope_flow_ids| {
                scope_flow_ids.iter().rev().find(|flow_id| {
                    g.matches_flow(**flow_id, &waiting_filter)
                        || g.matches_flow(**flow_id, &running_filter)
                })
            })
            .unwrap_or_default()
            .copied())
    }

    async fn try_get_all_scope_pending_flows(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let waiting_filter = FlowFilters {
            by_flow_types: None,
            by_flow_status: Some(FlowStatus::Waiting),
            by_initiator: None,
        };

        let retrying_filter = FlowFilters {
            by_flow_types: None,
            by_flow_status: Some(FlowStatus::Retrying),
            by_initiator: None,
        };

        let running_filter = FlowFilters {
            by_flow_types: None,
            by_flow_status: Some(FlowStatus::Running),
            by_initiator: None,
        };

        Ok(g.all_flows_by_scope
            .get(flow_scope)
            .map(|scope_flow_ids| {
                scope_flow_ids
                    .iter()
                    .rev()
                    .filter(|flow_id| {
                        g.matches_flow(**flow_id, &waiting_filter)
                            || g.matches_flow(**flow_id, &retrying_filter)
                            || g.matches_flow(**flow_id, &running_filter)
                    })
                    .copied()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn get_flow_run_stats(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowRunStats, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.flow_last_run_stats
            .get(flow_binding)
            .copied()
            .unwrap_or_default())
    }

    /// Returns nearest time when one or more flows are scheduled for activation
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.flows_by_scheduled_for_activation_time
            .keys()
            .next()
            .copied())
    }

    /// Returns flows scheduled for activation at the given time
    async fn get_flows_scheduled_for_activation_at(
        &self,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<Vec<FlowID>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        Ok(g.flows_by_scheduled_for_activation_time
            .get(&scheduled_for_activation_at)
            .map(|flow_ids| flow_ids.iter().copied().collect())
            .unwrap_or_default())
    }

    async fn get_count_flows_matching_scope_query(
        &self,
        flow_scope_query: &FlowScopeQuery,
        filters: &FlowFilters,
    ) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.all_flows
            .iter()
            .filter(|flow_id| {
                g.matches_flow(**flow_id, filters)
                    && g.flow_binding_by_flow_id
                        .get(flow_id)
                        .is_some_and(|flow_binding| {
                            flow_binding.scope.matches_query(flow_scope_query)
                        })
            })
            .count())
    }

    fn get_all_flow_ids_matching_scope_query(
        &self,
        flow_scope_query: FlowScopeQuery,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();

            // Collect FlowID -> Most recent event time, for sorting purposes
            let recent_events: HashMap<FlowID, DateTime<Utc>> = g.events.iter().fold(
                HashMap::new(),
                |mut acc: HashMap<FlowID, DateTime<Utc>>, i: &FlowEvent| {
                    let event_time = i.event_time();
                    acc.entry(i.flow_id())
                        .and_modify(|val| {
                            if event_time.gt(val) {
                                *val = event_time;
                            }
                        })
                        .or_insert(event_time);
                    acc
                },
            );

            // Split events by type
            let mut waiting_flows: Vec<_> = vec![];
            let mut running_flows: Vec<_> = vec![];
            let mut retrying_flows: Vec<_> = vec![];
            let mut finished_flows: Vec<_> = vec![];

            for flow_id in &g.all_flows {
                // Also also apply given filters on this stage in order to reduce amount of
                // items to process in further steps
                let flow_binding = g.flow_binding_by_flow_id.get(flow_id).unwrap();
                if flow_binding.scope.matches_query(&flow_scope_query)
                    && g.matches_flow(*flow_id, filters)
                    && let Some(flow) = g.flow_search_index.get(flow_id)
                {
                    let item = (flow_id, recent_events.get(flow_id));
                    match flow.flow_status {
                        FlowStatus::Waiting => waiting_flows.push(item),
                        FlowStatus::Running => running_flows.push(item),
                        FlowStatus::Retrying => retrying_flows.push(item),
                        FlowStatus::Finished => finished_flows.push(item),
                    }
                }
            }
            // Sort every group separately
            waiting_flows.sort_by(|a, b| b.cmp(a));
            running_flows.sort_by(|a, b| b.cmp(a));
            retrying_flows.sort_by(|a, b| b.cmp(a));
            finished_flows.sort_by(|a, b| b.cmp(a));

            let mut ordered_flows = vec![];
            ordered_flows.append(&mut waiting_flows);
            ordered_flows.append(&mut running_flows);
            ordered_flows.append(&mut retrying_flows);
            ordered_flows.append(&mut finished_flows);

            ordered_flows
                .iter()
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|(flow_id, _)| Ok(**flow_id))
                .collect()
        };

        Box::pin(futures::stream::iter(flow_ids_page))
    }

    fn list_scoped_flow_initiators(&self, flow_scope_query: FlowScopeQuery) -> InitiatorIDStream {
        let flow_initiators: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            let mut unique_initiators = HashSet::new();

            g.all_flows
                .iter()
                .filter_map(|flow_id| {
                    // Check if flow matches the scope query
                    let flow_binding = g.flow_binding_by_flow_id.get(flow_id).unwrap();
                    if !flow_binding.scope.matches_query(&flow_scope_query) {
                        return None;
                    }

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
        };

        Box::pin(futures::stream::iter(flow_initiators))
    }

    fn get_all_flow_ids(&self, filters: &FlowFilters, pagination: PaginationOpts) -> FlowIDStream {
        let flow_ids_page: Vec<_> = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.all_flows
                .iter()
                .rev()
                .filter(|flow_id| g.matches_flow(**flow_id, filters))
                .skip(pagination.offset)
                .take(pagination.limit)
                .map(|flow_id| Ok(*flow_id))
                .collect()
        };
        Box::pin(futures::stream::iter(flow_ids_page))
    }

    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.all_flows
            .iter()
            .filter(|flow_id| g.matches_flow(**flow_id, filters))
            .count())
    }

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream {
        Box::pin(async_stream::try_stream! {
            const CHUNK_SIZE: usize = 256;
            for chunk in flow_ids.chunks(CHUNK_SIZE) {
                let flows = Flow::load_multi(
                    chunk.to_vec(),
                    self
                ).await.int_err()?;
                for flow in flows {
                    yield flow.int_err()?.into();
                }
            }
        })
    }

    async fn filter_flow_scopes_having_flows(
        &self,
        flow_scopes: &[FlowScope],
    ) -> Result<Vec<FlowScope>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut filtered_flow_scopes = HashSet::new();
        for flow_id in &g.all_flows {
            let flow_binding = g.flow_binding_by_flow_id.get(flow_id).unwrap();
            if flow_scopes.contains(&flow_binding.scope)
                && !filtered_flow_scopes.contains(&flow_binding.scope)
            {
                filtered_flow_scopes.insert(flow_binding.scope.clone());
            }
        }
        Ok(filtered_flow_scopes.into_iter().collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
