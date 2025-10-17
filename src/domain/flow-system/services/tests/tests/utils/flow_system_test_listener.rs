// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::panic;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::*;
use internal_error::InternalError;
use kamu_adapter_flow_dataset::{
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_SCOPE_TYPE_DATASET,
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_RESET_TO_METADATA,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION, FlowScopeSubscription};
use kamu_flow_system::*;
use time_source::FakeSystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowSystemTestListener {
    fake_time_source: Arc<FakeSystemTimeSource>,
    state: Arc<Mutex<FlowSystemTestListenerState>>,
}

type FlowSnapshot = HashMap<FlowBinding, Vec<FlowState>>;

#[derive(Default)]
struct FlowSystemTestListenerState {
    loaded: bool,
    latest_flow_aggregates: HashMap<FlowID, Flow>,
    snapshots: BTreeMap<DateTime<Utc>, Vec<FlowSnapshot>>,
    dataset_display_names: HashMap<odf::DatasetID, String>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn FlowSystemEventProjector)]
impl FlowSystemTestListener {
    pub(crate) fn new(fake_time_source: Arc<FakeSystemTimeSource>) -> Self {
        Self {
            fake_time_source,
            state: Arc::new(Mutex::new(FlowSystemTestListenerState::default())),
        }
    }

    fn apply_flow_event(&self, e: FlowEvent) {
        let mut state = self.state.lock().unwrap();

        state
            .latest_flow_aggregates
            .entry(e.flow_id())
            .and_modify(|existing| {
                existing.apply(e.clone()).unwrap();
            })
            .or_insert_with(|| {
                let FlowEvent::Initiated(initiated_event) = e else {
                    panic!("First event for a flow must be Initiated");
                };
                Flow::new(
                    initiated_event.event_time,
                    initiated_event.flow_id,
                    initiated_event.flow_binding,
                    initiated_event.activation_cause,
                    initiated_event.config_snapshot,
                    initiated_event.retry_policy,
                )
            });
    }

    pub(crate) fn mark_as_loaded(&self) {
        let mut state = self.state.lock().unwrap();
        state.loaded = true;
    }

    pub(crate) fn make_a_snapshot(&self, event_time: DateTime<Utc>) {
        let mut state = self.state.lock().unwrap();

        let mut flow_states_map: HashMap<FlowBinding, Vec<FlowState>> = HashMap::new();

        let mut flow_ids = state
            .latest_flow_aggregates
            .keys()
            .copied()
            .collect::<Vec<_>>();

        flow_ids.sort_by(|a, b| b.cmp(a));

        for flow_id in flow_ids {
            let flow = state.latest_flow_aggregates.get(&flow_id).unwrap();
            flow_states_map
                .entry(flow.flow_binding.clone())
                .and_modify(|flows| flows.push(flow.as_ref().clone()))
                .or_insert(vec![flow.as_ref().clone()]);
        }

        state
            .snapshots
            .entry(event_time)
            .and_modify(|snapshots| snapshots.push(flow_states_map.clone()))
            .or_insert(vec![flow_states_map]);
    }

    pub(crate) fn define_dataset_display_name(&self, id: odf::DatasetID, display_name: String) {
        let mut state = self.state.lock().unwrap();
        state.dataset_display_names.insert(id, display_name);
    }

    fn display_flow_type(flow_type_label: &str) -> &'static str {
        match flow_type_label {
            FLOW_TYPE_DATASET_INGEST => "Ingest",
            FLOW_TYPE_DATASET_TRANSFORM => "ExecuteTransform",
            FLOW_TYPE_DATASET_COMPACT => "HardCompaction",
            FLOW_TYPE_DATASET_RESET => "Reset",
            FLOW_TYPE_DATASET_RESET_TO_METADATA => "ResetToMetadata",
            FLOW_TYPE_SYSTEM_GC => "GC",
            _ => "<unknown>",
        }
    }

    fn get_accumulated_reactive_records(
        &self,
        flow_state: &FlowState,
        reactive_flow_condition: &FlowStartConditionReactive,
    ) -> u64 {
        flow_state
            .get_reactive_data_increment(reactive_flow_condition.last_activation_cause_index)
            .records_added
    }
}

impl std::fmt::Display for FlowSystemTestListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let initial_time = self.fake_time_source.initial_time;

        let state = self.state.lock().unwrap();

        let snapshots_iter = state.snapshots.iter();
        let mut index = 0;

        for (snapshot_time, snapshots) in snapshots_iter {
            for snapshot in snapshots {
                writeln!(
                    f,
                    "#{index}: +{}ms:",
                    (*snapshot_time - initial_time).num_milliseconds(),
                )?;

                let mut flow_headings = snapshot
                    .keys()
                    .map(|flow_binding| {
                        (
                            flow_binding,
                            match flow_binding.scope.scope_type() {
                                FLOW_SCOPE_TYPE_DATASET => {
                                    let dataset_id =
                                        FlowScopeDataset::new(&flow_binding.scope).dataset_id();
                                    format!(
                                        "\"{}\" {}",
                                        state
                                            .dataset_display_names
                                            .get(&dataset_id)
                                            .cloned()
                                            .unwrap_or_else(|| dataset_id.to_string()),
                                        Self::display_flow_type(flow_binding.flow_type.as_str())
                                    )
                                }
                                FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION => {
                                    let subscription_scope =
                                        FlowScopeSubscription::new(&flow_binding.scope);
                                    let subscription_id = subscription_scope.subscription_id();
                                    let maybe_dataset_id = subscription_scope.maybe_dataset_id();

                                    format!(
                                        "\"{}\" Subscription: {} {}",
                                        match maybe_dataset_id {
                                            Some(dataset_id) => state
                                                .dataset_display_names
                                                .get(&dataset_id)
                                                .cloned()
                                                .unwrap_or_else(|| dataset_id.to_string()),
                                            None => "<None>".to_string(),
                                        },
                                        subscription_id,
                                        Self::display_flow_type(flow_binding.flow_type.as_str())
                                    )
                                }
                                FLOW_SCOPE_TYPE_SYSTEM => {
                                    format!(
                                        "System {}",
                                        Self::display_flow_type(flow_binding.flow_type.as_str())
                                    )
                                }
                                _ => panic!(
                                    "Unexpected flow scope type: {}",
                                    flow_binding.scope.scope_type()
                                ),
                            },
                        )
                    })
                    .collect::<Vec<_>>();
                flow_headings.sort_by_key(|(_, title)| title.clone());

                for (flow_binding, heading) in flow_headings {
                    writeln!(f, "  {heading}:")?;
                    for flow_state in snapshot.get(flow_binding).unwrap() {
                        write!(
                            f,
                            "    Flow ID = {} {}",
                            flow_state.flow_id,
                            match flow_state.status() {
                                FlowStatus::Waiting => "Waiting".to_string(),
                                FlowStatus::Running => format!(
                                    "{:?}(task={})",
                                    flow_state.status(),
                                    flow_state
                                        .task_ids
                                        .iter()
                                        .map(|task_id| format!("{task_id}"))
                                        .collect::<Vec<_>>()
                                        .join(",")
                                ),
                                FlowStatus::Retrying => format!(
                                    "{:?}(scheduled_at={}ms)",
                                    flow_state.status(),
                                    (flow_state.timing.scheduled_for_activation_at.unwrap()
                                        - initial_time)
                                        .num_milliseconds()
                                ),
                                _ => format!("{:?}", flow_state.status()),
                            }
                        )?;

                        if matches!(flow_state.status(), FlowStatus::Waiting) {
                            write!(
                                f,
                                " {}",
                                match flow_state.primary_activation_cause() {
                                    FlowActivationCause::Manual(_) => String::from("Manual"),
                                    FlowActivationCause::AutoPolling(_) =>
                                        String::from("AutoPolling"),
                                    FlowActivationCause::ResourceUpdate(update) => {
                                        let update_details: DatasetResourceUpdateDetails =
                                            serde_json::from_value(update.details.clone()).unwrap();
                                        match &update_details.source {
                                            DatasetUpdateSource::HttpIngest { .. } => {
                                                String::from("HttpIngest")
                                            }
                                            DatasetUpdateSource::SmartProtocolPush { .. } => {
                                                String::from("SmartProtocolPush")
                                            }
                                            DatasetUpdateSource::ExternallyDetectedChange => {
                                                String::from("ExternallyDetectedChange")
                                            }
                                            DatasetUpdateSource::UpstreamFlow { .. } => format!(
                                                "Input({})",
                                                state
                                                    .dataset_display_names
                                                    .get(&update_details.dataset_id)
                                                    .cloned()
                                                    .unwrap_or_else(|| update_details
                                                        .dataset_id
                                                        .to_string())
                                            ),
                                        }
                                    }
                                }
                            )?;
                        }

                        if let Some(start_condition) = flow_state.start_condition {
                            match start_condition {
                                FlowStartCondition::Throttling(t) => {
                                    write!(
                                        f,
                                        " Throttling(for={}ms, wakeup={}ms, shifted={}ms)",
                                        t.interval.num_milliseconds(),
                                        (t.wake_up_at - initial_time).num_milliseconds(),
                                        (t.shifted_from - initial_time).num_milliseconds()
                                    )?;
                                }
                                FlowStartCondition::Reactive(r) => {
                                    write!(
                                        f,
                                        " Batching({}/{}, until={}ms)",
                                        self.get_accumulated_reactive_records(flow_state, &r),
                                        r.active_rule.for_new_data.min_records_to_await(),
                                        (r.batching_deadline - initial_time).num_milliseconds(),
                                    )?;
                                    if let Some(activation_time) =
                                        flow_state.timing.scheduled_for_activation_at
                                    {
                                        write!(
                                            f,
                                            " Activating(at={}ms)",
                                            (activation_time - initial_time).num_milliseconds()
                                        )?;
                                    }
                                }
                                FlowStartCondition::Executor(e) => {
                                    write!(
                                        f,
                                        " Executor(task={}, since={}ms)",
                                        e.task_id,
                                        (flow_state.timing.awaiting_executor_since.unwrap()
                                            - initial_time)
                                            .num_milliseconds()
                                    )?;
                                }
                                FlowStartCondition::Schedule(s) => {
                                    write!(
                                        f,
                                        " Schedule(wakeup={}ms)",
                                        (s.wake_up_at - initial_time).num_milliseconds(),
                                    )?;
                                    if let Some(activation_time) =
                                        flow_state.timing.scheduled_for_activation_at
                                        && s.wake_up_at != activation_time
                                    {
                                        write!(
                                            f,
                                            " Activating(at={}ms)",
                                            (activation_time - initial_time).num_milliseconds()
                                        )?;
                                    }
                                }
                            }
                        }

                        if let Some(outcome) = &flow_state.outcome {
                            writeln!(
                                f,
                                " {}",
                                match outcome {
                                    FlowOutcome::Success(_) => "Success",
                                    FlowOutcome::Aborted => "Aborted",
                                    FlowOutcome::Failed(_) => "Failed",
                                }
                            )?;
                        } else {
                            writeln!(f)?;
                        }
                    }
                }
                writeln!(f)?;

                index += 1;
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventProjector for FlowSystemTestListener {
    fn name(&self) -> &'static str {
        "FlowSystemTestListener"
    }

    /// Apply a *single* event using the open transaction.
    /// Must be idempotent: safe to re-run for the same event id.
    async fn apply(&self, e: &FlowSystemEvent) -> Result<(), InternalError> {
        // We are only interested in flow events
        match e.source_type {
            FlowSystemEventSourceType::Flow => {
                // Apply all flow events to reconstruct Flow aggregates
                let flow_event: FlowEvent = serde_json::from_value(e.payload.clone()).unwrap();
                self.apply_flow_event(flow_event.clone());

                // Skip snapshots until we have received the Loaded message
                {
                    let state = self.state.lock().unwrap();
                    if !state.loaded {
                        return Ok(());
                    }
                }

                // During live phase, we only take snapshots on certain events
                match flow_event {
                    FlowEvent::ScheduledForActivation(_)
                    | FlowEvent::Aborted(_)
                    | FlowEvent::TaskRunning(_)
                    | FlowEvent::TaskFinished(_) => {
                        self.make_a_snapshot(flow_event.event_time());
                    }

                    FlowEvent::StartConditionUpdated(condition_updated) => {
                        match condition_updated.start_condition {
                            FlowStartCondition::Executor(_) | FlowStartCondition::Reactive(_) => {
                                self.make_a_snapshot(condition_updated.event_time);
                            }
                            FlowStartCondition::Throttling(_) | FlowStartCondition::Schedule(_) => {
                                // Ignore, the same info is in
                                // ScheduledForActivation
                            }
                        }
                    }

                    FlowEvent::ActivationCauseAdded(_)
                    | FlowEvent::ConfigSnapshotModified(_)
                    | FlowEvent::Completed(_)
                    | FlowEvent::Initiated(_)
                    | FlowEvent::TaskScheduled(_) => { /* Ignore */ }
                }
            }

            FlowSystemEventSourceType::FlowConfiguration
            | FlowSystemEventSourceType::FlowTrigger => { /* Ignore */ }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
