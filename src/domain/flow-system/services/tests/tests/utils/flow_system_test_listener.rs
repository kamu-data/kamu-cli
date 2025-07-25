// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use dill::*;
use internal_error::InternalError;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
};
use kamu_flow_system::*;
use kamu_flow_system_services::{
    MESSAGE_PRODUCER_KAMU_FLOW_AGENT,
    MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE,
};
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};
use time_source::FakeSystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowSystemTestListener {
    flow_query_service: Arc<dyn FlowQueryService>,
    fake_time_source: Arc<FakeSystemTimeSource>,
    state: Arc<Mutex<FlowSystemTestListenerState>>,
}

type FlowSnapshot = (DateTime<Utc>, HashMap<FlowBinding, Vec<FlowState>>);

#[derive(Default)]
struct FlowSystemTestListenerState {
    snapshots: Vec<FlowSnapshot>,
    dataset_display_names: HashMap<odf::DatasetID, String>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<FlowAgentUpdatedMessage>)]
#[interface(dyn MessageConsumerT<FlowProgressMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "FlowSystemTestListener",
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_FLOW_AGENT, MESSAGE_PRODUCER_KAMU_FLOW_PROGRESS_SERVICE],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
impl FlowSystemTestListener {
    pub(crate) fn new(
        flow_query_service: Arc<dyn FlowQueryService>,
        fake_time_source: Arc<FakeSystemTimeSource>,
    ) -> Self {
        Self {
            flow_query_service,
            fake_time_source,
            state: Arc::new(Mutex::new(FlowSystemTestListenerState::default())),
        }
    }

    pub(crate) async fn make_a_snapshot(&self, update_time: DateTime<Utc>) {
        use futures::TryStreamExt;
        let flows: Vec<_> = self
            .flow_query_service
            .list_all_flows(PaginationOpts {
                limit: 100,
                offset: 0,
            })
            .await
            .unwrap()
            .matched_stream
            .try_collect()
            .await
            .unwrap();

        let mut flow_states_map: HashMap<FlowBinding, Vec<FlowState>> = HashMap::new();
        for flow in flows {
            flow_states_map
                .entry(flow.flow_binding.clone())
                .and_modify(|flows| flows.push(flow.clone()))
                .or_insert(vec![flow]);
        }

        let mut state = self.state.lock().unwrap();
        state.snapshots.push((update_time, flow_states_map));
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
            FLOW_TYPE_SYSTEM_GC => "GC",
            _ => "<unknown>",
        }
    }
}

impl std::fmt::Display for FlowSystemTestListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let initial_time = self.fake_time_source.initial_time;

        let state = self.state.lock().unwrap();
        for i in 0..state.snapshots.len() {
            let (snapshot_time, snapshots) = state.snapshots.get(i).unwrap();
            writeln!(
                f,
                "#{i}: +{}ms:",
                (*snapshot_time - initial_time).num_milliseconds(),
            )?;

            let mut flow_headings = snapshots
                .keys()
                .map(|flow_binding| {
                    (
                        flow_binding,
                        match &flow_binding.scope {
                            FlowScope::Dataset { dataset_id } => format!(
                                "\"{}\" {}",
                                state
                                    .dataset_display_names
                                    .get(dataset_id)
                                    .cloned()
                                    .unwrap_or_else(|| dataset_id.to_string()),
                                Self::display_flow_type(flow_binding.flow_type.as_str())
                            ),
                            FlowScope::System => {
                                format!(
                                    "System {}",
                                    Self::display_flow_type(flow_binding.flow_type.as_str())
                                )
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            flow_headings.sort_by_key(|(_, title)| title.clone());

            for (flow_key, heading) in flow_headings {
                writeln!(f, "  {heading}:")?;
                for flow_state in snapshots.get(flow_key).unwrap() {
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
                            match flow_state.primary_trigger() {
                                FlowTriggerInstance::Manual(_) => String::from("Manual"),
                                FlowTriggerInstance::AutoPolling(_) => String::from("AutoPolling"),
                                FlowTriggerInstance::Push(_) => String::from("Push"),
                                FlowTriggerInstance::InputDatasetFlow(i) => format!(
                                    "Input({})",
                                    state
                                        .dataset_display_names
                                        .get(&i.dataset_id)
                                        .cloned()
                                        .unwrap_or_else(|| i.dataset_id.to_string()),
                                ),
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
                            FlowStartCondition::Batching(b) => write!(
                                f,
                                " Batching({}, until={}ms)",
                                b.active_batching_rule.min_records_to_await(),
                                (b.batching_deadline - initial_time).num_milliseconds(),
                            )?,
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
                                FlowOutcome::Failed => "Failed",
                            }
                        )?;
                    } else {
                        writeln!(f)?;
                    }
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

impl MessageConsumer for FlowSystemTestListener {}

#[async_trait::async_trait]
impl MessageConsumerT<FlowAgentUpdatedMessage> for FlowSystemTestListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &FlowAgentUpdatedMessage,
    ) -> Result<(), InternalError> {
        self.make_a_snapshot(message.update_time).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageConsumerT<FlowProgressMessage> for FlowSystemTestListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &FlowProgressMessage,
    ) -> Result<(), InternalError> {
        match message {
            FlowProgressMessage::Running(e) => self.make_a_snapshot(e.event_time).await,
            FlowProgressMessage::RetryScheduled(e) => self.make_a_snapshot(e.event_time).await,
            FlowProgressMessage::Finished(e) => self.make_a_snapshot(e.event_time).await,
            FlowProgressMessage::Cancelled(e) => self.make_a_snapshot(e.event_time).await,
            FlowProgressMessage::Scheduled(_) => {}
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
