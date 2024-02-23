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
use dill::*;
use event_bus::AsyncEventHandler;
use kamu_core::{FakeSystemTimeSource, InternalError};
use kamu_flow_system::{
    FlowKey,
    FlowOutcome,
    FlowPaginationOpts,
    FlowService,
    FlowServiceEvent,
    FlowState,
};
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowSystemTestListener {
    flow_service: Arc<dyn FlowService>,
    fake_time_source: Arc<FakeSystemTimeSource>,
    state: Arc<Mutex<FlowSystemTestListenerState>>,
}

type FlowSnapshot = (DateTime<Utc>, HashMap<FlowKey, Vec<FlowState>>);

#[derive(Default)]
struct FlowSystemTestListenerState {
    snapshots: Vec<FlowSnapshot>,
    dataset_display_names: HashMap<DatasetID, String>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AsyncEventHandler<FlowServiceEvent>)]
impl FlowSystemTestListener {
    pub(crate) fn new(
        flow_service: Arc<dyn FlowService>,
        fake_time_source: Arc<FakeSystemTimeSource>,
    ) -> Self {
        Self {
            flow_service,
            fake_time_source,
            state: Arc::new(Mutex::new(FlowSystemTestListenerState::default())),
        }
    }

    pub(crate) async fn make_a_snapshot(&self, event_time: DateTime<Utc>) {
        use futures::TryStreamExt;
        let flows: Vec<_> = self
            .flow_service
            .list_all_flows(FlowPaginationOpts {
                limit: 100,
                offset: 0,
            })
            .await
            .unwrap()
            .matched_stream
            .try_collect()
            .await
            .unwrap();

        let mut flow_states_map: HashMap<FlowKey, Vec<FlowState>> = HashMap::new();
        for flow in flows {
            flow_states_map
                .entry(flow.flow_key.clone())
                .and_modify(|flows| flows.push(flow.clone()))
                .or_insert(vec![flow]);
        }

        let mut state = self.state.lock().unwrap();
        state.snapshots.push((event_time, flow_states_map));
    }

    pub(crate) fn define_dataset_display_name(&self, id: DatasetID, display_name: String) {
        let mut state = self.state.lock().unwrap();
        state.dataset_display_names.insert(id, display_name);
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
                .map(|flow_key| {
                    (
                        flow_key,
                        match flow_key {
                            FlowKey::Dataset(fk_dataset) => format!(
                                "\"{}\" {:?}",
                                state
                                    .dataset_display_names
                                    .get(&fk_dataset.dataset_id)
                                    .cloned()
                                    .unwrap_or_else(|| fk_dataset.dataset_id.to_string()),
                                fk_dataset.flow_type
                            ),
                            FlowKey::System(fk_system) => {
                                format!("System {:?}", fk_system.flow_type)
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            flow_headings.sort_by_key(|(_, title)| title.clone());

            for (flow_key, heading) in flow_headings {
                writeln!(f, "  {heading}:")?;
                for state in snapshots.get(flow_key).unwrap() {
                    write!(f, "    Flow ID = {} {:?}", state.flow_id, state.status(),)?;
                    if let Some(outcome) = &state.outcome {
                        writeln!(
                            f,
                            " {}",
                            match outcome {
                                FlowOutcome::Success(_) => "Success",
                                FlowOutcome::Aborted => "Aborted",
                                FlowOutcome::Cancelled => "Cancelled",
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

#[async_trait::async_trait]
impl AsyncEventHandler<FlowServiceEvent> for FlowSystemTestListener {
    async fn handle(&self, event: &FlowServiceEvent) -> Result<(), InternalError> {
        self.make_a_snapshot(event.event_time()).await;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
