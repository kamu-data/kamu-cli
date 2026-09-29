// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_flow_system::{FlowOutcomeKind, FlowState};
use observability::metrics::{MetricsProvider, seconds_between};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Flows span their tasks plus retry backoff: seconds to a day
const FLOW_DURATION_BUCKETS_SECONDS: &[f64] = &[
    1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 21600.0, 43200.0, 86400.0,
];

// Retries are counts, but Prometheus histograms are float-valued
const FLOW_RETRIES_BUCKETS: [u32; 6] = [0, 1, 2, 3, 5, 10];

// Aborted flows are counted apart: an abort is a user decision, so its timing
// says nothing about the system
const COMPLETED_OUTCOMES: [FlowOutcomeKind; 2] =
    [FlowOutcomeKind::Success, FlowOutcomeKind::Failed];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowCompletionMetrics {
    pub flow_duration_seconds: prometheus::HistogramVec,
    pub flow_retries: prometheus::HistogramVec,
    pub flows_aborted_total: prometheus::IntCounterVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl FlowCompletionMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            // Its count doubles as the number of completed flows
            flow_duration_seconds: HistogramVec::new(
                HistogramOpts::new(
                    "flow_system_flow_duration_seconds",
                    "Time from a flow's first planned activation to its completion, across all \
                     its tasks and retries, by flow type and outcome",
                )
                .buckets(FLOW_DURATION_BUCKETS_SECONDS.to_vec()),
                &["flow_type", "outcome"],
            )
            .unwrap(),
            flow_retries: HistogramVec::new(
                HistogramOpts::new(
                    "flow_system_flow_retries",
                    "Number of retried task attempts of a completed flow, by flow type and outcome",
                )
                .buckets(FLOW_RETRIES_BUCKETS.map(f64::from).to_vec()),
                &["flow_type", "outcome"],
            )
            .unwrap(),
            flows_aborted_total: IntCounterVec::new(
                Opts::new(
                    "flow_system_flows_aborted_total",
                    "Number of flows aborted by users, or by removal of their trigger or dataset, \
                     by flow type",
                ),
                &["flow_type"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init<'a>(&self, flow_types: impl Iterator<Item = &'a str>) {
        for flow_type in flow_types {
            for outcome in COMPLETED_OUTCOMES {
                let outcome = outcome.into();
                self.flow_duration_seconds
                    .with_label_values(&[flow_type, outcome]);
                self.flow_retries.with_label_values(&[flow_type, outcome]);
            }
            self.flows_aborted_total
                .with_label_values(&[flow_type])
                .reset();
        }
    }

    /// Records a flow that has just reached its final outcome
    pub(crate) fn on_flow_finished(&self, flow: &FlowState) {
        let flow_type = flow.flow_binding.flow_type.as_str();
        let outcome: &str = match flow.outcome.as_ref().map(FlowOutcomeKind::from) {
            Some(FlowOutcomeKind::Aborted) => {
                self.flows_aborted_total
                    .with_label_values(&[flow_type])
                    .inc();
                return;
            }
            Some(kind) => kind.into(),
            None => return,
        };

        if let (Some(first_scheduled_at), Some(completed_at)) =
            (flow.timing.first_scheduled_at, flow.timing.completed_at)
        {
            self.flow_duration_seconds
                .with_label_values(&[flow_type, outcome])
                .observe(seconds_between(first_scheduled_at, completed_at));
        }

        let retries = flow.task_ids.len().saturating_sub(1);
        self.flow_retries
            .with_label_values(&[flow_type, outcome])
            .observe(f64::from(u32::try_from(retries).unwrap_or(u32::MAX)));
    }
}

impl MetricsProvider for FlowCompletionMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.flow_duration_seconds.clone()))?;
        reg.register(Box::new(self.flow_retries.clone()))?;
        reg.register(Box::new(self.flows_aborted_total.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
