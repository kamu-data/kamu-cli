// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use dill::*;
use observability::metrics::{MetricsProvider, seconds_between};
use strum::IntoEnumIterator as _;

use crate::ActivateFlowError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Activations are normally sub-second late; minutes mean the agent falls behind
const ACTIVATION_DELAY_BUCKETS_SECONDS: &[f64] =
    &[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0, 300.0];

#[derive(Clone, Copy, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
enum ActivationOutcome {
    Activated,
    /// The flow changed concurrently, re-read next pass
    Skipped,
    /// Retried later
    Failed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowAgentMetrics {
    pub activations_total: prometheus::IntCounterVec,
    pub activation_delay_seconds: prometheus::Histogram,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl FlowAgentMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            activations_total: IntCounterVec::new(
                Opts::new(
                    "flow_agent_activations_total",
                    "Number of flow activation attempts by flow type and outcome: activated, \
                     skipped (the flow changed concurrently) or failed (retried later)",
                ),
                &["flow_type", "outcome"],
            )
            .unwrap(),
            activation_delay_seconds: Histogram::with_opts(
                HistogramOpts::new(
                    "flow_agent_activation_delay_seconds",
                    "Time from the moment a flow was scheduled to activate until it was activated",
                )
                .buckets(ACTIVATION_DELAY_BUCKETS_SECONDS.to_vec()),
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init<'a>(&self, flow_types: impl Iterator<Item = &'a str>) {
        for flow_type in flow_types {
            for outcome in ActivationOutcome::iter() {
                self.activations_total
                    .with_label_values(&[flow_type, outcome.into()])
                    .reset();
            }
        }
    }

    pub(crate) fn on_activation(
        &self,
        flow_type: &str,
        result: &Result<(), ActivateFlowError>,
        activation_time: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        let outcome = match result {
            Ok(()) => {
                self.activation_delay_seconds
                    .observe(seconds_between(activation_time, now));
                ActivationOutcome::Activated
            }
            Err(ActivateFlowError::ConcurrentModification) => ActivationOutcome::Skipped,
            Err(ActivateFlowError::Internal(_)) => ActivationOutcome::Failed,
        };

        self.activations_total
            .with_label_values(&[flow_type, outcome.into()])
            .inc();
    }
}

impl MetricsProvider for FlowAgentMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.activations_total.clone()))?;
        reg.register(Box::new(self.activation_delay_seconds.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
