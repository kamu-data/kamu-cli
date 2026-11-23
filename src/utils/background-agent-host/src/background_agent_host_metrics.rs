// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use dill::{Singleton, component, interface, scope};
use observability::metrics::{MetricsProvider, unix_timestamp_seconds};
use strum::IntoEnumIterator as _;

use crate::BackgroundAgentRestartReason;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How the hosted agents run, by agent
pub struct BackgroundAgentHostMetrics {
    pub running: prometheus::IntGaugeVec,
    pub restarts_total: prometheus::IntCounterVec,
    pub last_failure_timestamp_seconds: prometheus::GaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl BackgroundAgentHostMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            running: IntGaugeVec::new(
                Opts::new(
                    "background_agent_running",
                    "1 while a background agent runs, 0 while it waits to be restarted or has \
                     stopped",
                ),
                &["agent"],
            )
            .unwrap(),
            restarts_total: IntCounterVec::new(
                Opts::new(
                    "background_agent_restarts_total",
                    "Number of times a background agent failed, panicked or returned early and \
                     was restarted, by agent and reason",
                ),
                &["agent", "reason"],
            )
            .unwrap(),
            last_failure_timestamp_seconds: GaugeVec::new(
                Opts::new(
                    "background_agent_last_failure_timestamp_seconds",
                    "Time when a background agent last failed or panicked; 0 if it never did",
                ),
                &["agent"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init<'a>(&self, agent_names: impl Iterator<Item = &'a str>) {
        for agent in agent_names {
            self.running.with_label_values(&[agent]);
            for reason in BackgroundAgentRestartReason::iter() {
                self.restarts_total
                    .with_label_values(&[agent, reason.into()]);
            }
            self.last_failure_timestamp_seconds
                .with_label_values(&[agent]);
        }
    }

    pub(crate) fn set_running(&self, agent: &str, running: bool) {
        self.running
            .with_label_values(&[agent])
            .set(i64::from(running));
    }

    pub(crate) fn on_restart(&self, agent: &str, reason: BackgroundAgentRestartReason) {
        self.restarts_total
            .with_label_values(&[agent, reason.into()])
            .inc();
    }

    pub(crate) fn on_failure(&self, agent: &str, at: DateTime<Utc>) {
        self.last_failure_timestamp_seconds
            .with_label_values(&[agent])
            .set(unix_timestamp_seconds(at));
    }
}

impl MetricsProvider for BackgroundAgentHostMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.running.clone()))?;
        reg.register(Box::new(self.restarts_total.clone()))?;
        reg.register(Box::new(self.last_failure_timestamp_seconds.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
