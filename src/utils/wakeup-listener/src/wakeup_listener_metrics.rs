// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::{SystemTime, UNIX_EPOCH};

use observability::metrics::MetricsProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WakeupListenerMetrics {
    pub last_heartbeat_timestamp_seconds: prometheus::GaugeVec,
}

#[dill::component(pub)]
#[dill::interface(dyn MetricsProvider)]
#[dill::scope(dill::Singleton)]
impl WakeupListenerMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            last_heartbeat_timestamp_seconds: GaugeVec::new(
                Opts::new(
                    "wakeup_listener_last_heartbeat_timestamp_seconds",
                    "Time when an agent's loop last proved alive: on start, around every wait for \
                     changes, and after every processed batch, so a stale value means a hung loop",
                ),
                &["agent"],
            )
            .unwrap(),
        }
    }

    pub(crate) fn record_heartbeat(&self, agent_name: &str) {
        // Wall clock, as waits run on real time on every backend
        let now_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        self.last_heartbeat_timestamp_seconds
            .with_label_values(&[agent_name])
            .set(now_seconds);
    }
}

impl MetricsProvider for WakeupListenerMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.last_heartbeat_timestamp_seconds.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
