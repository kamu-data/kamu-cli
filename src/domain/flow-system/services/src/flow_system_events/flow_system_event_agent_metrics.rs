// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use observability::metrics::MetricsProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowSystemEventAgentMetrics {
    pub projector_failing: prometheus::IntGaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl FlowSystemEventAgentMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            projector_failing: IntGaugeVec::new(
                Opts::new(
                    "flow_system_event_projector_failing",
                    "1 while the last attempt to apply flow system events to a projection failed; \
                     the projection makes no progress until a retry on a later wakeup succeeds",
                ),
                &["projector"],
            )
            .unwrap(),
        }
    }

    pub(crate) fn on_projector_batch<T, E>(&self, projector_name: &str, result: &Result<T, E>) {
        self.projector_failing
            .with_label_values(&[projector_name])
            .set(i64::from(result.is_err()));
    }
}

impl MetricsProvider for FlowSystemEventAgentMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.projector_failing.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
