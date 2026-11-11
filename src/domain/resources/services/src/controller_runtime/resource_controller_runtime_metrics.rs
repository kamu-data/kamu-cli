// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Singleton, component, interface, scope};
use observability::metrics::MetricsProvider;
use strum::IntoEnumIterator as _;

use crate::controller_runtime::ResourceProcessAttemptKind;
use crate::domain::ResourceControllerName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What the controller runtime did, by controller
pub struct ResourceControllerRuntimeMetrics {
    pub processed_total: prometheus::IntCounterVec,
    pub loop_restarts_total: prometheus::IntCounterVec,
    pub failing_resources: prometheus::IntGaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl ResourceControllerRuntimeMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            processed_total: IntCounterVec::new(
                Opts::new(
                    "resource_controller_processed_total",
                    "Number of times a resource controller was asked about a resource, by \
                     controller and outcome",
                ),
                &["controller", "outcome"],
            )
            .unwrap(),
            loop_restarts_total: IntCounterVec::new(
                Opts::new(
                    "resource_controller_loop_restarts_total",
                    "Number of times the loop of a resource controller failed and was restarted",
                ),
                &["controller"],
            )
            .unwrap(),
            failing_resources: IntGaugeVec::new(
                Opts::new(
                    "resource_controller_failing_resources",
                    "Number of resources whose last processing by a resource controller failed, \
                     as of its last drain",
                ),
                &["controller"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init<'a>(
        &self,
        controller_names: impl Iterator<Item = &'a ResourceControllerName>,
    ) {
        for controller_name in controller_names {
            let controller = controller_name.as_str();
            for outcome in ResourceProcessAttemptKind::iter() {
                self.processed_total
                    .with_label_values(&[controller, outcome.into()]);
            }
            self.loop_restarts_total.with_label_values(&[controller]);
            self.failing_resources.with_label_values(&[controller]);
        }
    }

    pub(crate) fn on_processed(
        &self,
        controller_name: &ResourceControllerName,
        outcome: ResourceProcessAttemptKind,
    ) {
        self.processed_total
            .with_label_values(&[controller_name.as_str(), outcome.into()])
            .inc();
    }

    pub(crate) fn on_loop_restart(&self, controller_name: &ResourceControllerName) {
        self.loop_restarts_total
            .with_label_values(&[controller_name.as_str()])
            .inc();
    }

    pub(crate) fn set_failing_resources(
        &self,
        controller_name: &ResourceControllerName,
        count: usize,
    ) {
        self.failing_resources
            .with_label_values(&[controller_name.as_str()])
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

impl MetricsProvider for ResourceControllerRuntimeMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.processed_total.clone()))?;
        reg.register(Box::new(self.loop_restarts_total.clone()))?;
        reg.register(Box::new(self.failing_resources.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
