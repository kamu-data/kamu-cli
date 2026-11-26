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

use crate::HousekeepingBatchReport;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct HousekeepingMetrics {
    pub items_removed_total: prometheus::IntCounterVec,
    pub backlog: prometheus::IntGaugeVec,
    pub last_pass_timestamp_seconds: prometheus::GaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl HousekeepingMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            items_removed_total: IntCounterVec::new(
                Opts::new(
                    "housekeeping_items_removed_total",
                    "Number of items housekeeping jobs removed, by job and kind",
                ),
                &["job", "kind"],
            )
            .unwrap(),
            backlog: IntGaugeVec::new(
                Opts::new(
                    "housekeeping_backlog",
                    "Items a housekeeping job found still due after its last batch",
                ),
                &["job"],
            )
            .unwrap(),
            last_pass_timestamp_seconds: GaugeVec::new(
                Opts::new(
                    "housekeeping_last_pass_timestamp_seconds",
                    "Time when a housekeeping agent last finished a pass, leaving no backlog; 0 \
                     until it does",
                ),
                &["job"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init(&self, job: &str) {
        self.backlog.with_label_values(&[job]);
        self.last_pass_timestamp_seconds.with_label_values(&[job]);
    }

    pub(crate) fn on_batch(&self, job: &str, report: &HousekeepingBatchReport) {
        for (kind, removed) in &report.removed_by_kind {
            self.items_removed_total
                .with_label_values(&[job, kind.as_str()])
                .inc_by(*removed);
        }
        self.backlog
            .with_label_values(&[job])
            .set(i64::try_from(report.backlog).unwrap_or(i64::MAX));
    }

    pub(crate) fn on_pass(&self, job: &str, at: DateTime<Utc>) {
        self.last_pass_timestamp_seconds
            .with_label_values(&[job])
            .set(unix_timestamp_seconds(at));
    }
}

impl MetricsProvider for HousekeepingMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.items_removed_total.clone()))?;
        reg.register(Box::new(self.backlog.clone()))?;
        reg.register(Box::new(self.last_pass_timestamp_seconds.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
