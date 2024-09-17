// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use dill::*;
use observability::metrics::MetricsProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OutboxExecutorMetrics {
    pub messages_processed_total: prometheus::IntCounterVec,
    pub messages_pending_total: prometheus::IntGaugeVec,
    pub failed_consumers_total: prometheus::IntGaugeVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl OutboxExecutorMetrics {
    pub fn new() -> Self {
        use prometheus::*;

        Self {
            messages_processed_total: IntCounterVec::new(
                Opts::new(
                    "outbox_messages_processed_total",
                    "Number of messages processed by an individual producer-consumer pair",
                ),
                &["producer", "consumer"],
            )
            .unwrap(),
            messages_pending_total: IntGaugeVec::new(
                Opts::new(
                    "outbox_messages_pending_total",
                    "Number of messages that are awaiting processing in an individual \
                     producer-consumer pair",
                ),
                &["producer", "consumer"],
            )
            .unwrap(),
            failed_consumers_total: IntGaugeVec::new(
                Opts::new(
                    "outbox_failed_consumers_total",
                    "Number of consumers that are in the failed state and have stopped consuming \
                     messages",
                ),
                &["producer", "consumer"],
            )
            .unwrap(),
        }
    }

    /// Initializes labeled metrics so they show up in the output early
    pub(crate) fn init(&self, producer_consumers: &HashMap<String, Vec<String>>) {
        for (producer, consumers) in producer_consumers {
            for consumer in consumers {
                self.messages_processed_total
                    .with_label_values(&[producer, consumer])
                    .reset();

                self.messages_pending_total
                    .with_label_values(&[producer, consumer])
                    .set(0);

                self.failed_consumers_total
                    .with_label_values(&[producer, consumer])
                    .set(0);
            }
        }
    }
}

impl MetricsProvider for OutboxExecutorMetrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(self.messages_processed_total.clone()))?;
        reg.register(Box::new(self.messages_pending_total.clone()))?;
        reg.register(Box::new(self.failed_consumers_total.clone()))?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
