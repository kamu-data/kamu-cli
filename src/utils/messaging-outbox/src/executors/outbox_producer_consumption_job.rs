// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use database_common_macros::transactional_method;
use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};

use super::{OutboxExecutorMetrics, OutboxRoutesStaticInfo, ProducerConsumptionTask};
use crate::{
    ConsumerFilter,
    OutboxMessage,
    OutboxMessageConsumptionBoundary,
    OutboxMessageConsumptionRepository,
    OutboxMessageID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ProducerConsumptionJob {
    catalog: Catalog,
    routes_static_info: Arc<OutboxRoutesStaticInfo>,
    producer_name: String,
    consumer_names: Vec<String>,
    failed_consumer_names: Mutex<HashSet<String>>,
    metrics: Arc<OutboxExecutorMetrics>,
}

impl ProducerConsumptionJob {
    pub(crate) fn new(
        catalog: Catalog,
        routes_static_info: Arc<OutboxRoutesStaticInfo>,
        producer_name: String,
        consumer_names: Vec<String>,
        metrics: Arc<OutboxExecutorMetrics>,
    ) -> Self {
        Self {
            catalog,
            routes_static_info,
            producer_name,
            consumer_names,
            metrics,
            failed_consumer_names: Mutex::new(HashSet::new()),
        }
    }

    #[inline]
    pub(crate) fn get_producer_name(&self) -> &str {
        &self.producer_name
    }

    pub(crate) fn all_consumers_failing(&self) -> bool {
        let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        assert!(g_failed_consumer_names.len() <= self.consumer_names.len());
        g_failed_consumer_names.len() == self.consumer_names.len()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(
        producer_name=%self.producer_name
    ))]
    pub(crate) async fn run_consumption_task(
        &self,
        consumption_task: ProducerConsumptionTask,
    ) -> Result<(), InternalError> {
        // Clone names of failing consumers before this iteration.
        let mut failing_consumer_names = {
            let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
            g_failed_consumer_names.clone()
        };

        // Feed consumers if they are behind this message
        // We must respect the sequential order of messages,
        // but individual consumers may process each message concurrently
        for message in consumption_task.unconsumed_messages {
            // Prepare consumer invocation tasks
            let mut consumer_tasks = Vec::new();
            for consumer_name in &self.consumer_names {
                // Skip consumers, which are failing
                if failing_consumer_names.contains(consumer_name) {
                    continue;
                }

                // Skip consumers, which are already beyond this message ID
                let boundary_id = consumption_task
                    .consumption_boundaries_by_consumer
                    .get(consumer_name)
                    .copied()
                    .unwrap_or_else(|| OutboxMessageID::new(0));
                if boundary_id < message.message_id {
                    // Non-failing consumer which hasn't seen this message is a task to execute
                    consumer_tasks.push((consumer_name.as_str(), &message));
                }
            }

            struct OutboxMessageConsumptionError {
                source: InternalError,
                consumer_name: String,
            }

            // Consume message concurrently
            let consumption_futures: Vec<_> = consumer_tasks
                .into_iter()
                .map(|(consumer_name, message)| async move {
                    self.invoke_consumer(consumer_name, message)
                        .await
                        .map_err(|e| OutboxMessageConsumptionError {
                            consumer_name: consumer_name.to_owned(),
                            source: e,
                        })
                })
                .collect();

            // Report errors and disable failing consumers, as they cannot advance to the
            // next messages without success of processing the current one
            futures::future::join_all(consumption_futures)
                .await
                .into_iter()
                .filter_map(Result::err)
                .for_each(|e: OutboxMessageConsumptionError| {
                    tracing::error!(
                        error = ?e.source,
                        error_msg = %e.source,
                        consumer_name = %e.consumer_name,
                        outbox_message = ?message,
                        "Consuming outbox message failed \
                            - pausing further message processing for consumer until restart."
                    );

                    self.metrics
                        .failed_consumers_total
                        .with_label_values(&[&self.producer_name, &e.consumer_name])
                        .set(1);

                    failing_consumer_names.insert(e.consumer_name);
                });

            // If all consumers are failing, time to interrupt this iteration
            assert!(failing_consumer_names.len() <= self.consumer_names.len());
            if failing_consumer_names.len() == self.consumer_names.len() {
                tracing::error!(
                    producer_name = self.producer_name,
                    message_id = ?message.message_id,
                    "Outbox consumption iteration interrupted, all consumers are failing"
                );
                break;
            }
        }

        // Update list of failing consumer names for this job
        let mut g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        *g_failed_consumer_names = failing_consumer_names;

        Ok(())
    }

    #[transactional_method]
    async fn invoke_consumer(
        &self,
        consumer_name: &str,
        message: &OutboxMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(consumer_name=%consumer_name, received_message=?message, "Invoking consumer");

        let dispatcher = self
            .routes_static_info
            .message_dispatchers_by_producers
            .get(&message.producer_name)
            .expect("No dispatcher for producer");

        let content_json = message.content_json.to_string();

        dispatcher
            .dispatch_message(
                &transaction_catalog,
                ConsumerFilter::SelectedConsumer(consumer_name),
                &content_json,
            )
            .await?;

        // Shift consumption record regardless of whether the consumer was interested in
        // the message
        tracing::debug!(
            consumer_name = %consumer_name,
            producer_name = %message.producer_name,
            last_consumed_message_id = %message.message_id,
            "Shifting consumption record"
        );

        // Report consumption metric
        self.metrics
            .messages_processed_total
            .with_label_values(&[&self.producer_name, consumer_name])
            .inc();

        let consumption_repository = transaction_catalog
            .get_one::<dyn OutboxMessageConsumptionRepository>()
            .unwrap();
        consumption_repository
            .update_consumption_boundary(OutboxMessageConsumptionBoundary {
                consumer_name: consumer_name.to_string(),
                producer_name: message.producer_name.to_string(),
                last_consumed_message_id: message.message_id,
            })
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
