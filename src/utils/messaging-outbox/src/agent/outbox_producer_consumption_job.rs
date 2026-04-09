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
use internal_error::{InternalError, ResultIntoInternal};
use tracing::Instrument;

use super::{OutboxAgentMetrics, OutboxRoutesStaticInfo, ProducerConsumptionTask};
use crate::{
    ConsumerFilter,
    MessageConsumptionMode,
    MessageDispatcher,
    OutboxMessage,
    OutboxMessageBoundary,
    OutboxMessageBridge,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type ProcessedConsumerTasksCount = usize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ProducerConsumptionJob {
    catalog: dill::CatalogWeakRef,
    routes_static_info: Arc<OutboxRoutesStaticInfo>,
    producer_name: String,
    consumer_names: Vec<String>,
    failed_consumer_names: Mutex<HashSet<String>>,
    metrics: Arc<OutboxAgentMetrics>,
}

impl ProducerConsumptionJob {
    pub(crate) fn new(
        catalog: dill::CatalogWeakRef,
        routes_static_info: Arc<OutboxRoutesStaticInfo>,
        producer_name: String,
        consumer_names: Vec<String>,
        metrics: Arc<OutboxAgentMetrics>,
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

    pub(crate) fn failed_consumer_names_snapshot(&self) -> HashSet<String> {
        let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        g_failed_consumer_names.clone()
    }

    pub(crate) fn all_consumers_failing(&self) -> bool {
        let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        assert!(g_failed_consumer_names.len() <= self.consumer_names.len());
        g_failed_consumer_names.len() == self.consumer_names.len()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(
        producer_name = %self.producer_name
    ))]
    pub(crate) async fn run_consumption_task(
        &self,
        consumption_task: ProducerConsumptionTask,
    ) -> Result<ProcessedConsumerTasksCount, InternalError> {
        // Clone names of failing consumers before this iteration.
        let mut permanently_failed_consumer_names = {
            let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
            g_failed_consumer_names.clone()
        };
        let mut blocked_consumer_names = permanently_failed_consumer_names.clone();

        let mut processed_consumer_tasks_count = 0;

        // Feed consumers if they are behind this message
        // We must respect the sequential order of messages,
        // but individual consumers may process each message concurrently
        for message in consumption_task.unconsumed_messages {
            // Prepare consumer invocation tasks
            let mut consumer_tasks = Vec::new();
            for consumer_name in &self.consumer_names {
                // Skip consumers, which are failing
                if blocked_consumer_names.contains(consumer_name) {
                    continue;
                }

                // Skip consumers, which are already beyond this message boundary
                let consumer_boundary = consumption_task
                    .consumption_boundaries_by_consumer
                    .get(consumer_name)
                    .copied()
                    .unwrap_or_default();

                let message_boundary = OutboxMessageBoundary {
                    message_id: message.message_id,
                    tx_id: message.tx_id,
                };

                if consumer_boundary < message_boundary {
                    // Non-failing consumer which hasn't seen this message is a task to execute
                    consumer_tasks.push((consumer_name.as_str(), &message));
                }
            }

            // Create individual transaction objects
            let catalog = self.catalog.upgrade();
            let consume_message_tasks: Vec<ConsumeMessageTask> = consumer_tasks
                .into_iter()
                .map(|(consumer_name, message)| ConsumeMessageTask {
                    catalog: catalog.clone(),
                    consumer_name: consumer_name.to_string(),
                    message: Arc::clone(message),
                    consumption_mode: self
                        .routes_static_info
                        .consumer_consumption_mode(consumer_name),
                    dispatcher: Arc::clone(
                        self.routes_static_info
                            .message_dispatchers_by_producers
                            .get(&message.producer_name)
                            .expect("No dispatcher for producer"),
                    ),
                })
                .collect();

            // Spawn per-consumer message processing tasks as concurrent tokio tasks
            let mut join_set = tokio::task::JoinSet::new();
            for consume_message_task in consume_message_tasks {
                join_set.spawn(consume_message_task.invoke());
            }

            // Report errors and block consumers from advancing to later messages in
            // this iteration, as they cannot skip over a failed message.
            while let Some(res) = join_set.join_next().await {
                match res.int_err()? {
                    Ok(tx) => {
                        processed_consumer_tasks_count += 1;

                        self.metrics
                            .messages_processed_total
                            .with_label_values(&[&tx.message.producer_name, &tx.consumer_name])
                            .inc();
                    }
                    Err(err) => {
                        blocked_consumer_names.insert(err.task.consumer_name.clone());

                        self.metrics
                            .failed_consumers_total
                            .with_label_values(&[&self.producer_name, &err.task.consumer_name])
                            .set(1);

                        permanently_failed_consumer_names.insert(err.task.consumer_name);
                    }
                }
            }

            // If all consumers are blocked, time to interrupt this iteration
            assert!(blocked_consumer_names.len() <= self.consumer_names.len());
            if blocked_consumer_names.len() == self.consumer_names.len() {
                tracing::error!(
                    producer_name = self.producer_name,
                    message_id = ?message.message_id,
                    "Outbox consumption iteration interrupted, all consumers are blocked"
                );
                break;
            }
        }

        // Update list of failing consumer names for this job
        let mut g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        *g_failed_consumer_names = permanently_failed_consumer_names;

        Ok(processed_consumer_tasks_count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A wrapper that represents handling a single consumer/message pair and
/// advancing the consumer boundary after successful processing.
struct ConsumeMessageTask {
    catalog: dill::Catalog,
    consumer_name: String,
    message: Arc<OutboxMessage>,
    consumption_mode: MessageConsumptionMode,
    dispatcher: Arc<dyn MessageDispatcher>,
}

impl ConsumeMessageTask {
    /// This is a root-level processing task and is expected to run in a
    /// dedicated tokio task.
    async fn invoke(self) -> Result<Self, ConsumeMessageError> {
        let span = observability::tracing::root_span!(
            "Outbox::consume_message",
            producer = %self.message.producer_name,
            consumer = %self.consumer_name,
            message_id = %self.message.message_id,
            consumption_mode = ?self.consumption_mode,
        );

        match self.invoke_with_mode().instrument(span).await {
            Ok(()) => Ok(self),
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    consumer_name = %self.consumer_name,
                    outbox_message = ?self.message,
                    "Consuming outbox message failed"
                );

                Err(ConsumeMessageError {
                    task: self,
                    source: err,
                })
            }
        }
    }

    async fn invoke_with_mode(&self) -> Result<(), InternalError> {
        match self.consumption_mode {
            MessageConsumptionMode::Immediate => Err(InternalError::new(
                "Immediate consumers are not expected in durable outbox processing",
            )),
            MessageConsumptionMode::TransactionalWrapped => self.invoke_wrapped().await,
            MessageConsumptionMode::TransactionalSelfManaged => self.invoke_self_managed().await,
        }
    }

    #[transactional_method]
    async fn invoke_wrapped(&self) -> Result<(), InternalError> {
        tracing::debug!(
            outbox_message = ?self.message,
            "Consuming message"
        );

        let content_json = self.message.content_json.to_string();

        self.dispatcher
            .dispatch_message(
                &transaction_catalog,
                ConsumerFilter::SelectedConsumer(&self.consumer_name),
                &content_json,
                self.message.version,
            )
            .await?;

        // Shift consumption record regardless of whether the consumer was interested in
        // the message
        tracing::debug!(
            consumer_name = %self.consumer_name,
            producer_name = %self.message.producer_name,
            last_consumed_message_id = %self.message.message_id,
            "Shifting consumption record"
        );

        self.mark_consumed(&transaction_catalog).await?;

        Ok(())
    }

    async fn invoke_self_managed(&self) -> Result<(), InternalError> {
        tracing::debug!(
            outbox_message = ?self.message,
            "Consuming message without framework transaction"
        );

        let content_json = self.message.content_json.to_string();

        self.dispatcher
            .dispatch_message(
                &self.catalog,
                ConsumerFilter::SelectedConsumer(&self.consumer_name),
                &content_json,
                self.message.version,
            )
            .await?;

        self.mark_consumed_transactionally().await
    }

    #[transactional_method]
    async fn mark_consumed_transactionally(&self) -> Result<(), InternalError> {
        tracing::debug!(
            consumer_name = %self.consumer_name,
            producer_name = %self.message.producer_name,
            last_consumed_message_id = %self.message.message_id,
            "Shifting consumption record"
        );

        self.mark_consumed(&transaction_catalog).await?;

        Ok(())
    }

    async fn mark_consumed(&self, target_catalog: &dill::Catalog) -> Result<(), InternalError> {
        let outbox_message_bridge = target_catalog.get_one::<dyn OutboxMessageBridge>().unwrap();

        outbox_message_bridge
            .mark_consumed(
                target_catalog,
                &self.message.producer_name,
                &self.consumer_name,
                OutboxMessageBoundary {
                    message_id: self.message.message_id,
                    tx_id: self.message.tx_id,
                },
            )
            .await
            .int_err()
    }
}

impl std::fmt::Debug for ConsumeMessageTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumeMessageTask")
            .field("consumer_name", &self.consumer_name)
            .field("message", &self.message)
            .finish_non_exhaustive()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("Consuming outbox message failed")]
struct ConsumeMessageError {
    pub task: ConsumeMessageTask,
    pub source: InternalError,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
