// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use internal_error::InternalError;

use super::{
    OutboxAgentMetrics,
    OutboxRoutesStaticInfo,
    ProducerConsumptionTask,
    UnconsumedProducerState,
};
use crate::{
    OutboxMessage,
    OutboxMessageConsumptionRepository,
    OutboxMessageID,
    OutboxMessageRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct OutboxConsumptionIterationPlanner {
    routes_static_info: Arc<OutboxRoutesStaticInfo>,
    outbox_message_repository: Arc<dyn OutboxMessageRepository>,
    outbox_message_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>,
    metrics: Arc<OutboxAgentMetrics>,
    messages_batch_size: usize,
}

impl OutboxConsumptionIterationPlanner {
    pub(crate) fn new(
        routes_static_info: Arc<OutboxRoutesStaticInfo>,
        outbox_message_repository: Arc<dyn OutboxMessageRepository>,
        outbox_message_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>,
        metrics: Arc<OutboxAgentMetrics>,
        messages_batch_size: usize,
    ) -> Self {
        Self {
            routes_static_info,
            outbox_message_repository,
            outbox_message_consumption_repository,
            metrics,
            messages_batch_size,
        }
    }

    pub(crate) async fn plan_consumption_tasks_by_producer(
        &self,
    ) -> Result<HashMap<String, ProducerConsumptionTask>, InternalError> {
        // producer A - message 17
        // producer B - message 19
        let latest_message_ids_by_producer = self.select_latest_message_ids_by_producer().await?;

        // producer A ->
        //   consumer X -> message 15
        //   consumer Y -> message 14
        // producer B ->
        //   consumer X -> message 12
        let consumption_boundaries_by_producer = self
            .select_latest_consumption_boundaries_by_producer()
            .await?;

        // producer A: >14
        // producer B: >12
        let unconsumed_states_by_producer = self.compute_unconsumed_state_by_producer(
            latest_message_ids_by_producer,
            consumption_boundaries_by_producer,
        );
        if unconsumed_states_by_producer.is_empty() {
            return Ok(HashMap::new());
        }

        // Load portion of unprocessed messages, as specified in filters
        //   producer A: >14
        //   producer B: >12
        let unprocessed_messages_by_producer = self
            .load_unprocessed_messages_by_producer(&unconsumed_states_by_producer)
            .await?;

        // Compose consumption tasks
        Ok(self.compose_producer_consumption_tasks(
            unprocessed_messages_by_producer,
            unconsumed_states_by_producer,
        ))
    }

    async fn select_latest_message_ids_by_producer(
        &self,
    ) -> Result<HashMap<String, OutboxMessageID>, InternalError> {
        // Extract latest (producer, max message id) relation
        let latest_message_ids_by_producer = self
            .outbox_message_repository
            .get_latest_message_ids_by_producer()
            .await?;

        // Convert into map
        Ok(latest_message_ids_by_producer
            .into_iter()
            .collect::<HashMap<_, _>>())
    }

    async fn select_latest_consumption_boundaries_by_producer(
        &self,
    ) -> Result<HashMap<String, HashMap<String, OutboxMessageID>>, InternalError> {
        // Extract consumption boundaries for all routes
        let mut all_boundaries = async move {
            let consumptions_stream = self
                .outbox_message_consumption_repository
                .list_consumption_boundaries();

            use futures::TryStreamExt;
            consumptions_stream.try_collect::<Vec<_>>().await
        }
        .await?;

        // Organize by producer->consumer hierarchically
        use itertools::Itertools;
        all_boundaries.sort();
        let boundaries_by_producer = all_boundaries
            .into_iter()
            .chunk_by(|b| b.producer_name.clone())
            .into_iter()
            .map(|(producer_name, producer_boundaries)| {
                (
                    producer_name.clone(),
                    producer_boundaries
                        .map(|b| (b.consumer_name.clone(), b.last_consumed_message_id))
                        .collect(),
                )
            })
            .collect();

        Ok(boundaries_by_producer)
    }

    fn compute_unconsumed_state_by_producer(
        &self,
        latest_message_ids_by_producer: HashMap<String, OutboxMessageID>,
        mut consumption_boundaries_by_producer: HashMap<String, HashMap<String, OutboxMessageID>>,
    ) -> HashMap<String, UnconsumedProducerState> {
        let mut unconsumed_states_by_producers = HashMap::new();
        for (producer_name, latest_produced_message_id) in latest_message_ids_by_producer {
            // Take consumption boundaries for this producer
            let Some(consumption_boundaries_by_consumer) =
                consumption_boundaries_by_producer.remove(&producer_name)
            else {
                continue;
            };

            // Extract list of consumers for this producer
            let Some(consumer_names) = self
                .routes_static_info
                .consumers_by_producers
                .get(&producer_name)
            else {
                continue;
            };

            // Report queue length metrics
            for (consumer, last_consumed_message_id) in &consumption_boundaries_by_consumer {
                let queue_length =
                    latest_produced_message_id.into_inner() - last_consumed_message_id.into_inner();
                self.metrics
                    .messages_pending_total
                    .with_label_values(&[&producer_name, consumer])
                    .set(queue_length);
            }

            // Determine the earliest message ID that was processed by consumers
            let maybe_processed_boundary_id = self.determine_processed_boundary_id(
                consumer_names,
                &consumption_boundaries_by_consumer,
            );

            // Was there an advancement?
            if let Some(processed_boundary_id) = maybe_processed_boundary_id
                && processed_boundary_id < latest_produced_message_id
            {
                tracing::debug!(
                    %producer_name,
                    %processed_boundary_id,
                    "Selected unsatisfied boundary message for producer",
                );

                let consumption_state = UnconsumedProducerState {
                    processed_boundary_id,
                    consumption_boundaries_by_consumer,
                };

                unconsumed_states_by_producers.insert(producer_name, consumption_state);
            }
        }

        unconsumed_states_by_producers
    }

    fn determine_processed_boundary_id(
        &self,
        consumer_names: &[String],
        consumption_boundaries: &HashMap<String, OutboxMessageID>,
    ) -> Option<OutboxMessageID> {
        let mut earliest_seen_id: Option<OutboxMessageID> = None;

        for consumer in consumer_names {
            if let Some(boundary_id) = consumption_boundaries.get(consumer) {
                match earliest_seen_id {
                    Some(id) => {
                        if *boundary_id < id {
                            earliest_seen_id = Some(*boundary_id);
                        }
                    }
                    None => earliest_seen_id = Some(*boundary_id),
                }
            } else {
                // We are seeing a new consumer, a full synchronization is a must
                return Some(OutboxMessageID::new(0));
            }
        }

        earliest_seen_id
    }

    async fn load_unprocessed_messages_by_producer(
        &self,
        unconsumed_state_by_producer: &HashMap<String, UnconsumedProducerState>,
    ) -> Result<HashMap<String, Vec<Arc<OutboxMessage>>>, InternalError> {
        // Prepare filter to load messages with boundary by each producer
        let boundaries_by_producer: Vec<_> = unconsumed_state_by_producer
            .iter()
            .map(|(producer_name, consumption_state)| {
                (
                    producer_name.clone(),
                    consumption_state.processed_boundary_id,
                )
            })
            .collect();

        // Load batch of unprocessed messages, which satisfy filters
        use futures::TryStreamExt;
        let mut unprocessed_messages = self
            .outbox_message_repository
            .get_messages(boundaries_by_producer, self.messages_batch_size)
            .map_ok(Arc::new)
            .try_collect::<Vec<_>>()
            .await?;

        // Group messages by producers
        use itertools::Itertools;
        unprocessed_messages.sort_by(|m1, m2| m1.producer_name.cmp(&m2.producer_name));
        let unprocessed_messages_by_producer: HashMap<_, _> = unprocessed_messages
            .into_iter()
            .chunk_by(|m| m.producer_name.clone())
            .into_iter()
            .map(|(producer_name, producer_messages)| {
                (producer_name.clone(), producer_messages.collect())
            })
            .collect();

        Ok(unprocessed_messages_by_producer)
    }

    fn compose_producer_consumption_tasks(
        &self,
        unprocessed_messages_by_producer: HashMap<String, Vec<Arc<OutboxMessage>>>,
        mut unconsumed_state_by_producers: HashMap<String, UnconsumedProducerState>,
    ) -> HashMap<String, ProducerConsumptionTask> {
        let mut consumption_tasks_by_producers = HashMap::new();
        for (producer_name, unprocessed_messages) in unprocessed_messages_by_producer {
            let consumption_state = unconsumed_state_by_producers
                .remove(&producer_name)
                .expect("Consumption record must be present");

            consumption_tasks_by_producers.insert(
                producer_name,
                ProducerConsumptionTask {
                    unconsumed_messages: unprocessed_messages,
                    consumption_boundaries_by_consumer: consumption_state
                        .consumption_boundaries_by_consumer,
                },
            );
        }

        consumption_tasks_by_producers
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
