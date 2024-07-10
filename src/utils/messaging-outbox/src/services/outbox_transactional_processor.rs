// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use dill::{component, scope, Catalog, Singleton};
use internal_error::{InternalError, ResultIntoInternal};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxTransactionalProcessor {
    catalog: Catalog,
    config: Arc<OutboxConfig>,
    routes_static_info: Arc<RoutesStaticInfo>,
    producer_relay_jobs: Vec<ProducerRelayJob>,
}

#[component(pub)]
#[scope(Singleton)]
impl OutboxTransactionalProcessor {
    pub fn new(
        catalog: Catalog,
        config: Arc<OutboxConfig>,
        message_consumer_mediators: Vec<Arc<dyn MessageConsumersMediator>>,
    ) -> Self {
        let routes_static_info =
            Arc::new(Self::make_static_routes_info(message_consumer_mediators));

        let mut producer_relay_jobs = Vec::new();
        for (producer_name, consumer_names) in &routes_static_info.consumers_by_producers {
            producer_relay_jobs.push(ProducerRelayJob::new(
                catalog.clone(),
                config.clone(),
                routes_static_info.clone(),
                producer_name.clone(),
                consumer_names.clone(),
            ));
        }

        Self {
            catalog,
            config,
            routes_static_info,
            producer_relay_jobs,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn make_static_routes_info(
        message_consumer_mediators: Vec<Arc<dyn MessageConsumersMediator>>,
    ) -> RoutesStaticInfo {
        let all_messaging_routes = enumerate_all_messaging_routes(&message_consumer_mediators);
        let messages_by_consumer = group_messages_by_consumer(&all_messaging_routes);
        let consumers_by_producers = group_consumers_by_producers(&all_messaging_routes);
        let consumers_mediator_per_message_type =
            organize_consumers_mediators(&message_consumer_mediators);

        RoutesStaticInfo::new(
            consumers_mediator_per_message_type,
            all_messaging_routes,
            consumers_by_producers,
            messages_by_consumer,
        )
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run(&self) -> Result<(), InternalError> {
        // Trace current routes
        self.debug_message_routes();

        // Make sure consumption records represent the routes
        self.init_consumption_records().await?;

        // Main relay loop
        loop {
            self.run_relay_iteration().await?;

            tracing::debug!("Awaiting next iteration");
            tokio::time::sleep(self.config.awaiting_step.to_std().unwrap()).await;
        }
    }

    fn debug_message_routes(&self) {
        for messaging_route in &self.routes_static_info.all_messaging_routes {
            tracing::debug!("{messaging_route}");
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn init_consumption_records(&self) -> Result<(), InternalError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>| async move {
                    // Load existing consumption records
                    use futures::TryStreamExt;
                    let consumptions = outbox_consumption_repository
                        .list_consumption_boundaries()
                        .await?
                        .try_collect::<Vec<_>>().await?;

                    // Build a set of producer-consumer pairs that already exist in the database
                    let mut matched_consumptions = HashSet::new();
                    for consumption in &consumptions {
                        matched_consumptions.insert((&consumption.producer_name, &consumption.consumer_name));
                    }

                    // Detect new routes, which are not associated with a consumption record yet
                    for (producer_name, consumer_names) in &self.routes_static_info.consumers_by_producers {
                        for consumer_name in consumer_names {
                            if !matched_consumptions.contains(&(producer_name, consumer_name)) {
                                // Create a new consumption boundary
                                outbox_consumption_repository.create_consumption_boundary(OutboxMessageConsumptionBoundary {
                                    consumer_name: consumer_name.clone(),
                                    producer_name: producer_name.clone(),
                                    last_consumed_message_id: OutboxMessageID::new(0)
                                }).await.int_err()?;
                            }
                        }
                    }

                    Ok(())
                },
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_relay_iteration(&self) -> Result<(), InternalError> {
        // producer A - message 17
        // producer B - message 19
        let latest_message_ids_by_producer = self.select_latest_message_ids_by_producers().await?;

        // producer A ->
        //   consumer X -> message 15
        //   consumer Y -> message 14
        // producer B ->
        //   consumer X -> message 12
        let mut consumption_boundaries_by_producer = self
            .select_latest_consumption_boundaries_by_producers()
            .await?;

        // TODO: each producer job can be executed concurrently
        for producer_relay_job in &self.producer_relay_jobs {
            // Extract latest message ID by producer
            let Some(latest_produced_message_id) =
                latest_message_ids_by_producer.get(&producer_relay_job.producer_name)
            else {
                continue;
            };

            // Take consumption boundaries for this producer
            let Some(consumption_boundaries) =
                consumption_boundaries_by_producer.remove(&producer_relay_job.producer_name)
            else {
                continue;
            };

            // Run relay job
            producer_relay_job
                .run_iteration(*latest_produced_message_id, consumption_boundaries)
                .await?;
        }

        Ok(())
    }

    async fn select_latest_message_ids_by_producers(
        &self,
    ) -> Result<HashMap<String, OutboxMessageID>, InternalError> {
        // Extract latest (producer, max message id) relation
        let latest_message_ids_by_producer = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |outbox_message_repository: Arc<dyn OutboxMessageRepository>| async move {
                    outbox_message_repository
                        .get_latest_message_ids_by_producer()
                        .await
                },
            )
            .await
            .unwrap();

        // Convert into map
        Ok(latest_message_ids_by_producer
            .into_iter()
            .collect::<HashMap<_, _>>())
    }

    async fn select_latest_consumption_boundaries_by_producers(
        &self,
    ) -> Result<HashMap<String, HashMap<String, OutboxMessageID>>, InternalError> {
        // Extract consumption boundaries for all routes
        let all_boundaries = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>| async move {
                    let consumptions_stream = outbox_consumption_repository
                        .list_consumption_boundaries()
                        .await?;

                        use futures::TryStreamExt;
                        consumptions_stream.try_collect::<Vec<_>>().await
                },
            )
            .await
            .unwrap();

        // Organize by producer->consumer hierarchically
        let mut boundaries_by_producer = HashMap::new();
        for boundary in all_boundaries {
            boundaries_by_producer
                .entry(boundary.producer_name)
                .and_modify(|by_consumer: &mut HashMap<String, OutboxMessageID>| {
                    by_consumer.insert(
                        boundary.consumer_name.clone(),
                        boundary.last_consumed_message_id,
                    );
                })
                .or_insert_with(|| {
                    HashMap::from([(boundary.consumer_name, boundary.last_consumed_message_id)])
                });
        }

        Ok(boundaries_by_producer)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RoutesStaticInfo {
    consumers_mediator_per_message_type: HashMap<String, Arc<dyn MessageConsumersMediator>>,
    all_messaging_routes: Vec<MessageConsumerMetaInfoRow>,
    consumers_by_producers: HashMap<String, Vec<String>>,
    messages_by_consumer: HashMap<String, HashSet<String>>,
}

impl RoutesStaticInfo {
    fn new(
        consumers_mediator_per_message_type: HashMap<String, Arc<dyn MessageConsumersMediator>>,
        all_messaging_routes: Vec<MessageConsumerMetaInfoRow>,
        consumers_by_producers: HashMap<String, Vec<String>>,
        messages_by_consumer: HashMap<String, HashSet<String>>,
    ) -> Self {
        Self {
            consumers_mediator_per_message_type,
            all_messaging_routes,
            consumers_by_producers,
            messages_by_consumer,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ProducerRelayJob {
    catalog: Catalog,
    config: Arc<OutboxConfig>,
    relay_routes_static_info: Arc<RoutesStaticInfo>,
    producer_name: String,
    consumer_names: Vec<String>,
}

impl ProducerRelayJob {
    fn new(
        catalog: Catalog,
        config: Arc<OutboxConfig>,
        relay_routes_static_info: Arc<RoutesStaticInfo>,
        producer_name: String,
        consumer_names: Vec<String>,
    ) -> Self {
        Self {
            catalog,
            config,
            relay_routes_static_info,
            producer_name,
            consumer_names,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(latest_produced_message_id))]
    async fn run_iteration(
        &self,
        latest_produced_message_id: OutboxMessageID,
        consumption_boundaries: HashMap<String, OutboxMessageID>,
    ) -> Result<(), InternalError> {
        // Decide on the earliest message that was processed by all of the consumers
        let maybe_processed_boundary_id =
            self.determine_processed_boundary_id(&consumption_boundaries);

        tracing::debug!(
            "Processed boundary for producer '{}' is {:?}",
            self.producer_name,
            maybe_processed_boundary_id
        );

        // Was there an advancement?
        if let Some(processed_boundary_id) = maybe_processed_boundary_id
            && processed_boundary_id < latest_produced_message_id
        {
            // Load all messages after the earliest
            let unprocessed_messages = self
                .load_messages_above(
                    processed_boundary_id,
                    usize::try_from(self.config.batch_size).unwrap(),
                )
                .await?;

            // Feed consumers if they are behind this message
            for message in unprocessed_messages {
                for consumer_name in &self.consumer_names {
                    let boundary_id = consumption_boundaries
                        .get(consumer_name)
                        .copied()
                        .unwrap_or_else(|| OutboxMessageID::new(0));

                    if boundary_id <= message.message_id {
                        self.invoke_consumer(consumer_name, message.clone()).await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn determine_processed_boundary_id(
        &self,
        consumption_boundaries: &HashMap<String, OutboxMessageID>,
    ) -> Option<OutboxMessageID> {
        let mut earliest_seen_id: Option<OutboxMessageID> = None;

        for consumer in &self.consumer_names {
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

    #[tracing::instrument(level = "debug", skip_all, fields(above_id, batch_size))]
    async fn load_messages_above(
        &self,
        above_id: OutboxMessageID,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |outbox_message_repository: Arc<dyn OutboxMessageRepository>| async move {
                    let messages_stream = outbox_message_repository
                        .get_producer_messages(&self.producer_name, above_id, batch_size)
                        .await?;

                    use futures::TryStreamExt;
                    messages_stream.try_collect::<Vec<_>>().await
                },
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(consumer_name, ?message, is_first_consumption))]
    async fn invoke_consumer(
        &self,
        consumer_name: &str,
        message: OutboxMessage,
    ) -> Result<(), InternalError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional(|transaction_catalog| async move {
                // Detect if particular consumer needs this type of message
                // (as it may skip some of the unwanted producer's messages)
                let is_supported_message = self
                    .relay_routes_static_info
                    .messages_by_consumer
                    .get(consumer_name)
                    .is_some_and(|messages| messages.contains(&message.message_type));

                // If it's supported, deliver it to consumer
                if is_supported_message {
                    let mediator = self
                        .relay_routes_static_info
                        .consumers_mediator_per_message_type
                        .get(&message.message_type)
                        .expect("No mediator for message type");

                    mediator
                        .consume_message(
                            &transaction_catalog,
                            ConsumerFilter::SelectedConsumer(consumer_name),
                            &message.message_type,
                            message.content_json.clone(),
                        )
                        .await?;
                }

                // Shift consumption record regardless of whether the consumer was interested in
                // the message
                let consumption_repository = transaction_catalog
                    .get_one::<dyn OutboxMessageConsumptionRepository>()
                    .unwrap();
                consumption_repository
                    .update_consumption_boundary(OutboxMessageConsumptionBoundary {
                        consumer_name: consumer_name.to_string(),
                        producer_name: message.producer_name,
                        last_consumed_message_id: message.message_id,
                    })
                    .await
                    .int_err()?;

                Ok(())
            })
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
