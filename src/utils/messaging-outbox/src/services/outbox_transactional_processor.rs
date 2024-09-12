// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use database_common_macros::{transactional_method, transactional_method1};
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
        message_dispatchers_by_producers: Vec<Arc<dyn MessageDispatcher>>,
    ) -> Self {
        let routes_static_info = Arc::new(Self::make_static_routes_info(
            &catalog,
            message_dispatchers_by_producers,
        ));

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
        catalog: &Catalog,
        message_dispatchers: Vec<Arc<dyn MessageDispatcher>>,
    ) -> RoutesStaticInfo {
        let all_durable_messaging_routes =
            enumerate_messaging_routes(catalog, MessageConsumptionDurability::Durable);
        let consumers_by_producers = group_consumers_by_producers(&all_durable_messaging_routes);
        let message_dispatchers_by_producers =
            group_message_dispatchers_by_producer(&message_dispatchers);

        RoutesStaticInfo::new(
            message_dispatchers_by_producers,
            all_durable_messaging_routes,
            consumers_by_producers,
        )
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn pre_run(&self) -> Result<(), InternalError> {
        // Trace current routes
        self.debug_message_routes();

        // Make sure consumption records represent the routes
        self.init_consumption_records().await
    }

    pub async fn run(&self) -> Result<(), InternalError> {
        // Main relay loop
        loop {
            self.run_relay_iteration().await?;
            tokio::time::sleep(self.config.awaiting_step.to_std().unwrap()).await;
        }
    }

    // To be used by tests only!
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_single_iteration_only(&self) -> Result<(), InternalError> {
        // Run single iteration instead of a loop
        self.run_relay_iteration().await?;
        Ok(())
    }

    fn debug_message_routes(&self) {
        for messaging_route in &self.routes_static_info.all_durable_messaging_routes {
            tracing::debug!("{messaging_route}");
        }
    }

    #[transactional_method1(outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>)]
    async fn init_consumption_records(&self) -> Result<(), InternalError> {
        // Load existing consumption records
        use futures::TryStreamExt;
        let consumptions = outbox_consumption_repository
            .list_consumption_boundaries()
            .try_collect::<Vec<_>>()
            .await?;

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
                    outbox_consumption_repository
                        .create_consumption_boundary(OutboxMessageConsumptionBoundary {
                            consumer_name: consumer_name.clone(),
                            producer_name: producer_name.clone(),
                            last_consumed_message_id: OutboxMessageID::new(0),
                        })
                        .await
                        .int_err()?;
                }
            }
        }

        Ok(())
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

        // Prepare iteration for each producer
        let mut producer_tasks = Vec::new();
        for producer_relay_job in &self.producer_relay_jobs {
            // Skip this relay if no more working consumers left
            if producer_relay_job.all_consumers_failing() {
                continue;
            }

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

            producer_tasks.push((
                producer_relay_job,
                latest_produced_message_id,
                consumption_boundaries,
            ));
        }

        // Run relay jobs of each producer concurrently
        use futures::{StreamExt, TryStreamExt};
        futures::stream::iter(producer_tasks)
            .map(Ok)
            .try_for_each_concurrent(/* limit */ None, |producer_task| async move {
                producer_task
                    .0
                    .run_iteration(*producer_task.1, producer_task.2)
                    .await
            })
            .await?;

        Ok(())
    }

    #[transactional_method1(outbox_message_repository: Arc<dyn OutboxMessageRepository>)]
    async fn select_latest_message_ids_by_producers(
        &self,
    ) -> Result<HashMap<String, OutboxMessageID>, InternalError> {
        // Extract latest (producer, max message id) relation
        let latest_message_ids_by_producer = outbox_message_repository
            .get_latest_message_ids_by_producer()
            .await?;

        // Convert into map
        Ok(latest_message_ids_by_producer
            .into_iter()
            .collect::<HashMap<_, _>>())
    }

    #[transactional_method1(outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>)]
    async fn select_latest_consumption_boundaries_by_producers(
        &self,
    ) -> Result<HashMap<String, HashMap<String, OutboxMessageID>>, InternalError> {
        // Extract consumption boundaries for all routes
        let all_boundaries = async move {
            let consumptions_stream = outbox_consumption_repository.list_consumption_boundaries();

            use futures::TryStreamExt;
            consumptions_stream.try_collect::<Vec<_>>().await
        }
        .await?;

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
    message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
    all_durable_messaging_routes: Vec<MessageSubscription>,
    consumers_by_producers: HashMap<String, Vec<String>>,
}

impl RoutesStaticInfo {
    fn new(
        message_dispatchers_by_producers: HashMap<String, Arc<dyn MessageDispatcher>>,
        all_durable_messaging_routes: Vec<MessageSubscription>,
        consumers_by_producers: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            message_dispatchers_by_producers,
            all_durable_messaging_routes,
            consumers_by_producers,
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
    failed_consumer_names: Mutex<HashSet<String>>,
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
            failed_consumer_names: Mutex::new(HashSet::new()),
        }
    }

    fn all_consumers_failing(&self) -> bool {
        let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        assert!(g_failed_consumer_names.len() <= self.consumer_names.len());
        g_failed_consumer_names.len() == self.consumer_names.len()
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%latest_produced_message_id, producer_name=%self.producer_name))]
    async fn run_iteration(
        &self,
        latest_produced_message_id: OutboxMessageID,
        consumption_boundaries: HashMap<String, OutboxMessageID>,
    ) -> Result<(), InternalError> {
        // Clone names of failing consumers before this iteration.
        let mut failing_consumer_names = {
            let g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
            g_failed_consumer_names.clone()
        };

        // Decide on the earliest message that was processed by all of the consumers
        let maybe_processed_boundary_id =
            self.determine_processed_boundary_id(&consumption_boundaries);

        tracing::debug!(
            "Selected boundary message for producer '{}' is {:?}",
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
            // We must respect the sequential order of messages,
            // but individual consumers may process each message concurrently
            for message in unprocessed_messages {
                // Prepare consumer invocation tasks
                let mut consumer_tasks = Vec::new();
                for consumer_name in &self.consumer_names {
                    // Skip consumers, which are failing
                    if failing_consumer_names.contains(consumer_name) {
                        continue;
                    }

                    // Skip consumers, which are already beyond this message ID
                    let boundary_id = consumption_boundaries
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
                            "Consuming outbox message failed - pausing further message processing for consumer until restart."
                        );
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
        }

        // Update list of failing consumer names for this job
        let mut g_failed_consumer_names = self.failed_consumer_names.lock().unwrap();
        *g_failed_consumer_names = failing_consumer_names;

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

    #[tracing::instrument(level = "debug", skip_all, fields(%above_id, %batch_size))]
    #[transactional_method1(outbox_message_repository: Arc<dyn OutboxMessageRepository>)]
    async fn load_messages_above(
        &self,
        above_id: OutboxMessageID,
        batch_size: usize,
    ) -> Result<Vec<OutboxMessage>, InternalError> {
        let messages_stream = outbox_message_repository.get_producer_messages(
            &self.producer_name,
            above_id,
            batch_size,
        );

        use futures::TryStreamExt;
        messages_stream.try_collect::<Vec<_>>().await
    }

    #[transactional_method]
    async fn invoke_consumer(
        &self,
        consumer_name: &str,
        message: &OutboxMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(consumer_name=%consumer_name, received_message=?message, "Invoking consumer");

        let dispatcher = self
            .relay_routes_static_info
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
            consumer_name=%consumer_name,
            producer_name=%message.producer_name,
            last_consumed_message_id=%message.message_id,
            "Shifting consumption record"
        );

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
