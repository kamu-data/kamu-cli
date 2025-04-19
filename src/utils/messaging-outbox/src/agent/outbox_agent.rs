// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use database_common_macros::transactional_method2;
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use tracing::Instrument as _;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum RunMode {
    SingleIterationOnly,
    WhileHasTasks,
    MainRelayLoop,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const JOB_MESSAGING_OUTBOX_STARTUP: &str = "dev.kamu.utils.outbox.OutboxAgentStartup";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxAgent {
    catalog: Catalog,
    config: Arc<OutboxConfig>,
    routes_static_info: Arc<OutboxRoutesStaticInfo>,
    producer_consumption_jobs: Vec<ProducerConsumptionJob>,
    metrics: Arc<OutboxAgentMetrics>,
    run_lock: tokio::sync::Mutex<()>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_MESSAGING_OUTBOX_STARTUP,
    depends_on: &[],
    requires_transaction: false,
})]
#[scope(Singleton)]
impl OutboxAgent {
    pub fn new(
        catalog: Catalog,
        config: Arc<OutboxConfig>,
        message_dispatchers_by_producers: Vec<Arc<dyn MessageDispatcher>>,
        metrics: Arc<OutboxAgentMetrics>,
    ) -> Self {
        let routes_static_info = Arc::new(Self::make_static_routes_info(
            &catalog,
            message_dispatchers_by_producers,
        ));

        metrics.init(&routes_static_info.consumers_by_producers);

        let mut producer_consumption_jobs = Vec::new();
        for (producer_name, consumer_names) in &routes_static_info.consumers_by_producers {
            producer_consumption_jobs.push(ProducerConsumptionJob::new(
                catalog.clone(),
                routes_static_info.clone(),
                producer_name.clone(),
                consumer_names.clone(),
                metrics.clone(),
            ));
        }

        Self {
            catalog,
            config,
            routes_static_info,
            producer_consumption_jobs,
            metrics,
            run_lock: tokio::sync::Mutex::new(()),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn make_static_routes_info(
        catalog: &Catalog,
        message_dispatchers: Vec<Arc<dyn MessageDispatcher>>,
    ) -> OutboxRoutesStaticInfo {
        let all_durable_messaging_routes =
            enumerate_messaging_routes(catalog, MessageDeliveryMechanism::Transactional);
        let consumers_by_producers = group_consumers_by_producers(&all_durable_messaging_routes);
        let message_dispatchers_by_producers =
            group_message_dispatchers_by_producer(&message_dispatchers);

        OutboxRoutesStaticInfo::new(
            message_dispatchers_by_producers,
            all_durable_messaging_routes,
            consumers_by_producers,
        )
    }

    pub async fn run(&self) -> Result<(), InternalError> {
        // Main consumption loop
        self.run_with_mode(RunMode::MainRelayLoop).await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_while_has_tasks(&self) -> Result<(), InternalError> {
        self.run_with_mode(RunMode::WhileHasTasks).await
    }

    // To be used by tests only!
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_single_iteration_only(&self) -> Result<(), InternalError> {
        // Run single iteration instead of a loop
        self.run_with_mode(RunMode::SingleIterationOnly).await
    }

    async fn run_with_mode(&self, mode: RunMode) -> Result<(), InternalError> {
        match mode {
            RunMode::SingleIterationOnly => {
                self.run_consumption_iteration().await?;
            }
            RunMode::WhileHasTasks => loop {
                let processed_consumer_tasks_count = self
                    .run_consumption_iteration()
                    .instrument(tracing::debug_span!("OutboxAgent::tick"))
                    .await?;

                if processed_consumer_tasks_count == 0 {
                    break;
                }
            },
            RunMode::MainRelayLoop => {
                let loop_delay = self.config.awaiting_step.to_std().unwrap();

                loop {
                    self.run_consumption_iteration()
                        .instrument(tracing::debug_span!("OutboxAgent::tick"))
                        .await?;

                    tokio::time::sleep(loop_delay).await;
                }
            }
        }

        Ok(())
    }

    fn debug_outbox_routes(&self) {
        for messaging_route in &self.routes_static_info.all_durable_messaging_routes {
            tracing::debug!("{messaging_route}");
        }
    }

    #[transactional_method2(outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>, outbox_message_repository: Arc<dyn OutboxMessageRepository>)]
    async fn init_consumption_records(&self) -> Result<(), InternalError> {
        // Load existing consumption records
        use futures::TryStreamExt;
        let consumptions = outbox_consumption_repository
            .list_consumption_boundaries()
            .try_collect::<Vec<_>>()
            .await?;

        // Fetch latest messages produced by each producer to use as default for new
        // consumers with InitialConsumerBoundary::Latest property
        let latest_message_ids_by_producer = {
            let latest_message_ids_by_producer = outbox_message_repository
                .get_latest_message_ids_by_producer()
                .await?;

            latest_message_ids_by_producer
                .into_iter()
                .collect::<HashMap<_, _>>()
        };

        // Build a set of producer-consumer pairs that already exist in the database
        let mut matched_consumptions = HashSet::new();
        for consumption in &consumptions {
            matched_consumptions.insert((&consumption.producer_name, &consumption.consumer_name));
        }

        // Detect new routes, which are not associated with a consumption record yet
        for (producer_name, consumer_names) in &self.routes_static_info.consumers_by_producers {
            let latest_produced_message_id_maybe =
                latest_message_ids_by_producer.get(producer_name);
            for consumer_name in consumer_names {
                if !matched_consumptions.contains(&(producer_name, consumer_name)) {
                    // If consumer is not yet registered, and InitialConsumerBoundary is Latest,
                    // we set the last consumed message ID to the latest produced message ID
                    // otherwise, we set it to 0
                    let last_consumed_message_id = if let Some(consumer_metadata) =
                        particular_consumer_metadata_for(&self.catalog, consumer_name)
                        && consumer_metadata.initial_consumer_boundary
                            == InitialConsumerBoundary::Latest
                        && let Some(latest_produced_message_id) = latest_produced_message_id_maybe
                    {
                        *latest_produced_message_id
                    } else {
                        OutboxMessageID::new(0)
                    };

                    // Create a new consumption boundary
                    outbox_consumption_repository
                        .create_consumption_boundary(OutboxMessageConsumptionBoundary {
                            consumer_name: consumer_name.clone(),
                            producer_name: producer_name.clone(),
                            last_consumed_message_id,
                        })
                        .await
                        .int_err()?;
                }
            }
        }

        Ok(())
    }

    async fn run_consumption_iteration(
        &self,
    ) -> Result<ProcessedConsumerTasksCount, InternalError> {
        // We should not allow multiple concurrent entrances into consumption flow.
        // I.e., there could be concurrently:
        //  - main scheduled loop iteration
        //  - flushed iteration (i.e. via e2e middleware)
        let _guard = self.run_lock.lock().await;

        // Read current state of producers and consumptions
        // Prepare consumption tasks for each progressed producer
        let mut consumption_tasks_by_producer = self.prepare_consumption_iteration().await?;
        let processed_consumer_tasks_counter = Arc::new(AtomicUsize::new(0));

        // Select jobs per consumption task, unless consumers are failing already
        let mut consumption_job_tasks = Vec::new();
        for producer_consumption_job in &self.producer_consumption_jobs {
            // Extract task by producer
            let Some(producer_consumption_task) =
                consumption_tasks_by_producer.remove(producer_consumption_job.get_producer_name())
            else {
                continue;
            };

            // Skip this job if no more working consumers left
            if producer_consumption_job.all_consumers_failing() {
                continue;
            }

            // Plan task to run during this iteration
            consumption_job_tasks.push((
                producer_consumption_job,
                producer_consumption_task,
                processed_consumer_tasks_counter.clone(),
            ));
        }

        // Run iteration of consumption jobs of each producer concurrently
        use futures::{StreamExt, TryStreamExt};
        futures::stream::iter(consumption_job_tasks)
            .map(Ok)
            .try_for_each_concurrent(
                /* limit */ None,
                |(consumption_job, consumption_task, processed_consumer_tasks_counter)| async move {
                    let processed = consumption_job
                        .run_consumption_task(consumption_task)
                        .await?;

                    processed_consumer_tasks_counter.fetch_add(processed, Ordering::Relaxed);

                    Ok(())
                },
            )
            .await?;

        Ok(processed_consumer_tasks_counter.load(Ordering::Relaxed))
    }

    #[transactional_method2(
        outbox_message_repository: Arc<dyn OutboxMessageRepository>,
        outbox_consumption_repository: Arc<dyn OutboxMessageConsumptionRepository>
    )]
    async fn prepare_consumption_iteration(
        &self,
    ) -> Result<HashMap<String, ProducerConsumptionTask>, InternalError> {
        let planner = OutboxConsumptionIterationPlanner::new(
            self.routes_static_info.clone(),
            outbox_message_repository,
            outbox_consumption_repository,
            self.metrics.clone(),
            usize::try_from(self.config.batch_size).unwrap(),
        );

        planner.plan_consumption_tasks_by_producer().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for OutboxAgent {
    #[tracing::instrument(level = "debug", skip_all, name = "OutboxAgent::run_initialization")]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Trace current routes
        self.debug_outbox_routes();

        // Make sure consumption records represent the routes
        self.init_consumption_records().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
