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

use database_common_macros::{transactional_method1, transactional_method2};
use dill::{component, scope, Catalog, Singleton};
use internal_error::{InternalError, ResultIntoInternal};
use tracing::Instrument as _;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxExecutor {
    catalog: Catalog,
    config: Arc<OutboxConfig>,
    routes_static_info: Arc<OutboxRoutesStaticInfo>,
    producer_consumption_jobs: Vec<ProducerConsumptionJob>,
    metrics: Arc<OutboxExecutorMetrics>,
}

#[component(pub)]
#[scope(Singleton)]
impl OutboxExecutor {
    pub fn new(
        catalog: Catalog,
        config: Arc<OutboxConfig>,
        message_dispatchers_by_producers: Vec<Arc<dyn MessageDispatcher>>,
        metrics: Arc<OutboxExecutorMetrics>,
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
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn make_static_routes_info(
        catalog: &Catalog,
        message_dispatchers: Vec<Arc<dyn MessageDispatcher>>,
    ) -> OutboxRoutesStaticInfo {
        let all_durable_messaging_routes =
            enumerate_messaging_routes(catalog, MessageConsumptionDurability::Durable);
        let consumers_by_producers = group_consumers_by_producers(&all_durable_messaging_routes);
        let message_dispatchers_by_producers =
            group_message_dispatchers_by_producer(&message_dispatchers);

        OutboxRoutesStaticInfo::new(
            message_dispatchers_by_producers,
            all_durable_messaging_routes,
            consumers_by_producers,
        )
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn pre_run(&self) -> Result<(), InternalError> {
        // Trace current routes
        self.debug_outbox_routes();

        // Make sure consumption records represent the routes
        self.init_consumption_records().await
    }

    pub async fn run(&self) -> Result<(), InternalError> {
        // Main consumption loop
        loop {
            self.run_consumption_iteration()
                .instrument(tracing::debug_span!("OutboxExecutor::tick"))
                .await?;

            tokio::time::sleep(self.config.awaiting_step.to_std().unwrap()).await;
        }
    }

    // To be used by tests only!
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn run_single_iteration_only(&self) -> Result<(), InternalError> {
        // Run single iteration instead of a loop
        self.run_consumption_iteration().await?;
        Ok(())
    }

    fn debug_outbox_routes(&self) {
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

    async fn run_consumption_iteration(&self) -> Result<(), InternalError> {
        // Read current state of producers and consumptions
        // Prepare consumption tasks for each progressed producer
        let mut consumption_tasks_by_producer = self.prepare_consumption_iteration().await?;

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
            consumption_job_tasks.push((producer_consumption_job, producer_consumption_task));
        }

        // Run iteration of consumption jobs of each producer concurrently
        use futures::{StreamExt, TryStreamExt};
        futures::stream::iter(consumption_job_tasks)
            .map(Ok)
            .try_for_each_concurrent(
                /* limit */ None,
                |(consumption_job, consumption_task)| async move {
                    consumption_job.run_consumption_task(consumption_task).await
                },
            )
            .await?;

        Ok(())
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
