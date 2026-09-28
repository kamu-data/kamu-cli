// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_utils::BackgroundAgent;
use database_common_macros::transactional_method;
use dill::Builder;
use event_sourcing::EventID;
use internal_error::InternalError;
use kamu_flow_system::{
    FlowSystemEventAgent,
    FlowSystemEventAgentConfig,
    FlowSystemEventBridge,
    FlowSystemEventProjector,
};
use tracing::Instrument as _;
use wakeup_listener::WakeupListenerConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn BackgroundAgent)]
#[dill::interface(dyn FlowSystemEventAgent)]
#[dill::scope(dill::Singleton)]
pub struct FlowSystemEventAgentImpl {
    catalog: dill::CatalogWeakRef,
    flow_system_event_bridge: Arc<dyn FlowSystemEventBridge>,
    agent_config: Arc<FlowSystemEventAgentConfig>,
    wakeup_config: Arc<WakeupListenerConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgentImpl {
    /// Applies all pending events to every projector
    #[tracing::instrument(level = "debug", skip_all)]
    async fn apply_pending_events(&self) {
        let catalog = self.catalog.upgrade();

        // For each projector, apply all existing unprocessed events
        let projector_builders = catalog
            .builders_for::<dyn FlowSystemEventProjector>()
            .collect::<Vec<_>>();

        // Loop until each projector is totally caught up
        for builder in projector_builders {
            tracing::debug!(
                instance_type = builder.instance_type().name,
                "Catching up projector"
            );

            // If projector is far behind, it might take multiple batches to catch up
            let mut num_total_processed = 0;
            loop {
                // Apply a batch of events to the projector
                match self.apply_batch_to_projector(&builder).await {
                    // Success
                    Ok(num_processed) => {
                        tracing::debug!(
                            instance_type = builder.instance_type().name,
                            num_processed,
                            "Projector batch processed",
                        );
                        if num_processed == 0 {
                            tracing::debug!(
                                instance_type = builder.instance_type().name,
                                num_total_processed,
                                "Projector caught up",
                            );
                            break;
                        }
                        num_total_processed += num_processed;
                    }

                    // Problem: log issue and continue with other projectors
                    Err(e) => {
                        tracing::error!(
                            error = ?e,
                            error_msg = %e,
                            instance_type = builder.instance_type().name,
                            "Projector batch processing failed",
                        );
                        break;
                    }
                }
            }
        }
    }

    #[transactional_method]
    #[tracing::instrument(level = "debug", skip_all, fields(projector = builder.instance_type().name))]
    async fn apply_batch_to_projector(
        &self,
        projector_builder: &dill::TypecastBuilder<'_, dyn FlowSystemEventProjector>,
    ) -> Result<usize, InternalError> {
        // Construct projector instance
        let projector = projector_builder.get(&transaction_catalog).unwrap();

        // Try load next batch
        let batch = self
            .flow_system_event_bridge
            .fetch_next_batch(
                &transaction_catalog,
                projector.name(),
                self.agent_config.batch_size,
            )
            .await?;
        tracing::debug!(batch_size = batch.len(), "Fetched batch");

        // Quick exit if no events
        if batch.is_empty() {
            return Ok(0);
        }

        // Apply each event to build projection
        for e in &batch {
            projector.apply(e).await?;
        }

        // Mark projection progress
        let ids: Vec<(EventID, i64)> = batch.iter().map(|e| (e.event_id, e.tx_id)).collect();
        self.flow_system_event_bridge
            .mark_applied(&transaction_catalog, projector.name(), &ids)
            .await?;

        // Return number of processed events
        Ok(batch.len())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for FlowSystemEventAgentImpl {
    fn agent_name(&self) -> &'static str {
        "dev.kamu.domain.flow-system.FlowSystemEventAgent"
    }

    async fn run(&self) -> Result<(), internal_error::InternalError> {
        // On startup, immediately sync all projectors to catch up with existing events
        self.apply_pending_events()
            .instrument(tracing::info_span!(
                "FlowSystemEventAgent::initial_catchup_phase"
            ))
            .await;

        // Access wakeup listener
        let wakeup_listener = self.flow_system_event_bridge.wakeup_listener();

        // Then enter the infinite main loop
        loop {
            // Wait for push or timeout - let the store handle the backoff strategy
            let hint = wakeup_listener
                .wait_wake(
                    self.wakeup_config.max_listening_timeout,
                    self.wakeup_config.min_debounce_interval,
                )
                .await?;
            tracing::debug!(hint = ?hint, "Agent woke up with a hint");

            self.apply_pending_events().await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventAgent for FlowSystemEventAgentImpl {
    async fn catchup_remaining_events(&self) -> Result<(), InternalError> {
        self.apply_pending_events().await;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
