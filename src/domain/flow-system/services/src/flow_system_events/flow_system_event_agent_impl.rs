// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use async_utils::BackgroundAgent;
use database_common_macros::transactional_method;
use dill::Builder;
use event_sourcing::EventID;
use internal_error::InternalError;
use kamu_flow_system::{
    FLOW_SYSTEM_EVENT_AGENT_NAME,
    FlowSystemEventAgent,
    FlowSystemEventAgentConfig,
    FlowSystemEventBridge,
    FlowSystemEventProjector,
};
use tracing::Instrument as _;
use wakeup_listener::{HELD_BACK_RECHECK_INTERVAL, WakeupListener, WakeupListenerConfig};

use crate::FlowSystemEventAgentMetrics;

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
    metrics: Arc<FlowSystemEventAgentMetrics>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgentImpl {
    /// Applies all pending events to every projector. Given the agent's
    /// listener, keeps its heartbeat going between batches
    #[tracing::instrument(level = "debug", skip_all)]
    async fn apply_pending_events(&self, wakeup_listener: Option<&dyn WakeupListener>) {
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
                let result = self.apply_batch_to_projector(&builder).await;
                if let Some(wakeup_listener) = wakeup_listener {
                    wakeup_listener.heartbeat();
                }

                match result {
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

    /// Applies pending events and returns how long to wait for the next wakeup.
    /// Held back events are checked before applying: a commit that releases
    /// them after the check is either seen by the applying, or happens while
    /// the check still holds
    async fn catch_up(&self, wakeup_listener: &dyn WakeupListener) -> Duration {
        let listening_timeout = self.listening_timeout().await;
        self.apply_pending_events(Some(wakeup_listener)).await;
        listening_timeout
    }

    /// How long to wait for a wakeup: briefly while committed events are held
    /// back, since the commit that releases them may raise no wakeup
    async fn listening_timeout(&self) -> Duration {
        match self.has_held_back_events().await {
            Ok(true) => HELD_BACK_RECHECK_INTERVAL,
            Ok(false) => self.wakeup_config.max_listening_timeout,
            Err(e) => {
                tracing::error!(
                    error = ?e,
                    error_msg = %e,
                    "Checking for held back events failed"
                );
                self.wakeup_config.max_listening_timeout
            }
        }
    }

    #[transactional_method]
    async fn has_held_back_events(&self) -> Result<bool, InternalError> {
        self.flow_system_event_bridge
            .has_held_back_events(&transaction_catalog)
            .await
    }

    #[transactional_method]
    #[tracing::instrument(level = "debug", skip_all, fields(projector = builder.instance_type().name))]
    async fn apply_batch_to_projector(
        &self,
        projector_builder: &dill::TypecastBuilder<'_, dyn FlowSystemEventProjector>,
    ) -> Result<usize, InternalError> {
        // Construct projector instance
        let projector = projector_builder.get(&transaction_catalog).unwrap();

        // Recorded before commit, as only the instance knows its name: a failing
        // commit alone is not reported here
        let result = self
            .apply_batch(&transaction_catalog, projector.as_ref())
            .await;
        self.metrics.on_projector_batch(projector.name(), &result);
        result
    }

    async fn apply_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector: &dyn FlowSystemEventProjector,
    ) -> Result<usize, InternalError> {
        // Try load next batch
        let batch = self
            .flow_system_event_bridge
            .fetch_next_batch(
                transaction_catalog,
                projector.name(),
                self.agent_config.batch_size.get(),
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
            .mark_applied(transaction_catalog, projector.name(), &ids)
            .await?;

        // Return number of processed events
        Ok(batch.len())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for FlowSystemEventAgentImpl {
    fn agent_name(&self) -> &'static str {
        FLOW_SYSTEM_EVENT_AGENT_NAME
    }

    async fn run(&self) -> Result<(), internal_error::InternalError> {
        let wakeup_listener = self.flow_system_event_bridge.new_wakeup_listener();

        // On startup, immediately sync all projectors to catch up with existing events
        let mut listening_timeout = self
            .catch_up(wakeup_listener.as_ref())
            .instrument(tracing::info_span!(
                "FlowSystemEventAgent::initial_catchup_phase"
            ))
            .await;

        // Then enter the infinite main loop
        loop {
            // Wait for push or timeout - let the store handle the backoff strategy
            let hint = wakeup_listener
                .wait_wake(listening_timeout, self.wakeup_config.min_debounce_interval)
                .await?;
            tracing::debug!(hint = ?hint, "Agent woke up with a hint");

            listening_timeout = self.catch_up(wakeup_listener.as_ref()).await;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventAgent for FlowSystemEventAgentImpl {
    async fn catchup_remaining_events(&self) -> Result<(), InternalError> {
        self.apply_pending_events(None).await;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
