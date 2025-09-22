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
use event_sourcing::EventID;
use internal_error::InternalError;
use kamu_flow_system::{
    FlowSystemEventAgent,
    FlowSystemEventAgentConfig,
    FlowSystemEventBridge,
    FlowSystemEventProjector,
    FlowSystemEventStoreWakeHint,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn BackgroundAgent)]
#[dill::interface(dyn FlowSystemEventAgent)]
#[dill::scope(dill::Singleton)]
pub struct FlowSystemEventAgentImpl {
    catalog: dill::Catalog,
    flow_system_event_store: Arc<dyn FlowSystemEventBridge>,
    projectors: Vec<Arc<dyn FlowSystemEventProjector>>,
    agent_config: Arc<FlowSystemEventAgentConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgentImpl {
    #[transactional_method]
    async fn apply_batch_to_projector(
        &self,
        projector: &dyn FlowSystemEventProjector,
        maybe_upper_event_id_bound: Option<EventID>,
    ) -> Result<(), InternalError> {
        let batch = self
            .flow_system_event_store
            .fetch_next_batch(
                &transaction_catalog,
                projector.name(),
                self.agent_config.batch_size,
                maybe_upper_event_id_bound,
            )
            .await?;

        if batch.is_empty() {
            return Ok(());
        }

        // Apply events to build projections
        for e in &batch {
            projector.apply(&transaction_catalog, e).await?;
        }

        // Mark projection progress
        let ids: Vec<EventID> = batch.iter().map(|e| e.event_id).collect();
        self.flow_system_event_store
            .mark_applied(&transaction_catalog, projector.name(), &ids)
            .await?;

        Ok(())
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
        tracing::info!("Initial projectors sync at startup");
        for projector in &self.projectors {
            self.apply_batch_to_projector(projector.as_ref(), None)
                .await?;
        }

        tracing::info!("Starting main event listening loop");

        loop {
            tracing::debug!("Starting iteration");

            // 1) Wait for push or timeout - let the store handle the backoff strategy
            let hint = self
                .flow_system_event_store
                .wait_wake(
                    self.agent_config.max_listening_timeout,
                    self.agent_config.min_debounce_interval,
                )
                .await?;
            tracing::debug!("Woke up, hint: {hint:?}");

            // 2) For each projector, drain until no work.
            let maybe_upper_event_id_bound = match hint {
                FlowSystemEventStoreWakeHint::NewEvents {
                    upper_event_id_bound,
                } => Some(upper_event_id_bound),
                FlowSystemEventStoreWakeHint::Timeout => None,
            };

            for projector in &self.projectors {
                self.apply_batch_to_projector(projector.as_ref(), maybe_upper_event_id_bound)
                    .await?;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgent for FlowSystemEventAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
