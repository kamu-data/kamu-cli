// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use background_agent_host::{BackgroundAgent, CancellationToken};
use dill::{CatalogWeakRef, Singleton, component, interface, scope};
use internal_error::{InternalError, ResultIntoInternal};
use time_source::SystemTimeSource;
use tokio::task::JoinSet;
use wakeup_listener::WakeupListenerConfig;

use crate::controller_runtime::{
    ResourceControllerDrainSessionImpl,
    ResourceControllerLoop,
    ResourceControllerRegistration,
    ResourceControllerRuntimeMetrics,
    ResourceControllerSupervisor,
};
use crate::domain::{
    RESOURCE_CONTROLLER_RUNTIME_AGENT_NAME,
    ResourceControllerDrainSession,
    ResourceControllerHints,
    ResourceControllerName,
    ResourceControllerRuntime,
    ResourceControllerRuntimeConfig,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One background agent for every registered `ResourceController`.
///
/// The controllers are collected from the catalog each time the agent or a
/// drain session starts, so the agent holds no controller, and no strong
/// reference to the catalog that owns it.
pub struct ResourceControllerRuntimeImpl {
    catalog: CatalogWeakRef,
    config: Arc<ResourceControllerRuntimeConfig>,
    wakeup_config: Arc<WakeupListenerConfig>,
    hints: Arc<dyn ResourceControllerHints>,
    time_source: Arc<dyn SystemTimeSource>,
    metrics: Arc<ResourceControllerRuntimeMetrics>,
    /// One per controller, kept across calls: the agent's loop and the drain
    /// sessions build loops of their own, which must not drain the
    /// same controller at once
    drain_locks: Mutex<HashMap<ResourceControllerName, Arc<tokio::sync::Mutex<()>>>>,
}

#[component(pub)]
#[interface(dyn ResourceControllerRuntime)]
#[interface(dyn BackgroundAgent)]
#[scope(Singleton)]
impl ResourceControllerRuntimeImpl {
    pub fn new(
        catalog: CatalogWeakRef,
        config: Arc<ResourceControllerRuntimeConfig>,
        wakeup_config: Arc<WakeupListenerConfig>,
        hints: Arc<dyn ResourceControllerHints>,
        time_source: Arc<dyn SystemTimeSource>,
        metrics: Arc<ResourceControllerRuntimeMetrics>,
    ) -> Self {
        Self {
            catalog,
            config,
            wakeup_config,
            hints,
            time_source,
            metrics,
            drain_locks: Mutex::new(HashMap::new()),
        }
    }

    /// A loop per registered controller
    fn build_loops(&self) -> Result<Vec<ResourceControllerLoop>, InternalError> {
        let catalog = self.catalog.upgrade();

        Ok(ResourceControllerRegistration::collect_all(&catalog)?
            .into_iter()
            .map(|registration| {
                let drain_lock = self.drain_lock(&registration.meta().controller_name);
                ResourceControllerLoop::new(
                    registration,
                    catalog.clone(),
                    self.config.clone(),
                    self.wakeup_config.clone(),
                    self.time_source.clone(),
                    self.metrics.clone(),
                    drain_lock,
                )
            })
            .collect())
    }

    fn drain_lock(&self, controller_name: &ResourceControllerName) -> Arc<tokio::sync::Mutex<()>> {
        self.drain_locks
            .lock()
            .unwrap()
            .entry(controller_name.clone())
            .or_default()
            .clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for ResourceControllerRuntimeImpl {
    fn agent_name(&self) -> &'static str {
        RESOURCE_CONTROLLER_RUNTIME_AGENT_NAME
    }

    async fn run(&self, shutdown: CancellationToken) -> Result<(), InternalError> {
        let loops = self.build_loops()?;

        // Returning early would count as an unexpected completion, so an agent
        // with nothing to drive waits for the shutdown instead
        if loops.is_empty() {
            tracing::info!("No resource controllers registered");
            shutdown.cancelled().await;
            return Ok(());
        }

        self.metrics.init(
            loops
                .iter()
                .map(|controller_loop| &controller_loop.registration().meta().controller_name),
        );

        let mut running = JoinSet::new();
        for controller_loop in loops {
            // Created once per controller and kept, so that no hint is missed
            // between waits
            let meta = controller_loop.registration().meta();
            let listener = Arc::from(
                self.hints
                    .new_wakeup_listener(meta.schema, &meta.controller_name),
            );

            running.spawn(
                ResourceControllerSupervisor::new(
                    controller_loop,
                    listener,
                    self.config.loop_restart_backoff,
                    self.time_source.clone(),
                    self.metrics.clone(),
                )
                .run(shutdown.clone()),
            );
        }

        // Supervisors end only on a shutdown
        while let Some(ended) = running.join_next().await {
            ended.int_err()?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceControllerRuntime for ResourceControllerRuntimeImpl {
    fn new_drain_session(&self) -> Result<Box<dyn ResourceControllerDrainSession>, InternalError> {
        Ok(Box::new(ResourceControllerDrainSessionImpl::new(
            self.build_loops()?,
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
