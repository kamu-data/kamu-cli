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

use async_utils::BackgroundAgent;
use dill::{CatalogWeakRef, Singleton, component, interface, scope};
use internal_error::{ErrorIntoInternal, InternalError};
use time_source::SystemTimeSource;
use tokio::task::JoinSet;
use wakeup_listener::WakeupListenerConfig;

use crate::controller_runtime::{ResourceControllerLoop, ResourceControllerRegistration};
use crate::domain::{
    RESOURCE_CONTROLLER_RUNTIME_AGENT_NAME,
    ResourceControllerHints,
    ResourceControllerName,
    ResourceControllerRuntime,
    ResourceControllerRuntimeConfig,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One background agent for every registered `ResourceController`.
///
/// The controllers are collected from the catalog each time the agent starts
/// or `run_until_idle` is called, so the agent holds no controller, and no
/// strong reference to the catalog that owns it.
pub struct ResourceControllerRuntimeImpl {
    catalog: CatalogWeakRef,
    config: Arc<ResourceControllerRuntimeConfig>,
    wakeup_config: Arc<WakeupListenerConfig>,
    hints: Arc<dyn ResourceControllerHints>,
    time_source: Arc<dyn SystemTimeSource>,
    /// One per controller, kept across calls: the agent's loop and
    /// `run_until_idle` build loops of their own, which must not drain the
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
    ) -> Self {
        Self {
            catalog,
            config,
            wakeup_config,
            hints,
            time_source,
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

    async fn run(&self) -> Result<(), InternalError> {
        let loops = self.build_loops()?;

        // A returning agent stops the server, so an agent with nothing to
        // drive waits forever instead
        if loops.is_empty() {
            tracing::info!("No resource controllers registered");
            return std::future::pending().await;
        }

        let mut running = JoinSet::new();
        for controller_loop in loops {
            // Created once per loop and kept, so that no hint is missed
            // between waits
            let meta = controller_loop.registration().meta();
            let listener = self
                .hints
                .new_wakeup_listener(meta.schema, &meta.controller_name);

            running.spawn(async move {
                loop {
                    let drained = controller_loop.drain(Some(listener.as_ref())).await?;
                    tracing::debug!(
                        processed = drained.processed(),
                        "Resource controller drain finished"
                    );
                    controller_loop.wait(listener.as_ref(), &drained).await?;
                }
            });
        }

        // Loops only end by failing
        match running.join_next().await {
            Some(Ok(result)) => result,
            Some(Err(join_error)) => Err(join_error.int_err()),
            None => Ok(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceControllerRuntime for ResourceControllerRuntimeImpl {
    async fn run_until_idle(&self) -> Result<(), InternalError> {
        for controller_loop in self.build_loops()? {
            controller_loop.drain(None).await?;
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
