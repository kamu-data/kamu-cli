// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use database_common::DatabaseTransactionRunner;
use dill::CatalogBuilder;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyResourceParams,
    ResourceControllerName,
    ResourceControllerRuntime,
    ResourceControllerRuntimeConfig,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourcePhase,
    ResourceVersion,
};
use kamu_resources_services::ResourceControllerRuntimeImpl;
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use messaging_outbox::OutboxProvider;
use retry_backoff::{RetryBackoff, RetryBackoffType};
use time_source::FakeSystemTimeSource;

use crate::tests::controller_runtime::utils::{
    TEST_CONTROLLER_NAME,
    TestControllerBehavior,
    TestControllerScript,
    TestOtherResourceControllerWithSameName,
    TestResourceController,
    TestResourceControllerLogic,
    TestResourceControllerRevision2,
    TestResourceSecondController,
    TestResourceStatusReactingController,
};
use crate::tests::utils::{
    ServiceLayerTestHarness,
    ServiceLayerTestHarnessOpts,
    TestResource,
    TestResourceSpec,
    test_stage_condition_key,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Calls after which one drain holds a resource back, as the runtime defines it
pub const MAX_PROCESS_CALLS_PER_DRAIN: usize = 10;

/// First failure backoff of the harness; doubles per consecutive failure
pub const FAILURE_BACKOFF: TimeDelta = TimeDelta::seconds(10);

/// Which controllers a simulated process registers
#[derive(Debug, Clone, Copy)]
pub enum TestControllers {
    Main,
    MainRevision2,
    StatusReacting,
    /// Two controllers for the test resource schema
    TwoForOneSchema,
    /// Two controllers of different schemas, with one name
    TwoWithOneName,
}

impl TestControllers {
    fn register(self, b: &mut CatalogBuilder) {
        match self {
            Self::Main => {
                b.add::<TestResourceController>();
            }
            Self::MainRevision2 => {
                b.add::<TestResourceControllerRevision2>();
            }
            Self::StatusReacting => {
                b.add::<TestResourceStatusReactingController>();
            }
            Self::TwoForOneSchema => {
                b.add::<TestResourceController>();
                b.add::<TestResourceSecondController>();
            }
            Self::TwoWithOneName => {
                b.add::<TestResourceController>();
                b.add::<TestOtherResourceControllerWithSameName>();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resources of the test type, driven by a scripted test controller on the
/// controller runtime.
///
/// Storage lives in the base catalog. The runtime and the controllers live in
/// a "process" catalog chained on it, which `restart` replaces: what survives
/// a restart is exactly what was persisted.
#[oop::extend(ServiceLayerTestHarness, base)]
pub struct ControllerRuntimeHarness {
    base: ServiceLayerTestHarness,
    account: odf::ResourceOwnerHandle,
    script: TestControllerScript,
    process_catalog: dill::Catalog,
}

impl ControllerRuntimeHarness {
    pub async fn new(controllers: TestControllers, behavior: TestControllerBehavior) -> Self {
        let base = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                // Messages wait in the outbox, so the one-shot lifecycle
                // dispatch never runs: only the runtime drives the resources
                outbox_provider: OutboxProvider::Dispatching,
                transactional: true,
                fake_time: true,
            },
            ..Default::default()
        });
        let account = base.ensure_test_account("test-owner").await;
        let script = TestControllerScript::new(behavior);
        let process_catalog = Self::start_process(&base, &script, controllers);

        Self {
            base,
            account,
            script,
            process_catalog,
        }
    }

    /// Simulates a restart: a new runtime with new controller instances, over
    /// the same storage
    pub fn restart(&mut self, controllers: TestControllers) {
        self.process_catalog = Self::start_process(&self.base, &self.script, controllers);
    }

    fn start_process(
        base: &ServiceLayerTestHarness,
        script: &TestControllerScript,
        controllers: TestControllers,
    ) -> dill::Catalog {
        let mut b = CatalogBuilder::new_chained(base.catalog());
        b.add::<ResourceControllerRuntimeImpl>();
        b.add_value(ResourceControllerRuntimeConfig {
            batch_size: NonZeroUsize::new(3).unwrap(),
            process_concurrency: NonZeroUsize::new(2).unwrap(),
            failure_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: FAILURE_BACKOFF.to_std().unwrap(),
                max_delay: Some(Duration::from_mins(1)),
            },
            ..Default::default()
        });
        b.add_value(script.clone());
        b.add::<TestResourceControllerLogic>();
        controllers.register(&mut b);
        b.build()
    }

    pub fn script(&self) -> &TestControllerScript {
        &self.script
    }

    pub fn runtime(&self) -> Arc<dyn ResourceControllerRuntime> {
        self.process_catalog.get_one().unwrap()
    }

    pub async fn run_until_idle(&self) {
        self.try_run_until_idle().await.unwrap();
    }

    pub async fn try_run_until_idle(&self) -> Result<(), InternalError> {
        self.runtime().run_until_idle().await
    }

    /// Runs until idle and returns how many `process` calls it made
    pub async fn process_calls_until_idle(&self) -> usize {
        let before = self.script.total_calls();
        self.run_until_idle().await;
        self.script.total_calls() - before
    }

    /// Moves the fake clock, waking the runtime if it sleeps until a moment
    /// that passed
    pub fn advance_time(&self, delta: TimeDelta) {
        self.catalog()
            .get_one::<FakeSystemTimeSource>()
            .unwrap()
            .advance(delta);
    }

    /// Starts the runtime's background agent; it stops when the returned
    /// handle is dropped
    pub fn start_agent(&self) -> RunningControllerRuntime {
        let runtime = self.runtime();
        RunningControllerRuntime {
            task: tokio::spawn(async move { runtime.run().await }),
        }
    }

    /// Lets the agent finish what it was doing and reach its wait. Real time:
    /// the agent's tasks run on the test's runtime meanwhile.
    pub async fn let_agent_settle(&self) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pub async fn apply(&self, name: &str) -> ResourceID {
        self.apply_and_get_id(&self.account, name).await
    }

    /// Re-applies the resource with another spec, which bumps its generation
    pub async fn update_spec(&self, id: ResourceID, value: &str) {
        let snapshot = self.get_snapshot_by_id(&id).await.unwrap();
        self.apply_test_uc()
            .apply(ApplyResourceParams {
                id: Some(id),
                headers: BaseResourceServiceHarness::make_headers_input(
                    self.account.clone(),
                    snapshot.headers.name.as_str(),
                ),
                spec: TestResourceSpec {
                    value: value.to_string(),
                },
            })
            .await
            .unwrap()
            .expect_applied();
    }

    pub async fn request_deletion(&self, id: ResourceID) {
        self.request_resources_deletion(self.account.did.clone(), vec![id])
            .await;
    }

    /// Another writer patches the `TestStage` condition of the resource
    pub async fn write_stage_as_other_writer(&self, id: ResourceID, stage: &str) {
        let status_generation = self.version(id).await.status_generation;
        let patch = TestResourceControllerLogic::stage_patch(stage);

        DatabaseTransactionRunner::new(self.catalog().clone())
            .transactional_with(
                |primitives: Arc<dyn ResourceLifecyclePrimitives<TestResource>>| async move {
                    primitives
                        .update_conditions(&id, status_generation, &patch)
                        .await
                        .int_err()
                },
            )
            .await
            .unwrap();
    }

    pub async fn version(&self, id: ResourceID) -> ResourceVersion {
        ResourceVersion::from(&self.get_snapshot_by_id(&id).await.unwrap().headers)
    }

    pub async fn exists(&self, id: ResourceID) -> bool {
        self.get_snapshot_by_id(&id).await.is_some()
    }

    pub async fn phase(&self, id: ResourceID) -> ResourcePhase {
        self.get_snapshot_by_id(&id)
            .await
            .unwrap()
            .status
            .unwrap()
            .phase
    }

    /// The `TestStage` condition of the resource
    pub async fn stage(&self, id: ResourceID) -> Option<String> {
        self.get_snapshot_by_id(&id)
            .await
            .unwrap()
            .status
            .unwrap()
            .conditions
            .entries
            .get(&test_stage_condition_key())
            .and_then(|value| value.as_str())
            .map(ToString::to_string)
    }

    /// The controller's bookkeeping row of the resource
    pub async fn state(&self, id: ResourceID) -> Option<ResourceControllerState> {
        self.catalog()
            .get_one::<dyn ResourceControllerStateRepository>()
            .unwrap()
            .get_state(&ResourceControllerName::new(TEST_CONTROLLER_NAME), &id)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The runtime's agent running in the background of a test
pub struct RunningControllerRuntime {
    task: tokio::task::JoinHandle<Result<(), InternalError>>,
}

impl Drop for RunningControllerRuntime {
    fn drop(&mut self) {
        self.task.abort();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
