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
    MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceControllerDrainSession,
    ResourceControllerHints,
    ResourceControllerName,
    ResourceControllerRuntime,
    ResourceControllerRuntimeConfig,
    ResourceControllerState,
    ResourceControllerStateRepository,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourcePhase,
    ResourceVersion,
    TypeUri,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    BaseResourceServiceHarnessOpts,
};
use kamu_resources_services::{ResourceControllerRuntimeImpl, ResourceControllerRuntimeMetrics};
use messaging_outbox::{OutboxMessageBridge, OutboxProvider};
use retry_backoff::{RetryBackoff, RetryBackoffType};
use time_source::FakeSystemTimeSource;

use crate::tests::controller_runtime::utils::{
    TEST_CONTROLLER_NAME,
    TEST_OTHER_SCHEMA,
    TestControllerBehavior,
    TestControllerScript,
    TestOtherResourceController,
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

/// First loop restart backoff of the harness; doubles per consecutive failure
pub const LOOP_RESTART_BACKOFF: TimeDelta = TimeDelta::seconds(30);

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
    /// The main controller, and one of a schema no resource has
    MainAndOther,
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
            Self::MainAndOther => {
                b.add::<TestResourceController>();
                b.add::<TestOtherResourceController>();
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
                outbox_provider: OutboxProvider::Dispatching,
                transactional: true,
                fake_time: true,
                ..Default::default()
            },
            // The scripted controllers drive the resources, on a runtime of its own
            simple_controller: false,
            ..Default::default()
        });
        // As at startup: consumers start from the latest message, so they
        // must be registered before the first one
        base.outbox_agent().run_initialization().await.unwrap();
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
            loop_restart_backoff: RetryBackoff {
                backoff_type: RetryBackoffType::Exponential,
                min_delay: LOOP_RESTART_BACKOFF.to_std().unwrap(),
                max_delay: Some(Duration::from_mins(5)),
            },
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

    pub async fn run_outbox_and_controllers_until_idle(&self) {
        self.try_run_outbox_and_controllers_until_idle()
            .await
            .unwrap();
    }

    /// Delivers the outbox messages and drains the controllers in turn, as a
    /// CLI command does before it exits
    pub async fn try_run_outbox_and_controllers_until_idle(&self) -> Result<(), InternalError> {
        let outbox_agent = self.outbox_agent();
        let mut drains = self.runtime().new_drain_session()?;

        loop {
            outbox_agent.run_while_has_tasks().await?;

            if drains.drain_due_resources().await? == 0 {
                return Ok(());
            }
        }
    }

    pub fn new_drain_session(&self) -> Box<dyn ResourceControllerDrainSession> {
        self.runtime().new_drain_session().unwrap()
    }

    /// Delivers the outbox messages, then drains once in the session, and
    /// returns the `process` calls made
    pub async fn deliver_and_drain(
        &self,
        session: &mut Box<dyn ResourceControllerDrainSession>,
    ) -> usize {
        self.deliver_messages().await;
        session.drain_due_resources().await.unwrap()
    }

    /// Whether the resource lifecycle consumer has messages left to deliver
    pub async fn has_undelivered_lifecycle_messages(&self) -> bool {
        let bridge = self.catalog().get_one::<dyn OutboxMessageBridge>().unwrap();

        let (_, latest) = bridge
            .get_latest_message_boundaries_by_producer(self.catalog())
            .await
            .unwrap()
            .into_iter()
            .find(|(producer, _)| producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE)
            .unwrap();
        let consumed = bridge
            .list_consumption_boundaries(self.catalog())
            .await
            .unwrap()
            .into_iter()
            .find(|boundary| {
                boundary.producer_name == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && boundary.consumer_name
                        == MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE
            })
            .unwrap();

        consumed.last_consumed_message_id < latest.message_id
    }

    /// Runs until idle and returns how many `process` calls it made
    pub async fn process_calls_until_idle(&self) -> usize {
        let before = self.script.total_calls();
        self.run_outbox_and_controllers_until_idle().await;
        self.script.total_calls() - before
    }

    /// Delivers the outbox messages, without draining the controllers
    pub async fn deliver_messages(&self) {
        self.outbox_agent().run_while_has_tasks().await.unwrap();
    }

    /// Moves the fake clock, waking the runtime if it sleeps until a moment
    /// that passed
    pub fn advance_time(&self, delta: TimeDelta) {
        self.catalog()
            .get_one::<FakeSystemTimeSource>()
            .unwrap()
            .advance(delta);
    }

    /// Hints the runtime that resources of the test type changed
    pub fn hint(&self) {
        self.catalog()
            .get_one::<dyn ResourceControllerHints>()
            .unwrap()
            .hint(&TypeUri::new_unchecked(TestResource::SCHEMA_STR));
    }

    /// Hints the runtime that resources of the schema no resource has changed
    pub fn hint_other_schema(&self) {
        self.catalog()
            .get_one::<dyn ResourceControllerHints>()
            .unwrap()
            .hint(&TypeUri::new_unchecked(TEST_OTHER_SCHEMA));
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

    /// The main controller's bookkeeping row of the resource
    pub async fn state(&self, id: ResourceID) -> Option<ResourceControllerState> {
        self.state_of(TEST_CONTROLLER_NAME, id).await
    }

    /// The named controller's bookkeeping row of the resource
    pub async fn state_of(
        &self,
        controller_name: &'static str,
        id: ResourceID,
    ) -> Option<ResourceControllerState> {
        self.catalog()
            .get_one::<dyn ResourceControllerStateRepository>()
            .unwrap()
            .get_state(&ResourceControllerName::new(controller_name), &id)
            .await
            .unwrap()
    }

    /// `resource_controller_processed_total` of the main controller
    pub fn processed_metric(&self, outcome: &str) -> u64 {
        self.metrics()
            .processed_total
            .with_label_values(&[TEST_CONTROLLER_NAME, outcome])
            .get()
    }

    /// `resource_controller_loop_restarts_total` of the named controller
    pub fn loop_restarts_metric(&self, controller_name: &str) -> u64 {
        self.metrics()
            .loop_restarts_total
            .with_label_values(&[controller_name])
            .get()
    }

    /// `resource_controller_failing_resources` of the main controller
    pub fn failing_resources_metric(&self) -> i64 {
        self.metrics()
            .failing_resources
            .with_label_values(&[TEST_CONTROLLER_NAME])
            .get()
    }

    fn metrics(&self) -> Arc<ResourceControllerRuntimeMetrics> {
        self.catalog().get_one().unwrap()
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
