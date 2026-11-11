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
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use database_common::DatabaseTransactionRunner;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    DeclarativeResource,
    ResourceAggregateLoader,
    ResourceConditionsPatch,
    ResourceDeletionCompletion,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceProcessOutcome,
    ResourceVersion,
    TypeUri,
};
use time_source::SystemTimeSource;
use tokio::sync::Notify;

use crate::tests::utils::{TEST_STAGE_CONDITION_URI, TestResource, test_stage_condition_key};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Declares a `ResourceController` component with its registration, all
/// delegating to one `TestResourceControllerLogic`
macro_rules! declare_test_resource_controller {
    (
        $controller:ident,
        schema = $schema:expr,
        controller_name = $controller_name:expr,
        revision = $revision:literal,
        reacts_to_status = $reacts_to_status:literal
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceController)]
        #[dill::meta(kamu_resources::ResourceControllerMeta {
                    schema: $schema,
                    controller_name: kamu_resources::ResourceControllerName::new($controller_name),
                    controller_revision: $revision,
                    reacts_to_status: $reacts_to_status,
                })]
        pub struct $controller {
            logic: Arc<TestResourceControllerLogic>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceController for $controller {
            async fn list_extra_actionable(
                &self,
                _now: DateTime<Utc>,
                limit: usize,
            ) -> Result<Vec<ResourceID>, InternalError> {
                Ok(self.logic.script.extras(limit))
            }

            async fn process(
                &self,
                id: &ResourceID,
            ) -> Result<ResourceProcessOutcome, InternalError> {
                self.logic.process(*id).await
            }

            async fn next_extra_deadline(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
                Ok(self.logic.script.next_extra_deadline())
            }
        }
    };
}

pub const TEST_CONTROLLER_NAME: &str = "dev.kamu.test.TestResourceController";

/// A schema no resource of these tests has
pub const TEST_OTHER_SCHEMA: &str = "https://test.kamu.dev/schemas/test/v1/OtherResource";

declare_test_resource_controller!(
    TestResourceController,
    schema = TestResource::SCHEMA_STR,
    controller_name = TEST_CONTROLLER_NAME,
    revision = 1,
    reacts_to_status = false
);

declare_test_resource_controller!(
    TestResourceControllerRevision2,
    schema = TestResource::SCHEMA_STR,
    controller_name = TEST_CONTROLLER_NAME,
    revision = 2,
    reacts_to_status = false
);

declare_test_resource_controller!(
    TestResourceStatusReactingController,
    schema = TestResource::SCHEMA_STR,
    controller_name = TEST_CONTROLLER_NAME,
    revision = 1,
    reacts_to_status = true
);

declare_test_resource_controller!(
    TestResourceSecondController,
    schema = TestResource::SCHEMA_STR,
    controller_name = "dev.kamu.test.SecondTestResourceController",
    revision = 1,
    reacts_to_status = false
);

declare_test_resource_controller!(
    TestOtherResourceControllerWithSameName,
    schema = TEST_OTHER_SCHEMA,
    controller_name = TEST_CONTROLLER_NAME,
    revision = 1,
    reacts_to_status = false
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What the test controller does when asked about a resource
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestControllerBehavior {
    /// Changes nothing and returns `Done` at the resource's current version
    FinishAtCurrentVersion,
    /// Works in two steps, each persisted in the `TestStage` condition, with a
    /// `RetryAt` of `step_wait` in between:
    /// - reconciliation: begin and mark `prepared`, then complete;
    /// - deletion: mark `finalizing`, then complete the deletion.
    Stepwise { step_wait: TimeDelta },
    /// Returns `RetryAt` this far from now
    RetryAfter(TimeDelta),
    /// Fails with an error
    Fail,
    /// Panics
    Panic,
}

/// What the tests script and observe, shared by every controller instance of
/// a harness, across restarts
#[derive(Clone)]
pub struct TestControllerScript {
    state: Arc<Mutex<TestControllerScriptState>>,
    /// Signaled on every `process` call
    called: Arc<Notify>,
}

struct TestControllerScriptState {
    default_behavior: TestControllerBehavior,
    behavior_by_resource: HashMap<ResourceID, TestControllerBehavior>,
    calls: Vec<ResourceID>,
    extras: Vec<ResourceID>,
    next_extra_deadline: Option<DateTime<Utc>>,
    /// How often the runtime asked for the next extra deadline: once per wait
    extra_deadline_queries: usize,
}

impl TestControllerScript {
    pub fn new(default_behavior: TestControllerBehavior) -> Self {
        Self {
            state: Arc::new(Mutex::new(TestControllerScriptState {
                default_behavior,
                behavior_by_resource: HashMap::new(),
                calls: Vec::new(),
                extras: Vec::new(),
                next_extra_deadline: None,
                extra_deadline_queries: 0,
            })),
            called: Arc::new(Notify::new()),
        }
    }

    pub fn set_behavior_of(&self, id: ResourceID, behavior: TestControllerBehavior) {
        self.lock().behavior_by_resource.insert(id, behavior);
    }

    pub fn set_extras(&self, ids: Vec<ResourceID>) {
        self.lock().extras = ids;
    }

    pub fn set_next_extra_deadline(&self, deadline: Option<DateTime<Utc>>) {
        self.lock().next_extra_deadline = deadline;
    }

    /// How many times the runtime prepared a wait, each asking for the next
    /// extra deadline
    pub fn extra_deadline_queries(&self) -> usize {
        self.lock().extra_deadline_queries
    }

    /// Waits until the controller was asked `total` times in all. Shorter than
    /// the runtime's fallback timeout, so a call it would make on that timeout
    /// does not count.
    pub async fn wait_for_total_calls(&self, total: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while self.total_calls() < total {
                self.called.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {total} process calls, got {}", self.total_calls()));
    }

    pub fn calls_of(&self, id: &ResourceID) -> usize {
        self.lock().calls.iter().filter(|call| *call == id).count()
    }

    pub fn total_calls(&self) -> usize {
        self.lock().calls.len()
    }

    fn record_call(&self, id: ResourceID) -> TestControllerBehavior {
        let behavior = {
            let mut state = self.lock();
            state.calls.push(id);
            state
                .behavior_by_resource
                .get(&id)
                .copied()
                .unwrap_or(state.default_behavior)
        };
        self.called.notify_one();
        behavior
    }

    fn extras(&self, limit: usize) -> Vec<ResourceID> {
        self.lock().extras.iter().take(limit).copied().collect()
    }

    fn next_extra_deadline(&self) -> Option<DateTime<Utc>> {
        let mut state = self.lock();
        state.extra_deadline_queries += 1;
        state.next_extra_deadline
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, TestControllerScriptState> {
        self.state.lock().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const TEST_STAGE_PREPARED: &str = "prepared";
pub const TEST_STAGE_FINALIZING: &str = "finalizing";

#[dill::component]
pub struct TestResourceControllerLogic {
    catalog: dill::Catalog,
    script: Arc<TestControllerScript>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl TestResourceControllerLogic {
    async fn process(&self, id: ResourceID) -> Result<ResourceProcessOutcome, InternalError> {
        let now = self.time_source.now();

        match self.script.record_call(id) {
            TestControllerBehavior::FinishAtCurrentVersion => {
                self.in_transaction(|_, loader| async move {
                    let resource = loader.load(&id).await.int_err()?;
                    Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                        resource.headers(),
                    )))
                })
                .await
            }
            TestControllerBehavior::Stepwise { step_wait } => {
                self.in_transaction(|primitives, loader| async move {
                    let resource = loader.load(&id).await.int_err()?;
                    Self::next_step(primitives.as_ref(), resource, now + step_wait).await
                })
                .await
            }
            TestControllerBehavior::RetryAfter(delay) => {
                Ok(ResourceProcessOutcome::RetryAt(now + delay))
            }
            TestControllerBehavior::Fail => Err(InternalError::new("scripted failure")),
            TestControllerBehavior::Panic => panic!("scripted panic"),
        }
    }

    /// One step of `TestControllerBehavior::Stepwise`
    async fn next_step(
        primitives: &dyn ResourceLifecyclePrimitives<TestResource>,
        resource: TestResource,
        next_step_at: DateTime<Utc>,
    ) -> Result<ResourceProcessOutcome, InternalError> {
        let id = *resource.id();
        let version = ResourceVersion::from(resource.headers());
        let stage = Self::stage_of(&resource);

        if resource.headers().deleted_at.is_some() {
            return Ok(ResourceProcessOutcome::Done(version));
        }

        if resource.headers().deletion_requested_at.is_some() {
            if stage.as_deref() != Some(TEST_STAGE_FINALIZING) {
                primitives
                    .update_conditions(
                        &id,
                        version.status_generation,
                        &Self::stage_patch(TEST_STAGE_FINALIZING),
                    )
                    .await
                    .int_err()?;
                return Ok(ResourceProcessOutcome::RetryAt(next_step_at));
            }

            let completed = primitives
                .complete_deletion(&[ResourceDeletionCompletion {
                    id,
                    expected_status_generation: None,
                }])
                .await
                .int_err()?;
            let deleted = completed.transitioned.first().unwrap();
            return Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
                deleted.headers(),
            )));
        }

        let reconciled_generation = resource.status().reconciled_generation.unwrap_or(0);
        if reconciled_generation >= resource.headers().generation {
            return Ok(ResourceProcessOutcome::Done(version));
        }

        if stage.as_deref() != Some(TEST_STAGE_PREPARED) {
            let status_generation = primitives
                .begin_reconciliation(&id, None)
                .await
                .int_err()?
                .map_or(version.status_generation, |started| {
                    started.headers().status_generation
                });
            primitives
                .update_conditions(
                    &id,
                    status_generation,
                    &Self::stage_patch(TEST_STAGE_PREPARED),
                )
                .await
                .int_err()?;
            return Ok(ResourceProcessOutcome::RetryAt(next_step_at));
        }

        let cleared = primitives
            .update_conditions(
                &id,
                version.status_generation,
                &ResourceConditionsPatch::new().remove(Self::stage_uri()),
            )
            .await
            .int_err()?;
        let completed = primitives
            .complete_reconciliation(&id, ResourceVersion::from(cleared.headers()))
            .await
            .int_err()?;
        Ok(ResourceProcessOutcome::Done(ResourceVersion::from(
            completed.headers(),
        )))
    }

    async fn in_transaction<F, Fut>(&self, f: F) -> Result<ResourceProcessOutcome, InternalError>
    where
        F: FnOnce(
            Arc<dyn ResourceLifecyclePrimitives<TestResource>>,
            Arc<dyn ResourceAggregateLoader<TestResource>>,
        ) -> Fut,
        Fut: Future<Output = Result<ResourceProcessOutcome, InternalError>>,
    {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with2(f)
            .await
    }

    pub fn stage_of(resource: &TestResource) -> Option<String> {
        resource
            .status()
            .conditions
            .entries
            .get(&test_stage_condition_key())
            .and_then(|value| value.as_str())
            .map(ToString::to_string)
    }

    pub fn stage_patch(stage: &str) -> ResourceConditionsPatch {
        ResourceConditionsPatch::new().set(Self::stage_uri(), serde_json::json!(stage))
    }

    fn stage_uri() -> TypeUri {
        TypeUri::new_unchecked(TEST_STAGE_CONDITION_URI)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
