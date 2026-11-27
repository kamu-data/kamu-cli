// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use dill::CatalogBuilder;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyResourceApplicationDecision,
    ApplyResourceOutcome,
    ApplyResourcePrecondition,
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    ApplyResourceUseCaseError,
    DeclarativeResource,
    EventSourcedResource,
    IntoApplyResourceRejection,
    InvariantViolationOf,
    ManagedResource,
    ResourceAggregateLoader,
    ResourceHeadersValidationError,
    ResourceLinterSpec,
    ResourceNameSuffixGenerator,
    ResourcePersistenceError,
    ResourcePersistenceService,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    ResourceSpecPreApply,
    ResourceSpecPreApplyContext,
    ResourceSpecPreApplyOutcome,
    ResourceValidateSpec,
    TypedResourceQueryService,
};
use kamu_resources_services::testing::{
    BaseResourceServiceHarness,
    ImmutableTestResource,
    ImmutableTestResourceSpec,
    TestResource,
    TestResourceSpec,
    TestTargetRef,
    TestTargets,
    register_immutable_test_resource,
    register_test_resource,
};
use kamu_resources_services::{ApplyResourceRunner, DefaultResourceOperationGuard};
use serde::Serialize;

use crate::tests::utils::make_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lost_create_race_is_applied_as_an_update() {
    let harness = ApplyResourceRunnerHarness::<TestResource>::new(register_test_resource);
    let pre_apply = RecordingPreApply::default();
    let winner = harness.make_resource(spec("theirs"));
    let winner_id = *winner.id();

    let result = harness
        .apply_losing_to(
            [winner],
            spec("mine"),
            &pre_apply,
            ApplyResourcePrecondition::None,
        )
        .await
        .unwrap()
        .expect_applied();

    assert_eq!(result.outcome, ApplyResourceOutcome::Updated);
    assert_eq!(result.id, winner_id);
    assert_eq!(result.state.spec, spec("mine"));
    assert_eq!(result.state.headers.generation, 2);

    // The spec was prepared against the one of the winner
    let account_id = account_handle().did;
    assert_eq!(
        *pre_apply.calls.lock().unwrap(),
        [
            (account_id.clone(), None),
            (account_id, Some(spec("theirs")))
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lost_create_race_with_the_same_spec_changes_nothing() {
    let harness = ApplyResourceRunnerHarness::<TestResource>::new(register_test_resource);
    let winner = harness.make_resource(spec("same"));
    let winner_id = *winner.id();

    let result = harness
        .apply_losing_to(
            [winner],
            spec("same"),
            &RecordingPreApply::default(),
            ApplyResourcePrecondition::None,
        )
        .await
        .unwrap()
        .expect_applied();

    assert_eq!(result.outcome, ApplyResourceOutcome::Untouched);
    assert_eq!(result.id, winner_id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lost_create_race_fails_when_absence_is_required() {
    let harness = ApplyResourceRunnerHarness::<TestResource>::new(register_test_resource);
    let winner = harness.make_resource(spec("theirs"));
    let winner_id = *winner.id();

    assert_matches!(
        harness
            .apply_losing_to(
                [winner],
                spec("mine"),
                &RecordingPreApply::default(),
                ApplyResourcePrecondition::Absent,
            )
            .await,
        Err(ApplyResourceUseCaseError::ConcurrentModification(_))
    );

    let stored = harness.aggregate_loader.load(&winner_id).await.unwrap();
    assert_eq!(*stored.spec(), spec("theirs"));
    assert_eq!(stored.headers().generation, 1);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_race_lost_twice_fails() {
    let harness = ApplyResourceRunnerHarness::<TestResource>::new(register_test_resource);
    let winners = [
        harness.make_resource(spec("theirs")),
        harness.make_resource(spec("theirs")),
    ];

    assert_matches!(
        harness
            .apply_losing_to(
                winners,
                spec("mine"),
                &RecordingPreApply::default(),
                ApplyResourcePrecondition::None,
            )
            .await,
        Err(ApplyResourceUseCaseError::Internal(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_lost_create_race_of_an_immutable_spec() {
    // The winner stored the target under the name it had before a rename
    let winner_spec = ImmutableTestResourceSpec {
        target: TestTargetRef {
            id: Some(TestTargets::ID.to_string()),
            name: Some("before-rename".to_string()),
        },
        mode: Some(ImmutableTestResourceSpec::DEFAULT_MODE.to_string()),
    };
    let lost_spec = |mode: Option<&str>| ImmutableTestResourceSpec {
        target: TestTargetRef {
            id: Some(TestTargets::ID.to_string()),
            name: None,
        },
        mode: mode.map(ToString::to_string),
    };

    for moment in [RaceMoment::Preparation, RaceMoment::Create] {
        let apply = async |lost_spec| {
            let harness = ApplyResourceRunnerHarness::<ImmutableTestResource>::new(
                register_immutable_test_resource,
            );
            let pre_apply = harness.registered_pre_apply();
            harness
                .apply_losing_at(
                    moment,
                    harness.make_resource(winner_spec.clone()),
                    lost_spec,
                    pre_apply.as_ref(),
                )
                .await
                .unwrap()
        };

        let same = apply(lost_spec(None)).await.expect_applied();
        assert_eq!(same.outcome, ApplyResourceOutcome::Untouched, "{moment:?}");
        assert_eq!(same.state.spec, winner_spec, "{moment:?}");

        assert_matches!(
            apply(lost_spec(Some("manual"))).await,
            ApplyResourceApplicationDecision::Rejected(ApplyResourceRejection {
                category: ApplyResourceRejectionCategory::ImmutableFieldChanged,
                ..
            }),
            "{moment:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_resource_changed_during_preparation_fails() {
    for change in [
        |resource: &mut TestResource| {
            let generation = resource.headers().generation + 1;
            <TestResource as ManagedResource>::try_update_spec(
                resource,
                Utc::now(),
                spec("theirs"),
                generation,
            )
            .unwrap();
        },
        |resource: &mut TestResource| resource.try_request_deletion(Utc::now()).unwrap(),
    ] {
        let harness = ApplyResourceRunnerHarness::<TestResource>::new(register_test_resource);
        let mut resource = harness.make_resource(spec("original"));
        harness.persistence_svc.create(&mut resource).await.unwrap();
        change(&mut resource);

        let pre_apply = RecordingPreApply::default();
        assert_matches!(
            harness
                .apply_losing_at(RaceMoment::Preparation, resource, spec("mine"), &pre_apply,)
                .await,
            Err(ApplyResourceUseCaseError::ConcurrentModification(_))
        );

        // The spec was prepared against the one before the change
        assert_eq!(
            *pre_apply.calls.lock().unwrap(),
            [(account_handle().did, Some(spec("original")))]
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ApplyResourceRunnerHarness<R: EventSourcedResource> {
    base: BaseResourceServiceHarness,
    typed_query_svc: Arc<dyn TypedResourceQueryService<R>>,
    aggregate_loader: Arc<dyn ResourceAggregateLoader<R>>,
    persistence_svc: Arc<dyn ResourcePersistenceService<R>>,
    name_suffix_generator: Arc<dyn ResourceNameSuffixGenerator>,
    default_operation_guard: Arc<DefaultResourceOperationGuard>,
}

impl<R> ApplyResourceRunnerHarness<R>
where
    R: EventSourcedResource + ResourceSchemaProvider + 'static,
    R::Spec: Serialize + PartialEq + Clone + ResourceSpecFromInput<R::SpecInput>,
    R::SpecInput: ResourceValidateSpec + ResourceLinterSpec + Clone,
    R::LifecycleError: InvariantViolationOf<R::ResourceState>
        + From<ResourceHeadersValidationError>
        + From<<R::SpecInput as ResourceValidateSpec>::ValidationError>
        + IntoApplyResourceRejection
        + std::fmt::Debug,
{
    fn new(register_resource: impl FnOnce(&mut CatalogBuilder)) -> Self {
        let base = BaseResourceServiceHarness::new_with_additional_dependencies(register_resource);
        let catalog = base.catalog();

        Self {
            typed_query_svc: catalog.get_one().unwrap(),
            aggregate_loader: catalog.get_one().unwrap(),
            persistence_svc: catalog.get_one().unwrap(),
            name_suffix_generator: catalog.get_one().unwrap(),
            default_operation_guard: catalog.get_one().unwrap(),
            base,
        }
    }

    /// The type's own [`ResourceSpecPreApply`], as the catalog registers it
    fn registered_pre_apply(&self) -> Arc<dyn ResourceSpecPreApply<R>> {
        self.base.catalog().get_one().unwrap()
    }

    fn make_resource(&self, spec: R::SpecInput) -> R {
        <R as ManagedResource>::try_create(
            Utc::now(),
            make_id(),
            BaseResourceServiceHarness::make_headers_input(account_handle(), "res-a"),
            spec,
            &BaseResourceServiceHarness::make_provenance(),
        )
        .unwrap()
    }

    /// Applies `res-a`, with each of its creates losing to the next of
    /// `winners`, all but the last of which are deleted afterwards
    async fn apply_losing_to(
        &self,
        winners: impl IntoIterator<Item = R>,
        spec: R::SpecInput,
        pre_apply: &dyn ResourceSpecPreApply<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        let persistence_svc = RacingPersistenceService {
            inner: self.persistence_svc.as_ref(),
            winners: Mutex::new(winners.into_iter().collect()),
        };
        self.apply(spec, pre_apply, &persistence_svc, precondition)
            .await
    }

    /// Applies `res-a`, with another writer writing `winner` at `moment`
    async fn apply_losing_at(
        &self,
        moment: RaceMoment,
        winner: R,
        spec: R::SpecInput,
        pre_apply: &dyn ResourceSpecPreApply<R>,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        match moment {
            RaceMoment::Preparation => {
                let pre_apply = RacingPreApply {
                    inner: pre_apply,
                    persistence_svc: self.persistence_svc.as_ref(),
                    maybe_winner: Mutex::new(Some(winner)),
                };
                self.apply(
                    spec,
                    &pre_apply,
                    self.persistence_svc.as_ref(),
                    ApplyResourcePrecondition::None,
                )
                .await
            }
            RaceMoment::Create => {
                self.apply_losing_to([winner], spec, pre_apply, ApplyResourcePrecondition::None)
                    .await
            }
        }
    }

    async fn apply(
        &self,
        spec: R::SpecInput,
        pre_apply: &dyn ResourceSpecPreApply<R>,
        persistence_svc: &dyn ResourcePersistenceService<R>,
        precondition: ApplyResourcePrecondition,
    ) -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>> {
        let runner = ApplyResourceRunner {
            generic_resource_query_service: self.base.generic_query_svc(),
            typed_resource_query_service: self.typed_query_svc.as_ref(),
            resource_aggregate_loader: self.aggregate_loader.as_ref(),
            resource_persistence_service: persistence_svc,
            maybe_resource_spec_pre_apply: Some(pre_apply),
            maybe_resource_placement: None,
            maybe_resource_operation_guard: None,
            default_resource_operation_guard: self.default_operation_guard.as_ref(),
            name_suffix_generator: self.name_suffix_generator.as_ref(),
            outbox: self.base.outbox(),
            time_source: self.base.time_source(),
        };

        runner
            .apply(
                BaseResourceServiceHarness::make_apply_params(
                    account_handle(),
                    Some("res-a"),
                    spec,
                ),
                precondition,
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn account_handle() -> odf::ResourceOwnerHandle {
    odf::ResourceOwnerHandle::new_test("test")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn spec(value: &str) -> TestResourceSpec {
    TestResourceSpec {
        value: value.to_string(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// When another writer gets ahead of an apply
#[derive(Debug, Clone, Copy)]
enum RaceMoment {
    /// After the current spec was read
    Preparation,
    /// Just before the resource is created
    Create,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the account and the current spec of each preparation
#[derive(Default)]
struct RecordingPreApply {
    calls: Mutex<Vec<(odf::AccountID, Option<TestResourceSpec>)>>,
}

#[async_trait::async_trait]
impl ResourceSpecPreApply<TestResource> for RecordingPreApply {
    async fn pre_apply(
        &self,
        new_spec: TestResourceSpec,
        context: ResourceSpecPreApplyContext<'_, TestResource>,
    ) -> Result<ResourceSpecPreApplyOutcome<TestResource>, InternalError> {
        self.calls.lock().unwrap().push((
            context.account_id.clone(),
            context.maybe_current_spec.cloned(),
        ));
        Ok(ResourceSpecPreApplyOutcome::Prepared(new_spec))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plays another writer, writing its resource during the first preparation
struct RacingPreApply<'a, R: EventSourcedResource> {
    inner: &'a dyn ResourceSpecPreApply<R>,
    persistence_svc: &'a dyn ResourcePersistenceService<R>,
    maybe_winner: Mutex<Option<R>>,
}

#[async_trait::async_trait]
impl<R: EventSourcedResource> ResourceSpecPreApply<R> for RacingPreApply<'_, R> {
    async fn pre_apply(
        &self,
        new_spec: R::SpecInput,
        context: ResourceSpecPreApplyContext<'_, R>,
    ) -> Result<ResourceSpecPreApplyOutcome<R>, InternalError> {
        let is_new = context.maybe_current_spec.is_none();
        let outcome = self.inner.pre_apply(new_spec, context).await?;

        let maybe_winner = self.maybe_winner.lock().unwrap().take();
        if let Some(mut winner) = maybe_winner {
            if is_new {
                self.persistence_svc.create(&mut winner).await.int_err()?;
            } else {
                self.persistence_svc.save(&mut winner).await.int_err()?;
            }
        }

        Ok(outcome)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plays other writers, each creating its resource just before a create
struct RacingPersistenceService<'a, R: EventSourcedResource> {
    inner: &'a dyn ResourcePersistenceService<R>,
    winners: Mutex<VecDeque<R>>,
}

#[async_trait::async_trait]
impl<R> ResourcePersistenceService<R> for RacingPersistenceService<'_, R>
where
    R: EventSourcedResource,
    R::LifecycleError: std::fmt::Debug,
{
    async fn create(&self, resource: &mut R) -> Result<(), ResourcePersistenceError> {
        let (maybe_winner, is_last) = {
            let mut winners = self.winners.lock().unwrap();
            (winners.pop_front(), winners.is_empty())
        };
        let Some(mut winner) = maybe_winner else {
            return self.inner.create(resource).await;
        };

        self.inner.create(&mut winner).await?;
        let result = self.inner.create(resource).await;
        if !is_last {
            winner.try_request_deletion(Utc::now()).unwrap();
            self.inner.save(&mut winner).await?;
        }
        result
    }

    async fn save(&self, resource: &mut R) -> Result<(), ResourcePersistenceError> {
        self.inner.save(resource).await
    }

    async fn save_many(&self, resources: &mut [R]) -> Result<(), ResourcePersistenceError> {
        self.inner.save_many(resources).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
