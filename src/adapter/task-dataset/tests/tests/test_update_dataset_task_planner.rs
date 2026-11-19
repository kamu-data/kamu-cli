// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::testing::{BaseRepoHarness, BaseUseCaseHarness, BaseUseCaseHarnessOptions};
use kamu::{
    DatasetPullSourceResolverImpl,
    DatasetUpdateKindResolverImpl,
    PullRequestPlannerImpl,
    RemoteAliasResolverImpl,
    RemoteAliasesRegistryImpl,
    RemoteDatasetUrlResolverImpl,
    RemoteReposDir,
    RemoteRepositoryRegistryImpl,
    TransformPlannerImpl,
};
use kamu_adapter_task_dataset::{
    DatasetUpdateJob,
    LogicalPlanDatasetUpdate,
    TaskDefinitionDatasetUpdate,
    UpdateDatasetTaskPlanner,
};
use kamu_core::RemoteAliasKind;
use kamu_datasets::CreateDatasetResult;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_task_system::{PrepareTaskDefinitionError, TaskDefinitionPlanner, TaskError, TaskID};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_planned_job_follows_dataset_kind_unless_pulled_from_remote() {
    let cases = [
        (odf::DatasetKind::Root, None, "Ingest"),
        (odf::DatasetKind::Derivative, None, "Transform"),
        (
            odf::DatasetKind::Root,
            Some("odf+http://example.com/foo"),
            "Sync",
        ),
    ];

    for (kind, pull_alias, expected_job) in cases {
        let harness = UpdateDatasetTaskPlannerHarness::new();
        let target = harness.create_target(kind).await;
        if let Some(pull_alias) = pull_alias {
            harness
                .add_remote_alias(&target.dataset_handle, pull_alias, RemoteAliasKind::Pull)
                .await;
        }

        let job = harness.plan_update(&target).await.unwrap();

        assert_eq!(
            (
                expected_job,
                &target.dataset_handle,
                pull_alias.map(str::to_string)
            ),
            UpdateDatasetTaskPlannerHarness::job_parts(&job),
            "{kind:?} with pull alias {pull_alias:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_deleted_dataset_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let failure = harness.plan_update(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dataset_with_several_pull_aliases_fails_unrecoverably() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .add_remote_alias(
            &foo.dataset_handle,
            "odf+http://example.com/foo",
            RemoteAliasKind::Pull,
        )
        .await;
    harness
        .add_remote_alias(
            &foo.dataset_handle,
            "odf+http://example.org/foo",
            RemoteAliasKind::Pull,
        )
        .await;

    let failure = harness.plan_update(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_fetch_uncacheable_is_planned_into_the_ingest_options() {
    let alias_foo = BaseRepoHarness::alias("foo");
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let not_fetching = harness.plan_update_definition(&foo, false).await.unwrap();
    let fetching = harness.plan_update_definition(&foo, true).await.unwrap();

    assert_eq!(
        (false, true),
        (
            not_fetching.ingest_options.fetch_uncacheable,
            fetching.ingest_options.fetch_uncacheable
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateDatasetTaskPlannerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    planner: Arc<UpdateDatasetTaskPlanner>,
}

impl UpdateDatasetTaskPlannerHarness {
    fn new() -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(BaseUseCaseHarnessOptions::new());

        let repos_dir = base_use_case_harness.temp_dir_path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        kamu_datasets_inmem::register_dependencies(&mut b);
        b.add::<UpdateDatasetTaskPlanner>()
            .add::<PullRequestPlannerImpl>()
            .add::<DatasetUpdateKindResolverImpl>()
            .add::<DatasetPullSourceResolverImpl>()
            .add::<TransformPlannerImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add::<RemoteDatasetUrlResolverImpl>()
            .add::<RemoteRepositoryRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<odf::dataset::DatasetFactoryImpl>()
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add_value(odf::dataset::IpfsGateway::default())
            .add::<DependencyGraphServiceImpl>();

        let catalog = b.build();

        Self {
            base_use_case_harness,
            planner: catalog.get_one().unwrap(),
        }
    }

    /// Creates a dataset of `kind`: root `foo`, or derivative `bar` of it
    async fn create_target(&self, kind: odf::DatasetKind) -> CreateDatasetResult {
        let foo = self
            .create_root_dataset(&BaseRepoHarness::alias("foo"))
            .await;
        match kind {
            odf::DatasetKind::Root => foo,
            odf::DatasetKind::Derivative => {
                self.create_derived_dataset(
                    &BaseRepoHarness::alias("bar"),
                    vec![foo.dataset_handle.as_local_ref()],
                )
                .await
            }
        }
    }

    /// The job's variant name, its target, and the remote it syncs from
    fn job_parts(job: &DatasetUpdateJob) -> (&'static str, &odf::DatasetHandle, Option<String>) {
        match job {
            DatasetUpdateJob::Ingest(target) => ("Ingest", target, None),
            DatasetUpdateJob::Transform(target) => ("Transform", target, None),
            DatasetUpdateJob::Sync { target, remote_ref } => {
                ("Sync", target, Some(remote_ref.to_string()))
            }
        }
    }

    /// Plans an update of `created`, returning the planned job or the error
    /// the task fails with
    async fn plan_update(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<DatasetUpdateJob, TaskError> {
        self.plan_update_definition(created, false)
            .await
            .map(|definition| definition.job)
    }

    /// Plans an update of `created`, returning the task definition or the error
    /// the task fails with
    async fn plan_update_definition(
        &self,
        created: &CreateDatasetResult,
        fetch_uncacheable: bool,
    ) -> Result<TaskDefinitionDatasetUpdate, TaskError> {
        let logical_plan = LogicalPlanDatasetUpdate {
            dataset_id: created.dataset_handle.id.clone(),
            fetch_uncacheable,
        }
        .into_logical_plan();

        match self
            .planner
            .prepare_task_definition(TaskID::new(1), &logical_plan)
            .await
        {
            Ok(task_definition) => Ok(*task_definition
                .downcast::<TaskDefinitionDatasetUpdate>()
                .unwrap()),
            Err(PrepareTaskDefinitionError::Failed(task_error)) => Err(task_error),
            Err(PrepareTaskDefinitionError::Internal(e)) => {
                panic!("Unexpected internal error: {e}")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
