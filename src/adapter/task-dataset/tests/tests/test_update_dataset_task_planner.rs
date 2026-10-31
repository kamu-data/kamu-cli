// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions};
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
use kamu_core::{RemoteAliasKind, RemoteAliasesRegistry};
use kamu_datasets::CreateDatasetResult;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_task_system::{PrepareTaskDefinitionError, TaskDefinitionPlanner, TaskError, TaskID};
use pretty_assertions::assert_eq;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_root_dataset_plans_an_ingest() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;

    let job = harness.plan_update(&foo).await.unwrap();

    assert_matches!(job, DatasetUpdateJob::Ingest(target) if target == foo.dataset_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_derived_dataset_plans_a_transform() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness
        .create_derived_dataset(&alias_bar, vec![foo.dataset_handle.as_local_ref()])
        .await;

    let job = harness.plan_update(&bar).await.unwrap();

    assert_matches!(job, DatasetUpdateJob::Transform(target) if target == bar.dataset_handle);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dataset_with_one_pull_alias_plans_a_sync() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .add_pull_alias(&foo, "odf+http://example.com/foo")
        .await;

    let job = harness.plan_update(&foo).await.unwrap();

    assert_matches!(
        job,
        DatasetUpdateJob::Sync { target, remote_ref }
        if target == foo.dataset_handle && remote_ref.to_string() == "odf+http://example.com/foo"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_deleted_dataset_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness.delete_dataset(&foo).await;

    let failure = harness.plan_update(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_dataset_with_several_pull_aliases_fails_unrecoverably() {
    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let harness = UpdateDatasetTaskPlannerHarness::new();
    let foo = harness.create_root_dataset(&alias_foo).await;
    harness
        .add_pull_alias(&foo, "odf+http://example.com/foo")
        .await;
    harness
        .add_pull_alias(&foo, "odf+http://example.org/foo")
        .await;

    let failure = harness.plan_update(&foo).await.unwrap_err();

    assert_eq!(TaskError::empty_unrecoverable(), failure);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct UpdateDatasetTaskPlannerHarness {
    base_use_case_harness: BaseUseCaseHarness,
    planner: Arc<UpdateDatasetTaskPlanner>,
    remote_aliases_registry: Arc<dyn RemoteAliasesRegistry>,
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

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            base_use_case_harness,
            planner: catalog.get_one().unwrap(),
            remote_aliases_registry: catalog.get_one().unwrap(),
        }
    }

    /// Deletes the dataset's storage, as if it was deleted after the task was
    /// scheduled
    async fn delete_dataset(&self, created: &CreateDatasetResult) {
        self.dataset_storage_unit_writer()
            .delete_dataset(&created.dataset_handle.id)
            .await
            .unwrap();
    }

    async fn add_pull_alias(&self, created: &CreateDatasetResult, remote_ref: &str) {
        self.remote_aliases_registry
            .get_remote_aliases(&created.dataset_handle)
            .await
            .unwrap()
            .add(&remote_ref.parse().unwrap(), RemoteAliasKind::Pull)
            .await
            .unwrap();
    }

    /// Plans an update of `created`, returning the planned job or the error
    /// the task fails with
    async fn plan_update(
        &self,
        created: &CreateDatasetResult,
    ) -> Result<DatasetUpdateJob, TaskError> {
        let logical_plan = LogicalPlanDatasetUpdate {
            dataset_id: created.dataset_handle.id.clone(),
            fetch_uncacheable: false,
        }
        .into_logical_plan();

        match self
            .planner
            .prepare_task_definition(TaskID::new(1), &logical_plan)
            .await
        {
            Ok(task_definition) => Ok(task_definition
                .downcast::<TaskDefinitionDatasetUpdate>()
                .unwrap()
                .job),
            Err(PrepareTaskDefinitionError::Failed(task_error)) => Err(task_error),
            Err(PrepareTaskDefinitionError::Internal(e)) => {
                panic!("Unexpected internal error: {e}")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
