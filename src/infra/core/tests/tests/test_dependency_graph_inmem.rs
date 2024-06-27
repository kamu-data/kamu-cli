// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::Component;
use event_bus::EventBus;
use futures::{future, StreamExt, TryStreamExt};
use internal_error::ResultIntoInternal;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetRepositoryLocalFs,
    DependencyGraphRepositoryInMemory,
    DependencyGraphServiceInMemory,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    auth,
    DatasetDependencies,
    DatasetRepository,
    DependencyGraphRepository,
    DependencyGraphService,
    SystemTimeSourceDefault,
};
use opendatafabric::{
    AccountName,
    DatasetAlias,
    DatasetID,
    DatasetKind,
    DatasetName,
    MetadataEvent,
};
use tempfile::TempDir;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_single_tenant_repository() {
    let harness = DependencyGraphHarness::new(false);

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(&all_dependencies),
        ""
    );

    harness.create_single_tenant_graph().await;

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(&all_dependencies),
        indoc::indoc!(
            r#"
            bar -> foo-bar
            baz -> foo-baz
            foo -> foo-bar
            foo -> foo-baz
            foo-bar -> foo-bar-foo-baz
            foo-baz -> foo-bar-foo-baz"#
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_tenant_repository() {
    let harness = DependencyGraphHarness::new(true);

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(&all_dependencies),
        ""
    );

    harness.create_multi_tenant_graph().await;

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(&all_dependencies),
        indoc::indoc!(
            r#"
            alice/bar -> alice/foo-bar
            alice/foo -> alice/foo-bar
            alice/foo -> bob/foo-baz
            alice/foo-bar -> eve/foo-bar-foo-baz
            bob/baz -> bob/foo-baz
            bob/foo-baz -> eve/foo-bar-foo-baz"#
        ),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_service_queries() {
    let harness = DependencyGraphHarness::new(false);
    harness.create_single_tenant_graph().await;
    harness.eager_initialization().await;

    assert_eq!(
        harness.dataset_dependencies_report("foo").await,
        "[] -> foo -> [foo-bar, foo-baz]"
    );

    assert_eq!(
        harness.dataset_dependencies_report("bar").await,
        "[] -> bar -> [foo-bar]"
    );

    assert_eq!(
        harness.dataset_dependencies_report("baz").await,
        "[] -> baz -> [foo-baz]"
    );

    assert_eq!(
        harness.dataset_dependencies_report("foo-bar").await,
        "[bar, foo] -> foo-bar -> [foo-bar-foo-baz]"
    );

    assert_eq!(
        harness.dataset_dependencies_report("foo-baz").await,
        "[baz, foo] -> foo-baz -> [foo-bar-foo-baz]"
    );

    assert_eq!(
        harness.dataset_dependencies_report("foo-bar-foo-baz").await,
        "[foo-bar, foo-baz] -> foo-bar-foo-baz -> []"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_service_new_datasets() {
    let harness = DependencyGraphHarness::new(false);
    harness.create_single_tenant_graph().await;
    harness.eager_initialization().await;

    harness.create_root_dataset(None, "test-root").await;

    assert_eq!(
        harness.dataset_dependencies_report("test-root").await,
        "[] -> test-root -> []"
    );
    assert_eq!(
        harness.dataset_dependencies_report("foo").await,
        "[] -> foo -> [foo-bar, foo-baz]"
    );

    harness
        .create_derived_dataset(
            None,
            "test-deriv",
            vec![
                DatasetAlias::new(None, DatasetName::new_unchecked("foo")),
                DatasetAlias::new(None, DatasetName::new_unchecked("test-root")),
            ],
        )
        .await;

    assert_eq!(
        harness.dataset_dependencies_report("test-root").await,
        "[] -> test-root -> [test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("test-deriv").await,
        "[foo, test-root] -> test-deriv -> []"
    );
    assert_eq!(
        harness.dataset_dependencies_report("foo").await,
        "[] -> foo -> [foo-bar, foo-baz, test-deriv]"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_service_derived_dataset_modifies_links() {
    let harness = DependencyGraphHarness::new(false);
    harness.create_single_tenant_graph().await;
    harness.eager_initialization().await;

    assert_eq!(
        harness.dataset_dependencies_report("bar").await,
        "[] -> bar -> [foo-bar]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("baz").await,
        "[] -> baz -> [foo-baz]"
    );

    // Initially "test-deriv" will have 2 upstream dependencies: "bar" and "baz"
    harness
        .create_derived_dataset(
            None,
            "test-deriv",
            vec![
                DatasetAlias::new(None, DatasetName::new_unchecked("bar")),
                DatasetAlias::new(None, DatasetName::new_unchecked("baz")),
            ],
        )
        .await;

    assert_eq!(
        harness.dataset_dependencies_report("bar").await,
        "[] -> bar -> [foo-bar, test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("baz").await,
        "[] -> baz -> [foo-baz, test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("test-deriv").await,
        "[bar, baz] -> test-deriv -> []"
    );

    // Drop "baz" dependency
    harness
        .modify_derived_dataset(
            None,
            "test-deriv",
            vec![DatasetAlias::new(None, DatasetName::new_unchecked("bar"))],
        )
        .await;

    // Confirm we only have "bar" left
    assert_eq!(
        harness.dataset_dependencies_report("bar").await,
        "[] -> bar -> [foo-bar, test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("baz").await,
        "[] -> baz -> [foo-baz]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("test-deriv").await,
        "[bar] -> test-deriv -> []"
    );

    // Add "baz" dependency back
    harness
        .modify_derived_dataset(
            None,
            "test-deriv",
            vec![
                DatasetAlias::new(None, DatasetName::new_unchecked("bar")),
                DatasetAlias::new(None, DatasetName::new_unchecked("baz")),
            ],
        )
        .await;

    // Confirm we both "bar" and "baz" now
    assert_eq!(
        harness.dataset_dependencies_report("bar").await,
        "[] -> bar -> [foo-bar, test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("baz").await,
        "[] -> baz -> [foo-baz, test-deriv]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("test-deriv").await,
        "[bar, baz] -> test-deriv -> []"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_service_dataset_deleted() {
    let harness = DependencyGraphHarness::new(false);
    harness.create_single_tenant_graph().await;
    harness.eager_initialization().await;

    assert_eq!(
        harness.dataset_dependencies_report("foo-bar").await,
        "[bar, foo] -> foo-bar -> [foo-bar-foo-baz]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("foo-baz").await,
        "[baz, foo] -> foo-baz -> [foo-bar-foo-baz]"
    );
    assert_eq!(
        harness.dataset_dependencies_report("foo-bar-foo-baz").await,
        "[foo-bar, foo-baz] -> foo-bar-foo-baz -> []"
    );

    harness
        .dataset_repo
        .delete_dataset(
            &DatasetAlias::new(None, DatasetName::new_unchecked("foo-bar-foo-baz")).as_local_ref(),
        )
        .await
        .unwrap();

    assert_eq!(
        harness.dataset_dependencies_report("foo-bar").await,
        "[bar, foo] -> foo-bar -> []"
    );
    assert_eq!(
        harness.dataset_dependencies_report("foo-baz").await,
        "[baz, foo] -> foo-baz -> []"
    );
}

#[test_log::test(tokio::test)]
async fn test_get_recursive_downstream_dependencies() {
    let harness = create_large_dataset_graph().await;

    // Request downstream dependencies for last element
    // should return only itself
    let request_dataset = "test-derive-foo-foo-foo-bar-foo-bar";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;
    assert_eq!(result, vec![request_dataset]);

    let request_dataset = "test-derive-baz-baz-foo-bar";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    assert_eq!(result, vec![request_dataset]);

    let request_dataset = "test-derive-bar-baz";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    assert_eq!(result, vec![request_dataset]);

    // Request downstream dependencies for element with 1 dependency
    // should return dependency and itself at the end
    let request_dataset = "test-derive-foo-foo-foo-bar-foo";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec!["test-derive-foo-foo-foo-bar-foo-bar", request_dataset];
    assert_eq!(result, expected_result);

    let request_dataset = "test-derive-baz-baz-foo";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec!["test-derive-baz-baz-foo-bar", request_dataset];
    assert_eq!(result, expected_result);

    // Request downstream dependencies for element with 2 dependencies
    // should return dependencies(from last to first) and itself at the end
    let request_dataset = "test-root-bar";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec![
        "test-derive-bar-baz",
        "test-derive-bar-bar",
        request_dataset,
    ];
    assert_eq!(result, expected_result);

    let request_dataset = "test-derive-baz-baz";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec![
        "test-derive-baz-baz-foo-bar",
        "test-derive-baz-baz-foo",
        request_dataset,
    ];
    assert_eq!(result, expected_result);

    let request_dataset = "test-derive-foo-foo-foo-bar";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec![
        "test-derive-foo-foo-foo-bar-foo-bar",
        "test-derive-foo-foo-foo-bar-foo",
        request_dataset,
    ];
    assert_eq!(result, expected_result);

    // Request downstream dependencies for element with 3 dependencies
    // should return dependencies(from last to first) and itself at the end
    let request_dataset = "test-root-baz";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec![
        "test-derive-baz-baz-foo-bar",
        "test-derive-baz-baz-foo",
        "test-derive-baz-baz",
        request_dataset,
    ];
    assert_eq!(result, expected_result);

    // Request downstream dependencies for element with 9 dependencies
    // should return dependencies(from last to first) and itself at the end
    let request_dataset = "test-root-foo";
    let result = harness
        .get_recursive_downstream_dependencies(vec![request_dataset])
        .await;

    let expected_result = vec![
        "test-derive-foo-baz",
        "test-derive-foo-bar",
        "test-derive-foo-foo-bar",
        "test-derive-foo-foo-foo-baz",
        "test-derive-foo-foo-foo-bar-foo-bar",
        "test-derive-foo-foo-foo-bar-foo",
        "test-derive-foo-foo-foo-bar",
        "test-derive-foo-foo-foo",
        "test-derive-foo-foo",
        request_dataset,
    ];
    assert_eq!(result, expected_result);

    // Request downstream dependencies for 2 elements with 2 dependencies
    // should return dependencies(from last to first) and requested dataset
    // at the end of each chain
    let request_datasets = vec!["test-root-bar", "test-derive-baz-baz"];
    let result = harness
        .get_recursive_downstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![
        "test-derive-bar-baz",
        "test-derive-bar-bar",
        request_datasets[0],
        "test-derive-baz-baz-foo-bar",
        "test-derive-baz-baz-foo",
        request_datasets[1],
    ];
    assert_eq!(result, expected_result);

    // Request downstream dependencies for 2 elements with 2 dependencies
    // and one element which is included in chain of dependencies from previous
    // should return dependencies(from last to first) and requested dataset
    // at the end of each chain without duplicates
    let request_datasets = vec![
        "test-derive-baz-baz-foo-bar",
        "test-root-bar",
        "test-derive-baz-baz",
    ];
    let result = harness
        .get_recursive_downstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![
        request_datasets[0],
        "test-derive-bar-baz",
        "test-derive-bar-bar",
        request_datasets[1],
        "test-derive-baz-baz-foo",
        request_datasets[2],
    ];
    assert_eq!(result, expected_result);

    // Add additional dataset which will be derived to 2 root nodes in graph
    harness
        .create_derived_dataset(
            None,
            "test-derive-multiple-bar-baz",
            vec![
                DatasetAlias::new(None, DatasetName::new_unchecked("test-root-bar")),
                DatasetAlias::new(None, DatasetName::new_unchecked("test-root-baz")),
            ],
        )
        .await;

    // Request dependencies for 2 root nodes with duplicate dependencies
    // duplicate should be in result only once in the first chain
    let request_datasets = vec!["test-root-bar", "test-root-baz"];
    let result = harness
        .get_recursive_downstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![
        "test-derive-multiple-bar-baz",
        "test-derive-bar-baz",
        "test-derive-bar-bar",
        request_datasets[0],
        "test-derive-baz-baz-foo-bar",
        "test-derive-baz-baz-foo",
        "test-derive-baz-baz",
        request_datasets[1],
    ];
    assert_eq!(result, expected_result);

    let request_datasets = vec!["test-derive-foo-foo-foo"];
    let result = harness
        .get_recursive_downstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![
        "test-derive-foo-foo-foo-baz",
        "test-derive-foo-foo-foo-bar-foo-bar",
        "test-derive-foo-foo-foo-bar-foo",
        "test-derive-foo-foo-foo-bar",
        request_datasets[0],
    ];

    assert_eq!(result, expected_result);
}

#[test_log::test(tokio::test)]
async fn test_get_recursive_upstream_dependencies() {
    let harness = create_large_dataset_graph().await;

    // Should return only itself
    let request_datasets = vec!["test-root-foo"];
    let result = harness
        .get_recursive_upstream_dependencies(request_datasets.clone())
        .await;

    assert_eq!(result, request_datasets);

    let request_datasets = vec!["test-root-foo", "test-root-bar", "test-root-baz"];
    let result = harness
        .get_recursive_upstream_dependencies(request_datasets.clone())
        .await;

    assert_eq!(result, request_datasets);

    // Should correctly return result for root dataset and
    // one of coming node
    let request_datasets = vec!["test-root-foo", "test-derive-foo-foo"];
    let result = harness
        .get_recursive_upstream_dependencies(request_datasets.clone())
        .await;

    assert_eq!(result, request_datasets);

    let request_datasets = vec!["test-root-bar", "test-derive-foo-foo"];
    let result = harness
        .get_recursive_upstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![request_datasets[0], "test-root-foo", request_datasets[1]];

    assert_eq!(result, expected_result);

    let request_datasets = vec!["test-root-foo", "test-derive-foo-foo-foo-bar-foo-bar"];
    let result = harness
        .get_recursive_upstream_dependencies(request_datasets.clone())
        .await;

    let expected_result = vec![
        request_datasets[0],
        "test-derive-foo-foo",
        "test-derive-foo-foo-foo",
        "test-derive-foo-foo-foo-bar",
        "test-derive-foo-foo-foo-bar-foo",
        request_datasets[1],
    ];

    assert_eq!(result, expected_result);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DependencyGraphHarness {
    _workdir: TempDir,
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dependency_graph_repository: Arc<dyn DependencyGraphRepository>,
}

impl DependencyGraphHarness {
    fn new(multi_tenant: bool) -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let datasets_dir = workdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let dependency_graph_service = catalog.get_one::<dyn DependencyGraphService>().unwrap();

        // Note: don't place into catalog, avoid cyclic dependency
        let dependency_graph_repository =
            Arc::new(DependencyGraphRepositoryInMemory::new(dataset_repo.clone()));

        Self {
            _workdir: workdir,
            _catalog: catalog,
            dataset_repo,
            dependency_graph_service,
            dependency_graph_repository,
        }
    }

    async fn list_all_dependencies(&self) -> Vec<(String, String)> {
        let dependencies: Vec<_> = self
            .dependency_graph_repository
            .list_dependencies_of_all_datasets()
            .try_collect()
            .await
            .unwrap();

        let mut res = Vec::new();
        for dataset_dependencies in dependencies {
            let DatasetDependencies {
                downstream_dataset_id,
                upstream_dataset_ids,
            } = dataset_dependencies;

            let downstream_hdl = self
                .dataset_repo
                .resolve_dataset_ref(&downstream_dataset_id.as_local_ref())
                .await
                .unwrap();

            for upstream_dataset_id in upstream_dataset_ids {
                let upstream_hdl = self
                    .dataset_repo
                    .resolve_dataset_ref(&upstream_dataset_id.as_local_ref())
                    .await
                    .unwrap();

                res.push((
                    format!("{}", upstream_hdl.alias),
                    format!("{}", downstream_hdl.alias),
                ));
            }
        }

        res.sort();
        res
    }

    fn all_dependencies_report(dependencies: &[(String, String)]) -> String {
        dependencies
            .iter()
            .map(|(name1, name2)| format!("{name1} -> {name2}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn eager_initialization(&self) {
        self.dependency_graph_service
            .eager_initialization(self.dependency_graph_repository.as_ref())
            .await
            .unwrap();
    }

    async fn dataset_dependencies_report(&self, dataset_name: &str) -> String {
        let downstream = self.get_downstream_dependencies(dataset_name).await;
        let upstream = self.get_upstream_dependencies(dataset_name).await;

        format!(
            "[{}] -> {} -> [{}]",
            upstream.join(", "),
            dataset_name,
            downstream.join(", "),
        )
    }

    async fn get_downstream_dependencies(&self, dataset_name: &str) -> Vec<String> {
        let dataset_id = self.dataset_id_by_name(dataset_name).await;

        let downstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(&dataset_id)
            .await
            .int_err()
            .unwrap()
            .collect()
            .await;

        let mut res = Vec::new();
        for downstream_dataset_id in downstream_dataset_ids {
            let dataset_alias = self.dataset_alias_by_id(&downstream_dataset_id).await;
            res.push(format!("{dataset_alias}"));
        }

        res.sort();
        res
    }

    async fn get_recursive_downstream_dependencies(&self, dataset_names: Vec<&str>) -> Vec<String> {
        let dataset_ids: Vec<_> = future::join_all(
            dataset_names
                .iter()
                .map(|dataset_name| async { self.dataset_id_by_name(dataset_name).await }),
        )
        .await;

        let downstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_recursive_downstream_dependencies(dataset_ids)
            .await
            .int_err()
            .unwrap()
            .collect()
            .await;

        let mut res = Vec::new();
        for downstream_dataset_id in downstream_dataset_ids {
            let dataset_alias = self.dataset_alias_by_id(&downstream_dataset_id).await;
            res.push(format!("{dataset_alias}"));
        }

        res
    }

    async fn get_recursive_upstream_dependencies(&self, dataset_names: Vec<&str>) -> Vec<String> {
        let dataset_ids: Vec<_> = future::join_all(
            dataset_names
                .iter()
                .map(|dataset_name| async { self.dataset_id_by_name(dataset_name).await }),
        )
        .await;

        let upstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_recursive_upstream_dependencies(dataset_ids)
            .await
            .int_err()
            .unwrap()
            .collect()
            .await;

        let mut res = Vec::new();
        for upstream_dataset_id in upstream_dataset_ids {
            let dataset_alias = self.dataset_alias_by_id(&upstream_dataset_id).await;
            res.push(format!("{dataset_alias}"));
        }

        res
    }

    async fn get_upstream_dependencies(&self, dataset_name: &str) -> Vec<String> {
        let dataset_id = self.dataset_id_by_name(dataset_name).await;

        let upstream_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_upstream_dependencies(&dataset_id)
            .await
            .int_err()
            .unwrap()
            .collect()
            .await;

        let mut res = Vec::new();
        for upstream_dataset_id in upstream_dataset_ids {
            let dataset_alias = self.dataset_alias_by_id(&upstream_dataset_id).await;
            res.push(format!("{dataset_alias}"));
        }

        res.sort();
        res
    }

    async fn dataset_id_by_name(&self, dataset_name: &str) -> DatasetID {
        let dataset_alias = DatasetAlias::try_from(dataset_name).unwrap();
        let dataset_hdl = self
            .dataset_repo
            .resolve_dataset_ref(&dataset_alias.as_local_ref())
            .await
            .unwrap();
        dataset_hdl.id
    }

    async fn dataset_alias_by_id(&self, dataset_id: &DatasetID) -> DatasetAlias {
        let dataset_ref = dataset_id.as_local_ref();
        let dataset_hdl = self
            .dataset_repo
            .resolve_dataset_ref(&dataset_ref)
            .await
            .unwrap();
        dataset_hdl.alias
    }

    async fn create_single_tenant_graph(&self) {
        self.create_graph(|_| None).await;
    }

    async fn create_multi_tenant_graph(&self) {
        let alice = AccountName::new_unchecked("alice");
        let bob = AccountName::new_unchecked("bob");
        let eve: AccountName = AccountName::new_unchecked("eve");

        let mut dataset_accounts: HashMap<&'static str, AccountName> = HashMap::new();
        dataset_accounts.insert("foo", alice.clone());
        dataset_accounts.insert("bar", alice.clone());
        dataset_accounts.insert("baz", bob.clone());
        dataset_accounts.insert("foo-bar", alice);
        dataset_accounts.insert("foo-baz", bob);
        dataset_accounts.insert("foo-bar-foo-baz", eve);

        self.create_graph(|dataset_name| dataset_accounts.get(dataset_name).cloned())
            .await;
    }

    async fn create_graph(&self, account_getter: impl Fn(&str) -> Option<AccountName>) {
        self.create_root_dataset(account_getter("foo"), "foo").await;
        self.create_root_dataset(account_getter("bar"), "bar").await;
        self.create_root_dataset(account_getter("baz"), "baz").await;

        self.create_derived_dataset(
            account_getter("foo-bar"),
            "foo-bar",
            vec![
                DatasetAlias::new(account_getter("foo"), DatasetName::new_unchecked("foo")),
                DatasetAlias::new(account_getter("bar"), DatasetName::new_unchecked("bar")),
            ],
        )
        .await;

        self.create_derived_dataset(
            account_getter("foo-baz"),
            "foo-baz",
            vec![
                DatasetAlias::new(account_getter("foo"), DatasetName::new_unchecked("foo")),
                DatasetAlias::new(account_getter("baz"), DatasetName::new_unchecked("baz")),
            ],
        )
        .await;

        self.create_derived_dataset(
            account_getter("foo-bar-foo-baz"),
            "foo-bar-foo-baz",
            vec![
                DatasetAlias::new(
                    account_getter("foo-bar"),
                    DatasetName::new_unchecked("foo-bar"),
                ),
                DatasetAlias::new(
                    account_getter("foo-baz"),
                    DatasetName::new_unchecked("foo-baz"),
                ),
            ],
        )
        .await;
    }

    async fn create_root_dataset(&self, account_name: Option<AccountName>, dataset_name: &str) {
        self.dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(
                        account_name,
                        DatasetName::new_unchecked(dataset_name),
                    ))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn create_derived_dataset(
        &self,
        account_name: Option<AccountName>,
        dataset_name: &str,
        input_aliases: Vec<DatasetAlias>,
    ) {
        self.dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(DatasetAlias::new(
                        account_name,
                        DatasetName::new_unchecked(dataset_name),
                    ))
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_aliases)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
    }

    async fn modify_derived_dataset(
        &self,
        account_name: Option<AccountName>,
        dataset_name: &str,
        input_aliases: Vec<DatasetAlias>,
    ) {
        let dataset_alias =
            DatasetAlias::new(account_name, DatasetName::new_unchecked(dataset_name));

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_alias.as_local_ref())
            .await
            .unwrap();

        let mut id_aliases = Vec::new();
        for input_alias in input_aliases {
            id_aliases.push((
                self.dataset_id_by_name(input_alias.dataset_name.as_str())
                    .await,
                input_alias.to_string(),
            ));
        }

        dataset
            .commit_event(
                MetadataEvent::SetTransform(
                    MetadataFactory::set_transform()
                        .inputs_from_refs_and_aliases(id_aliases)
                        .build(),
                ),
                Default::default(),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_large_dataset_graph() -> DependencyGraphHarness {
    let dependency_harness = DependencyGraphHarness::new(false);
    dependency_harness.create_single_tenant_graph().await;
    dependency_harness.eager_initialization().await;

    /*
       Graph representation:
       foo_bar   foo_foo_bar
          |          |
         foo  --- foo_foo --- foo_foo_foo - foo_foo_foo_bar - foo_foo_foo_bar_foo - foo_foo_foo_bar_foo_bar
          |                        |
       foo_baz               foo_foo_foo_baz

         bar - bar_baz
          |
        bar_bar

         baz - baz_baz - baz_baz_foo - baz_baz_foo_bar
    */
    dependency_harness
        .create_root_dataset(None, "test-root-foo")
        .await;
    dependency_harness
        .create_root_dataset(None, "test-root-bar")
        .await;
    dependency_harness
        .create_root_dataset(None, "test-root-baz")
        .await;

    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-baz",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-bar-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-bar"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-bar-baz",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-bar"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-baz-baz",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-root-baz"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-foo",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-baz-baz-foo",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-baz-baz"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-foo-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-foo-baz",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-baz-baz-foo-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-baz-baz-foo"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-foo-bar-foo",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo-foo-bar"),
            )],
        )
        .await;
    dependency_harness
        .create_derived_dataset(
            None,
            "test-derive-foo-foo-foo-bar-foo-bar",
            vec![DatasetAlias::new(
                None,
                DatasetName::new_unchecked("test-derive-foo-foo-foo-bar-foo"),
            )],
        )
        .await;

    dependency_harness
}
