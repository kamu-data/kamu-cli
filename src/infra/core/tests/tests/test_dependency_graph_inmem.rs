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
use futures::{StreamExt, TryStreamExt};
use internal_error::ResultIntoInternal;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetRepositoryLocalFs,
    DependencyGraphRepositoryInMemory,
    DependencyGraphServiceInMemory,
};
use kamu_core::{
    auth,
    CurrentAccountSubject,
    DatasetRepository,
    DependencyGraphRepository,
    DependencyGraphService,
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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_single_tenant_repository() {
    let harness = DependencyGraphHarness::new(false);

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(all_dependencies),
        ""
    );

    harness.create_single_tenant_graph().await;

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(all_dependencies),
        indoc::indoc!(
            r#"
            bar -> foo-bar
            baz -> foo-baz
            foo -> foo-bar
            foo -> foo-baz
            foo-bar -> foo-bar-foo-baz
            foo-baz -> foo-bar-foo-baz"#
        ),
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_tenant_repository() {
    let harness = DependencyGraphHarness::new(true);

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(all_dependencies),
        ""
    );

    harness.create_multi_tenant_graph().await;

    let all_dependencies: Vec<_> = harness.list_all_dependencies().await;
    assert_eq!(
        DependencyGraphHarness::all_dependencies_report(all_dependencies),
        indoc::indoc!(
            r#"
            alice/bar -> alice/foo-bar
            alice/foo -> alice/foo-bar
            alice/foo -> bob/foo-baz
            alice/foo-bar -> eve/foo-bar-foo-baz
            bob/baz -> bob/foo-baz
            bob/foo-baz -> eve/foo-bar-foo-baz"#
        ),
    )
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

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
        for (downstream_id, upstream_id) in dependencies {
            let downstream_hdl = self
                .dataset_repo
                .resolve_dataset_ref(&downstream_id.as_local_ref())
                .await
                .unwrap();
            let upstream_hdl = self
                .dataset_repo
                .resolve_dataset_ref(&upstream_id.as_local_ref())
                .await
                .unwrap();

            res.push((
                format!("{}", upstream_hdl.alias),
                format!("{}", downstream_hdl.alias),
            ));
        }

        res.sort();
        res
    }

    fn all_dependencies_report(dependencies: Vec<(String, String)>) -> String {
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
                account_name,
                MetadataFactory::dataset_snapshot()
                    .name(DatasetName::new_unchecked(dataset_name))
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
                account_name,
                MetadataFactory::dataset_snapshot()
                    .name(DatasetName::new_unchecked(dataset_name))
                    .kind(DatasetKind::Derivative)
                    .push_event(MetadataFactory::set_transform_aliases(input_aliases).build())
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

        let mut id_map = HashMap::new();
        for input_alias in &input_aliases {
            id_map.insert(
                input_alias.dataset_name.clone(),
                self.dataset_id_by_name(input_alias.dataset_name.as_str())
                    .await,
            );
        }

        dataset
            .commit_event(
                MetadataEvent::SetTransform(
                    MetadataFactory::set_transform_aliases(input_aliases)
                        .set_dataset_ids(id_map)
                        .build(),
                ),
                Default::default(),
            )
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
