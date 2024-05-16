// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use event_bus::EventBus;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_accounts::{AuthenticationService, CurrentAccountSubject, MockAuthenticationService};
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_services::*;
use mockall::predicate::eq;
use opendatafabric::*;

#[test_log::test(tokio::test)]
async fn test_get_account_downstream_dependencies() {
    let foo_account_name = AccountName::new_unchecked("foo");
    let bar_account_name = AccountName::new_unchecked("bar");
    let foo_account_id = AccountID::new_seeded_ed25519(foo_account_name.as_bytes());
    let harness =
        FlowPermissionsPluginHarness::new(foo_account_name.clone(), foo_account_id.clone());
    harness.initialize_dependecnies().await;
    let foo_root_dataset_name = DatasetName::new_unchecked("foo");
    let derived_foo_bar_dataset_name = DatasetName::new_unchecked("foo.bar");
    let derived_foo_baz_dataset_name = DatasetName::new_unchecked("foo.baz");

    let root_created_dataset = harness
        .create_root_dataset(DatasetAlias {
            account_name: Some(foo_account_name.clone()),
            dataset_name: foo_root_dataset_name.clone(),
        })
        .await;

    let derived_foo_bar_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(foo_account_name.clone()),
                dataset_name: derived_foo_bar_dataset_name.clone(),
            },
            vec![root_created_dataset.clone()],
        )
        .await;

    let derived_foo_baz_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(foo_account_name.clone()),
                dataset_name: derived_foo_baz_dataset_name.clone(),
            },
            vec![root_created_dataset.clone()],
        )
        .await;

    let bar_root_dataset_name = DatasetName::new_unchecked("bar");
    let derived_bar_bar_dataset_name = DatasetName::new_unchecked("bar.bar");

    let bar_root_created_dataset = harness
        .create_root_dataset(DatasetAlias {
            account_name: Some(bar_account_name.clone()),
            dataset_name: bar_root_dataset_name.clone(),
        })
        .await;

    // Create derived dataset for bar account with root dataset from foo account
    let bar_bar_derived_created_dataset = harness
        .create_derived_dataset(
            DatasetAlias {
                account_name: Some(bar_account_name.clone()),
                dataset_name: derived_bar_bar_dataset_name.clone(),
            },
            vec![
                root_created_dataset.clone(),
                bar_root_created_dataset.clone(),
            ],
        )
        .await;

    let filtered_foo_downstream_datasets = harness
        .flow_permissions_plugin
        .get_account_downstream_dependencies(&root_created_dataset, Some(foo_account_id))
        .await
        .unwrap();

    assert_eq!(
        filtered_foo_downstream_datasets,
        vec![
            derived_foo_baz_created_dataset.clone(),
            derived_foo_bar_created_dataset.clone(),
        ]
    );

    // Should return downstream dependencies for all accounts
    let filtered_foo_downstream_datasets = harness
        .flow_permissions_plugin
        .get_account_downstream_dependencies(&root_created_dataset, None)
        .await
        .unwrap();

    assert_eq!(
        filtered_foo_downstream_datasets,
        vec![
            bar_bar_derived_created_dataset,
            derived_foo_baz_created_dataset,
            derived_foo_bar_created_dataset,
        ]
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowPermissionsPluginHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    flow_permissions_plugin: Arc<dyn FlowPermissionsPlugin>,
}

impl FlowPermissionsPluginHarness {
    fn new(current_account_name: AccountName, current_account_id: AccountID) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let current_account_name_clone = current_account_name.clone();
        let mut mock_authentication_service = MockAuthenticationService::new();
        mock_authentication_service
            .expect_find_account_name_by_id()
            .with(eq(current_account_id.clone()))
            .returning(move |_| Ok(Some(current_account_name_clone.clone())));

        let catalog = CatalogBuilder::new()
            .add::<EventBus>()
            .add::<SystemTimeSourceDefault>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(true),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::logged(
                current_account_id,
                current_account_name,
                false,
            ))
            .add_value(mock_authentication_service)
            .bind::<dyn AuthenticationService, MockAuthenticationService>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .add::<FlowPermissionsPluginImpl>()
            .build();

        let flow_permissions_plugin = catalog.get_one::<dyn FlowPermissionsPlugin>().unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_permissions_plugin,
            dataset_repo,
        }
    }

    async fn initialize_dependecnies(&self) {
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        let dependency_graph_repository =
            DependencyGraphRepositoryInMemory::new(self.dataset_repo.clone());
        dependency_graph_service
            .eager_initialization(&dependency_graph_repository)
            .await
            .unwrap();
    }

    async fn create_root_dataset(&self, dataset_aliast: DatasetAlias) -> DatasetID {
        let result = self
            .dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_aliast)
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();

        result.dataset_handle.id
    }

    async fn create_derived_dataset(
        &self,
        dataset_aliast: DatasetAlias,
        input_ids: Vec<DatasetID>,
    ) -> DatasetID {
        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_aliast)
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_ids)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        create_result.dataset_handle.id
    }
}
