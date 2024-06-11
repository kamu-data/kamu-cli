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

use database_common::{DatabaseTransactionRunner, FakeDatabasePlugin};
use dill::Component;
use event_bus::EventBus;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetOwnershipServiceInMemory,
    DatasetRepositoryLocalFs,
    DependencyGraphServiceInMemory,
};
use kamu_accounts::{
    AccountConfig,
    AuthenticationService,
    CurrentAccountSubject,
    JwtAuthenticationConfig,
    PredefinedAccountsConfig,
    DEFAULT_ACCOUNT_ID,
};
use kamu_accounts_inmem::AccountRepositoryInMemory;
use kamu_accounts_services::{AuthenticationServiceImpl, PredefinedAccountsRegistrator};
use kamu_core::{auth, DatasetOwnershipService, DatasetRepository, SystemTimeSourceDefault};
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetID, DatasetKind, DatasetName};
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_tenant_dataset_owners() {
    let mut harness = DatasetOwnershipHarness::new(true).await;

    harness.create_multi_tenant_datasets().await;
    harness.eager_initialization().await;

    for (account_id, mut dataset_ids) in harness.account_datasets {
        let mut owner_datasets = harness
            .dataset_ownership_service
            .get_owned_datasets(&account_id)
            .await
            .unwrap();
        owner_datasets.sort();
        dataset_ids.sort();
        assert_eq!(owner_datasets, dataset_ids);

        for dataset_id in dataset_ids {
            let is_owner = harness
                .dataset_ownership_service
                .is_dataset_owned_by(&dataset_id, &account_id)
                .await
                .unwrap();
            assert!(is_owner);

            let is_invalid_owner = harness
                .dataset_ownership_service
                .is_dataset_owned_by(&dataset_id, &DEFAULT_ACCOUNT_ID)
                .await
                .unwrap();
            assert!(!is_invalid_owner);

            let dataset_owners = harness
                .dataset_ownership_service
                .get_dataset_owners(&dataset_id)
                .await
                .unwrap();

            assert_eq!(dataset_owners, [account_id.clone()]);
        }
    }
}

struct DatasetOwnershipHarness {
    _workdir: TempDir,
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
    auth_svc: Arc<dyn AuthenticationService>,
    account_datasets: HashMap<AccountID, Vec<DatasetID>>,
}

impl DatasetOwnershipHarness {
    async fn new(multi_tenant: bool) -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let datasets_dir = workdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();
        let predefined_accounts = [
            AccountName::new_unchecked("alice"),
            AccountName::new_unchecked("bob"),
            AccountName::new_unchecked("eve"),
        ];
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for account_name in predefined_accounts {
            predefined_accounts_config
                .predefined
                .push(AccountConfig::from_name(account_name));
        }

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add::<EventBus>()
                .add_builder(
                    DatasetRepositoryLocalFs::builder()
                        .with_root(datasets_dir)
                        .with_multi_tenant(multi_tenant),
                )
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .add_value(CurrentAccountSubject::new_test())
                .add::<AuthenticationServiceImpl>()
                .add_value(predefined_accounts_config.clone())
                .add_value(JwtAuthenticationConfig::default())
                .add::<AccountRepositoryInMemory>()
                .add::<DatasetOwnershipServiceInMemory>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<DependencyGraphServiceInMemory>()
                .add::<DatabaseTransactionRunner>()
                .add::<PredefinedAccountsRegistrator>();

            FakeDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        DatabaseTransactionRunner::new(catalog.clone())
            .transactional(|transactional_catalog| async move {
                let registrator = transactional_catalog
                    .get_one::<PredefinedAccountsRegistrator>()
                    .unwrap();

                registrator
                    .ensure_predefined_accounts_are_registered()
                    .await
            })
            .await
            .unwrap();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        let dataset_ownership_service = catalog.get_one::<dyn DatasetOwnershipService>().unwrap();
        let auth_svc = catalog.get_one::<dyn AuthenticationService>().unwrap();

        Self {
            _workdir: workdir,
            _catalog: catalog,
            dataset_repo,
            dataset_ownership_service,
            auth_svc,
            account_datasets: HashMap::new(),
        }
    }

    async fn eager_initialization(&self) {
        self.dataset_ownership_service
            .eager_initialization(&self.auth_svc)
            .await
            .unwrap();
    }

    async fn create_multi_tenant_datasets(&mut self) {
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

        self.create_datasets(|dataset_name| dataset_accounts.get(dataset_name).cloned())
            .await;
    }

    async fn create_datasets(&mut self, account_getter: impl Fn(&str) -> Option<AccountName>) {
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

    async fn create_root_dataset(&mut self, account_name: Option<AccountName>, dataset_name: &str) {
        let account_id = self
            .auth_svc
            .find_account_id_by_name(account_name.as_ref().unwrap())
            .await
            .unwrap()
            .unwrap();
        let created_dataset = self
            .dataset_repo
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
        self.account_datasets
            .entry(account_id.clone())
            .and_modify(|e| {
                e.push(created_dataset.dataset_handle.id.clone());
            })
            .or_insert_with(|| vec![created_dataset.dataset_handle.id.clone()]);
    }

    async fn create_derived_dataset(
        &mut self,
        account_name: Option<AccountName>,
        dataset_name: &str,
        input_aliases: Vec<DatasetAlias>,
    ) {
        let account_id = self
            .auth_svc
            .find_account_id_by_name(account_name.as_ref().unwrap())
            .await
            .unwrap()
            .unwrap();
        let created_dataset = self
            .dataset_repo
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

        self.account_datasets
            .entry(account_id.clone())
            .and_modify(|e| {
                e.push(created_dataset.dataset_handle.id.clone());
            })
            .or_insert_with(|| vec![created_dataset.dataset_handle.id.clone()]);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
