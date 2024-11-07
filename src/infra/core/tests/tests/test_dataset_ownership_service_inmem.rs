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

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use kamu::testing::MetadataFactory;
use kamu::{
    DatasetOwnershipServiceInMemory,
    DatasetOwnershipServiceInMemoryStateInitializer,
    DatasetRegistryRepoBridge,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
};
use kamu_accounts::{
    AccountConfig,
    AuthenticationService,
    CurrentAccountSubject,
    JwtAuthenticationConfig,
    PredefinedAccountsConfig,
    DEFAULT_ACCOUNT_ID,
};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_core::{DatasetOwnershipService, DatasetRepository, TenancyConfig};
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetID, DatasetKind, DatasetName};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_multi_tenant_dataset_owners() {
    let mut harness = DatasetOwnershipHarness::new(TenancyConfig::MultiTenant).await;

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
    catalog: dill::Catalog,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    dataset_ownership_service: Arc<dyn DatasetOwnershipService>,
    auth_svc: Arc<dyn AuthenticationService>,
    account_datasets: HashMap<AccountID, Vec<DatasetID>>,
}

impl DatasetOwnershipHarness {
    async fn new(tenancy_config: TenancyConfig) -> Self {
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

        let base_catalog = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add_value(tenancy_config)
                .add_builder(DatasetRepositoryLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
                .add::<DatasetRegistryRepoBridge>()
                .add_value(CurrentAccountSubject::new_test())
                .add::<AccessTokenServiceImpl>()
                .add::<AuthenticationServiceImpl>()
                .add_value(predefined_accounts_config.clone())
                .add_value(JwtAuthenticationConfig::default())
                .add::<InMemoryAccountRepository>()
                .add::<InMemoryAccessTokenRepository>()
                .add::<DatasetOwnershipServiceInMemory>()
                .add::<DatabaseTransactionRunner>()
                .add::<LoginPasswordAuthProvider>()
                .add::<PredefinedAccountsRegistrator>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        init_on_startup::run_startup_jobs(&base_catalog)
            .await
            .unwrap();

        // Attach ownership initializer in separate catalog,
        // so that the startup job is not run before creating datasets
        let catalog = {
            let mut b = dill::CatalogBuilder::new_chained(&base_catalog);
            b.add::<DatasetOwnershipServiceInMemoryStateInitializer>();
            b.build()
        };

        let dataset_repo_writer = catalog.get_one::<dyn DatasetRepositoryWriter>().unwrap();

        let dataset_ownership_service = catalog.get_one::<dyn DatasetOwnershipService>().unwrap();
        let auth_svc = catalog.get_one::<dyn AuthenticationService>().unwrap();

        Self {
            _workdir: workdir,
            catalog,
            dataset_repo_writer,
            dataset_ownership_service,
            auth_svc,
            account_datasets: HashMap::new(),
        }
    }

    async fn eager_initialization(&self) {
        use init_on_startup::InitOnStartup;
        let initializer = self
            .catalog
            .get_one::<DatasetOwnershipServiceInMemoryStateInitializer>()
            .unwrap();
        initializer.run_initialization().await.unwrap();
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
            .dataset_repo_writer
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
            .unwrap()
            .create_dataset_result;

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
            .dataset_repo_writer
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
            .unwrap()
            .create_dataset_result;

        self.account_datasets
            .entry(account_id.clone())
            .and_modify(|e| {
                e.push(created_dataset.dataset_handle.id.clone());
            })
            .or_insert_with(|| vec![created_dataset.dataset_handle.id.clone()]);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
