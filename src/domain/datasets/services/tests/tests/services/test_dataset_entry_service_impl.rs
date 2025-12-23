// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, TimeZone, Utc};
use init_on_startup::InitOnStartup;
use kamu_accounts::{Account, AccountRepository, CurrentAccountSubject, DidSecretEncryptionConfig};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::AccountServiceImpl;
use kamu_core::TenancyConfig;
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryRepository,
    DatasetLifecycleMessage,
    DatasetRegistry,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    MockDatasetEntryRepository,
};
use kamu_datasets_services::{DatasetEntryIndexer, DatasetEntryServiceImpl};
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use odf::metadata::testing::MetadataFactory;
use pretty_assertions::{assert_eq, assert_matches};
use time_source::{FakeSystemTimeSource, SystemTimeSource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_utils::test_for_each_tenancy!(test_indexes_datasets_correctly);
test_utils::test_for_each_tenancy!(test_try_to_resolve_non_existing_dataset);
test_utils::test_for_each_tenancy!(test_try_to_resolve_all_datasets_for_non_existing_user);
test_utils::test_for_each_tenancy!(test_resolve_dataset_handles_by_refs);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_indexes_datasets_correctly(tenancy_config: TenancyConfig) {
    let dataset_name_1 = "dataset1";
    let dataset_name_2 = "dataset2";
    let dataset_name_3 = "dataset3";
    let (_, dataset_id_1) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_2) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_id_3) = odf::DatasetID::new_generated_ed25519();

    let mut mock_dataset_entry_repository = MockDatasetEntryRepository::new();
    DatasetEntryServiceHarness::add_dataset_entries_count_expectation(
        &mut mock_dataset_entry_repository,
    );

    let dataset_entry_collector = Arc::new(RwLock::new(Vec::new()));
    DatasetEntryServiceHarness::add_save_dataset_entry_expectation_with_state(
        &mut mock_dataset_entry_repository,
        dataset_entry_collector.clone(),
    );

    let harness =
        DatasetEntryServiceHarness::new(mock_dataset_entry_repository.into(), tenancy_config);

    let (_, owner_account_id_1) = odf::AccountID::new_generated_ed25519();
    harness
        .account_repo
        .save_account(&Account::test(owner_account_id_1.clone(), "user1"))
        .await
        .unwrap();
    let (_, owner_account_id_2) = odf::AccountID::new_generated_ed25519();
    harness
        .account_repo
        .save_account(&Account::test(owner_account_id_2.clone(), "user2"))
        .await
        .unwrap();

    // Create 3 datasets
    let mut stored_by_id = HashMap::new();
    for dataset_id in [&dataset_id_1, &dataset_id_2, &dataset_id_3] {
        // Store the dataset
        let stored = harness
            .dataset_storage_unit_writer
            .store_dataset(
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root)
                        .id(dataset_id.clone())
                        .build(),
                )
                .build_typed(),
            )
            .await
            .unwrap();

        // Set initial head ref
        stored
            .dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &stored.seed,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(None),
                },
            )
            .await
            .unwrap();

        stored_by_id.insert(dataset_id.clone(), stored);
    }

    // Write aliases manually
    odf::dataset::write_dataset_alias(
        stored_by_id.get(&dataset_id_1).unwrap().dataset.as_ref(),
        &odf::DatasetAlias::new(
            Some(odf::AccountName::new_unchecked("user1")),
            odf::DatasetName::new_unchecked(dataset_name_1),
        ),
    )
    .await
    .unwrap();

    odf::dataset::write_dataset_alias(
        stored_by_id.get(&dataset_id_2).unwrap().dataset.as_ref(),
        &odf::DatasetAlias::new(
            Some(odf::AccountName::new_unchecked("user1")),
            odf::DatasetName::new_unchecked(dataset_name_2),
        ),
    )
    .await
    .unwrap();

    odf::dataset::write_dataset_alias(
        stored_by_id.get(&dataset_id_3).unwrap().dataset.as_ref(),
        &odf::DatasetAlias::new(
            Some(odf::AccountName::new_unchecked("user2")),
            odf::DatasetName::new_unchecked(dataset_name_3),
        ),
    )
    .await
    .unwrap();

    harness
        .dataset_entry_indexer
        .run_initialization()
        .await
        .unwrap();

    let mut dataset_entries = dataset_entry_collector.read().unwrap().clone();

    dataset_entries.sort_by(|l, r| l.name.cmp(&r.name));

    assert_eq!(
        [
            DatasetEntry {
                id: dataset_id_1,
                owner_id: owner_account_id_1.clone(),
                owner_name: odf::AccountName::new_unchecked("user1"),
                name: odf::DatasetName::new_unchecked(dataset_name_1),
                created_at: frozen_time_point(),
                kind: odf::DatasetKind::Root,
            },
            DatasetEntry {
                id: dataset_id_2,
                owner_id: owner_account_id_1,
                owner_name: odf::AccountName::new_unchecked("user1"),
                name: odf::DatasetName::new_unchecked(dataset_name_2),
                created_at: frozen_time_point(),
                kind: odf::DatasetKind::Root,
            },
            DatasetEntry {
                id: dataset_id_3,
                owner_id: owner_account_id_2,
                owner_name: odf::AccountName::new_unchecked("user2"),
                name: odf::DatasetName::new_unchecked(dataset_name_3),
                created_at: frozen_time_point(),
                kind: odf::DatasetKind::Root,
            }
        ],
        *dataset_entries,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_try_to_resolve_non_existing_dataset(tenancy_config: TenancyConfig) {
    let harness =
        DatasetEntryServiceHarness::new(TestDatasetEntryRepository::InMemory, tenancy_config);

    let dataset_ref = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked("foo")),
        odf::DatasetName::new_unchecked("bar"),
    )
    .as_local_ref();

    let resolve_dataset_result = harness
        .dataset_registry
        .resolve_dataset_handle_by_ref(&dataset_ref)
        .await;

    assert_matches!(
        resolve_dataset_result,
        Err(odf::DatasetRefUnresolvedError::NotFound(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_try_to_resolve_all_datasets_for_non_existing_user(tenancy_config: TenancyConfig) {
    use futures::TryStreamExt;

    let harness =
        DatasetEntryServiceHarness::new(TestDatasetEntryRepository::InMemory, tenancy_config);

    let resolve_dataset_result = harness
        .dataset_registry
        .all_dataset_handles_by_owner_name(&odf::AccountName::new_unchecked("foo"));

    let list_dataset: Vec<_> = resolve_dataset_result.try_collect().await.unwrap();

    assert!(list_dataset.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_resolve_dataset_handles_by_refs(tenancy_config: TenancyConfig) {
    let harness =
        DatasetEntryServiceHarness::new(TestDatasetEntryRepository::InMemory, tenancy_config);

    let account_1 = Account::test(odf::AccountID::new_generated_ed25519().1, "account-1");
    let account_2 = Account::test(odf::AccountID::new_generated_ed25519().1, "account-2");
    let account_3_subject = Account::test(odf::AccountID::new_generated_ed25519().1, "kamu");

    for account in [&&account_1, &account_2, &account_3_subject] {
        harness.account_repo.save_account(account).await.unwrap();
    }

    let [
        account_1_dataset_1_handle,
        account_1_dataset_2_handle,
        account_2_dataset_3_handle,
        account_3_subject_dataset_4_handle,
        account_3_subject_dataset_5_not_created_handle,
    ] = [
        (&account_1, "dataset-1"),
        (&account_1, "dataset-2"),
        (&account_2, "dataset-3"),
        (&account_3_subject, "dataset-4"),
        (&account_3_subject, "dataset-5"),
    ]
    .map(|(account, dataset_name)| {
        let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
        let dataset_alias = tenancy_config.make_alias(
            account.account_name.clone(),
            odf::DatasetName::new_unchecked(dataset_name),
        );
        odf::DatasetHandle::new(dataset_id, dataset_alias, odf::DatasetKind::Root)
    });

    for (dataset_handle, account) in [
        (&account_1_dataset_1_handle, &account_1),
        (&account_1_dataset_2_handle, &account_1),
        (&account_2_dataset_3_handle, &account_2),
        (&account_3_subject_dataset_4_handle, &account_3_subject),
    ] {
        harness
            .dataset_entry_writer
            .create_entry(
                &dataset_handle.id,
                &account.id,
                &account.account_name,
                &dataset_handle.alias.dataset_name,
                dataset_handle.kind,
            )
            .await
            .unwrap();
    }

    // IDs
    {
        let not_found_dataset_id_ref = account_3_subject_dataset_5_not_created_handle
            .id
            .as_local_ref();

        let refs = [
            &account_1_dataset_1_handle.id.as_local_ref(),
            &account_1_dataset_2_handle.id.as_local_ref(),
            &account_2_dataset_3_handle.id.as_local_ref(),
            &account_3_subject_dataset_4_handle.id.as_local_ref(),
            &not_found_dataset_id_ref,
        ];

        let resolution = harness
            .dataset_registry
            .resolve_dataset_handles_by_refs(&refs)
            .await
            .unwrap();

        assert_eq!(
            [
                (
                    account_1_dataset_1_handle.id.as_local_ref(),
                    account_1_dataset_1_handle.clone()
                ),
                (
                    account_1_dataset_2_handle.id.as_local_ref(),
                    account_1_dataset_2_handle.clone()
                ),
                (
                    account_2_dataset_3_handle.id.as_local_ref(),
                    account_2_dataset_3_handle.clone()
                ),
                (
                    account_3_subject_dataset_4_handle.id.as_local_ref(),
                    account_3_subject_dataset_4_handle.clone()
                ),
            ],
            *resolution.resolved_handles
        );
        assert_matches!(
            &resolution.unresolved_refs[..],
            [(unresolved_ref, odf::DatasetRefUnresolvedError::NotFound(e))]
                if *unresolved_ref == not_found_dataset_id_ref
                    && e.dataset_ref == not_found_dataset_id_ref
        );
    }

    // Aliases
    {
        let refs = [
            &account_1_dataset_1_handle.alias.as_local_ref(),
            &account_1_dataset_2_handle.alias.as_local_ref(),
            &account_2_dataset_3_handle.alias.as_local_ref(),
            &account_3_subject_dataset_4_handle.alias.as_local_ref(),
            &account_3_subject_dataset_5_not_created_handle
                .alias
                .as_local_ref(),
        ];

        let resolution = harness
            .dataset_registry
            .resolve_dataset_handles_by_refs(&refs)
            .await
            .unwrap();

        match tenancy_config {
            TenancyConfig::SingleTenant => {
                assert_eq!(
                    [(
                        account_3_subject_dataset_4_handle.alias.as_local_ref(),
                        account_3_subject_dataset_4_handle.clone()
                    ),],
                    *resolution.resolved_handles
                );
            }
            TenancyConfig::MultiTenant => {
                assert_eq!(
                    [
                        (
                            account_1_dataset_1_handle.alias.as_local_ref(),
                            account_1_dataset_1_handle.clone()
                        ),
                        (
                            account_1_dataset_2_handle.alias.as_local_ref(),
                            account_1_dataset_2_handle.clone()
                        ),
                        (
                            account_2_dataset_3_handle.alias.as_local_ref(),
                            account_2_dataset_3_handle.clone()
                        ),
                        (
                            account_3_subject_dataset_4_handle.alias.as_local_ref(),
                            account_3_subject_dataset_4_handle.clone()
                        ),
                    ],
                    *resolution.resolved_handles
                );
            }
        }
    }

    // Handles
    // todo

    // Mixed
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum TestDatasetEntryRepository {
    Mock(MockDatasetEntryRepository),
    InMemory,
}

impl From<MockDatasetEntryRepository> for TestDatasetEntryRepository {
    fn from(mock: MockDatasetEntryRepository) -> Self {
        Self::Mock(mock)
    }
}

struct DatasetEntryServiceHarness {
    _temp_dir: tempfile::TempDir,
    dataset_entry_indexer: Arc<DatasetEntryIndexer>,
    account_repo: Arc<dyn AccountRepository>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    dataset_entry_writer: Arc<dyn kamu_datasets_services::DatasetEntryWriter>,
}

impl DatasetEntryServiceHarness {
    fn new(
        test_dataset_entry_repository: TestDatasetEntryRepository,
        tenancy_config: TenancyConfig,
    ) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = {
            use dill::Component;

            let mut b = dill::CatalogBuilder::new();

            use odf::dataset::DatasetStorageUnitLocalFs;
            b.add_builder(DatasetStorageUnitLocalFs::builder(datasets_dir));
            b.add::<odf::dataset::DatasetLfsBuilderDefault>();

            b.add::<DatasetEntryServiceImpl>();
            b.add::<DatasetEntryIndexer>();

            match test_dataset_entry_repository {
                TestDatasetEntryRepository::Mock(mock) => {
                    b.add_value(mock);
                    b.bind::<dyn DatasetEntryRepository, MockDatasetEntryRepository>();
                }
                TestDatasetEntryRepository::InMemory => {
                    b.add::<kamu_datasets_inmem::InMemoryDatasetEntryRepository>();
                }
            }

            let t = frozen_time_point();
            let fake_system_time_source = FakeSystemTimeSource::new(t);
            b.add_value(fake_system_time_source);
            b.bind::<dyn SystemTimeSource, FakeSystemTimeSource>();

            b.add::<InMemoryAccountRepository>();
            b.add::<InMemoryDidSecretKeyRepository>();
            b.add::<AccountServiceImpl>();

            b.add_value(DidSecretEncryptionConfig::sample());

            b.add_builder(
                OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            );
            b.bind::<dyn Outbox, OutboxImmediateImpl>();

            b.add_value(CurrentAccountSubject::new_test());

            b.add_value(tenancy_config);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            b.build()
        };

        Self {
            _temp_dir: temp_dir,
            dataset_entry_indexer: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            dataset_entry_writer: catalog.get_one().unwrap(),
        }
    }

    // Expectation: MockDatasetEntryRepository

    fn add_save_dataset_entry_expectation_with_state(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
        state: Arc<RwLock<Vec<DatasetEntry>>>,
    ) {
        mock_dataset_entry_repository
            .expect_save_dataset_entry()
            .returning(move |dataset_entry| {
                let mut writable_state = state.write().unwrap();

                (*writable_state).push(dataset_entry.clone());

                Ok(())
            });
    }

    fn add_dataset_entries_count_expectation(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
    ) {
        mock_dataset_entry_repository
            .expect_dataset_entries_count()
            .times(1)
            .returning(|| Ok(0));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn frozen_time_point() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
