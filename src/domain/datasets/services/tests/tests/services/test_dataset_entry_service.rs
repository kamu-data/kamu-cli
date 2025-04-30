// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono::{DateTime, TimeZone, Utc};
use dill::{CatalogBuilder, Component};
use init_on_startup::InitOnStartup;
use kamu_accounts::{Account, AccountRepository, CurrentAccountSubject};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::AccountServiceImpl;
use kamu_core::{DatasetRegistry, TenancyConfig};
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryRepository,
    DatasetLifecycleMessage,
    MockDatasetEntryRepository,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use kamu_datasets_services::{DatasetEntryIndexer, DatasetEntryServiceImpl};
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxImmediateImpl};
use odf::metadata::testing::MetadataFactory;
use time_source::{FakeSystemTimeSource, SystemTimeSource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_indexes_datasets_correctly() {
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

    let harness = DatasetEntryServiceHarness::new(mock_dataset_entry_repository);

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

    pretty_assertions::assert_eq!(
        dataset_entries,
        vec![
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
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_to_resolve_non_existing_dataset() {
    let harness = DatasetEntryServiceHarness::new(MockDatasetEntryRepository::new());

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

#[test_log::test(tokio::test)]
async fn test_try_to_resolve_all_datasets_for_non_existing_user() {
    use futures::TryStreamExt;

    let harness = DatasetEntryServiceHarness::new(MockDatasetEntryRepository::new());

    let resolve_dataset_result = harness
        .dataset_registry
        .all_dataset_handles_by_owner(&odf::AccountName::new_unchecked("foo"));

    let list_dataset: Vec<_> = resolve_dataset_result.try_collect().await.unwrap();

    assert!(list_dataset.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetEntryServiceHarness {
    _tempdir: tempfile::TempDir,
    dataset_entry_indexer: Arc<DatasetEntryIndexer>,
    account_repo: Arc<dyn AccountRepository>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
}

impl DatasetEntryServiceHarness {
    fn new(mock_dataset_entry_repository: MockDatasetEntryRepository) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = {
            let mut b = CatalogBuilder::new();

            use odf::dataset::DatasetStorageUnitLocalFs;
            b.add_builder(DatasetStorageUnitLocalFs::builder(datasets_dir));
            b.add::<odf::dataset::DatasetLfsBuilderDefault>();

            b.add::<DatasetEntryServiceImpl>();
            b.add::<DatasetEntryIndexer>();

            b.add_value(mock_dataset_entry_repository);
            b.bind::<dyn DatasetEntryRepository, MockDatasetEntryRepository>();

            let t = frozen_time_point();
            let fake_system_time_source = FakeSystemTimeSource::new(t);
            b.add_value(fake_system_time_source);
            b.bind::<dyn SystemTimeSource, FakeSystemTimeSource>();

            b.add::<InMemoryAccountRepository>();
            b.add::<AccountServiceImpl>();

            b.add_builder(
                OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            );
            b.bind::<dyn Outbox, OutboxImmediateImpl>();

            b.add_value(CurrentAccountSubject::new_test());

            b.add_value(TenancyConfig::SingleTenant);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            b.build()
        };

        Self {
            _tempdir: tempdir,
            dataset_entry_indexer: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
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
