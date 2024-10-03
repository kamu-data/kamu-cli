// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, RwLock};

use chrono::{DateTime, TimeZone, Utc};
use dill::{Catalog, CatalogBuilder, Component};
use init_on_startup::InitOnStartup;
use kamu::{DatasetRepositoryWriter, MockDatasetRepositoryWriter};
use kamu_accounts::{Account, AccountRepository, CurrentAccountSubject};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_core::testing::MockDatasetRepository;
use kamu_core::{
    DatasetLifecycleMessage,
    DatasetRepository,
    DatasetVisibility,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository, MockDatasetEntryRepository};
use kamu_datasets_services::{DatasetEntryIndexer, DatasetEntryService};
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxExt, OutboxImmediateImpl};
use mockall::predicate::eq;
use opendatafabric::{AccountID, AccountName, DatasetAlias, DatasetHandle, DatasetID, DatasetName};
use time_source::{FakeSystemTimeSource, SystemTimeSource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_correctly_handles_outbox_messages() {
    let (_, dataset_id) = DatasetID::new_generated_ed25519();
    let (_, owner_account_id) = AccountID::new_generated_ed25519();
    let initial_dataset_name = DatasetName::new_unchecked("initial-name");
    let new_dataset_name = DatasetName::new_unchecked("new-name");

    let mut mock_dataset_entry_repository = MockDatasetEntryRepository::new();
    DatasetEntryServiceHarness::add_save_dataset_entry_expectation(
        &mut mock_dataset_entry_repository,
        dataset_id.clone(),
        owner_account_id.clone(),
        initial_dataset_name.clone(),
    );
    DatasetEntryServiceHarness::add_update_dataset_entry_name_expectation(
        &mut mock_dataset_entry_repository,
        dataset_id.clone(),
        new_dataset_name.clone(),
    );
    DatasetEntryServiceHarness::add_delete_dataset_entry_expectation(
        &mut mock_dataset_entry_repository,
        dataset_id.clone(),
    );

    let harness = DatasetEntryServiceHarness::new(
        mock_dataset_entry_repository,
        MockDatasetRepository::new(),
    );

    harness
        .mimic_dataset_created(
            dataset_id.clone(),
            owner_account_id.clone(),
            initial_dataset_name.clone(),
        )
        .await;

    harness
        .mimic_dataset_renamed(
            dataset_id.clone(),
            owner_account_id,
            initial_dataset_name,
            new_dataset_name,
        )
        .await;

    harness.mimic_dataset_deleted(dataset_id).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_indexes_datasets_correctly() {
    let dataset_name_1 = "dataset1";
    let dataset_name_2 = "dataset2";
    let dataset_name_3 = "dataset3";
    let (_, dataset_id_1) = DatasetID::new_generated_ed25519();
    let (_, dataset_id_2) = DatasetID::new_generated_ed25519();
    let (_, dataset_id_3) = DatasetID::new_generated_ed25519();
    let dataset_handles = vec![
        DatasetHandle::new(
            dataset_id_1.clone(),
            DatasetAlias::new(
                Some(AccountName::new_unchecked("user1")),
                DatasetName::new_unchecked(dataset_name_1),
            ),
        ),
        DatasetHandle::new(
            dataset_id_2.clone(),
            DatasetAlias::new(
                Some(AccountName::new_unchecked("user1")),
                DatasetName::new_unchecked(dataset_name_2),
            ),
        ),
        DatasetHandle::new(
            dataset_id_3.clone(),
            DatasetAlias::new(
                Some(AccountName::new_unchecked("user2")),
                DatasetName::new_unchecked(dataset_name_3),
            ),
        ),
    ];

    let mut mock_dataset_repository = MockDatasetRepository::new();
    DatasetEntryServiceHarness::add_get_all_datasets_expectation(
        &mut mock_dataset_repository,
        dataset_handles,
    );

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
        DatasetEntryServiceHarness::new(mock_dataset_entry_repository, mock_dataset_repository);

    let (_, owner_account_id_1) = AccountID::new_generated_ed25519();
    harness
        .account_repo
        .create_account(&Account::test(owner_account_id_1.clone(), "user1"))
        .await
        .unwrap();
    let (_, owner_account_id_2) = AccountID::new_generated_ed25519();
    harness
        .account_repo
        .create_account(&Account::test(owner_account_id_2.clone(), "user2"))
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
                name: DatasetName::new_unchecked(dataset_name_1),
                created_at: frozen_time_point(),
            },
            DatasetEntry {
                id: dataset_id_2,
                owner_id: owner_account_id_1,
                name: DatasetName::new_unchecked(dataset_name_2),
                created_at: frozen_time_point(),
            },
            DatasetEntry {
                id: dataset_id_3,
                owner_id: owner_account_id_2,
                name: DatasetName::new_unchecked(dataset_name_3),
                created_at: frozen_time_point(),
            }
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetEntryServiceHarness {
    _catalog: Catalog,
    outbox: Arc<dyn Outbox>,
    dataset_entry_indexer: Arc<DatasetEntryIndexer>,
    account_repo: Arc<dyn AccountRepository>,
}

impl DatasetEntryServiceHarness {
    fn new(
        mock_dataset_entry_repository: MockDatasetEntryRepository,
        mock_dataset_repository: MockDatasetRepository,
    ) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add::<DatasetEntryService>();
            b.add::<DatasetEntryIndexer>();

            b.add_value(mock_dataset_entry_repository);
            b.bind::<dyn DatasetEntryRepository, MockDatasetEntryRepository>();

            let t = frozen_time_point();
            let fake_system_time_source = FakeSystemTimeSource::new(t);
            b.add_value(fake_system_time_source);
            b.bind::<dyn SystemTimeSource, FakeSystemTimeSource>();

            b.add_value(mock_dataset_repository);
            b.bind::<dyn DatasetRepository, MockDatasetRepository>();

            b.add_value(MockDatasetRepositoryWriter::new());
            b.bind::<dyn DatasetRepositoryWriter, MockDatasetRepositoryWriter>();

            let account_repository = InMemoryAccountRepository::new();
            b.add_value(account_repository);
            b.bind::<dyn AccountRepository, InMemoryAccountRepository>();

            b.add_builder(
                OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            );
            b.bind::<dyn Outbox, OutboxImmediateImpl>();

            b.add_value(CurrentAccountSubject::new_test());

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            );

            b.build()
        };

        Self {
            outbox: catalog.get_one().unwrap(),
            dataset_entry_indexer: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }

    // Outbox: mimic messages

    async fn mimic_dataset_created(
        &self,
        dataset_id: DatasetID,
        owner_account_id: AccountID,
        dataset_name: DatasetName,
    ) {
        let private_visibility = DatasetVisibility::Private;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_id,
                    owner_account_id,
                    private_visibility,
                    dataset_name,
                ),
            )
            .await
            .unwrap();
    }

    async fn mimic_dataset_renamed(
        &self,
        dataset_id: DatasetID,
        owner_account_id: AccountID,
        old_dataset_name: DatasetName,
        new_dataset_name: DatasetName,
    ) {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::renamed(
                    dataset_id,
                    owner_account_id,
                    old_dataset_name,
                    new_dataset_name,
                ),
            )
            .await
            .unwrap();
    }

    async fn mimic_dataset_deleted(&self, dataset_id: DatasetID) {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(dataset_id),
            )
            .await
            .unwrap();
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

    fn add_update_dataset_entry_name_expectation(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
        dataset_id: DatasetID,
        new_dataset_name: DatasetName,
    ) {
        mock_dataset_entry_repository
            .expect_update_dataset_entry_name()
            .with(eq(dataset_id), eq(new_dataset_name))
            .times(1)
            .returning(|_, _| Ok(()));
    }

    fn add_delete_dataset_entry_expectation(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
        dataset_id: DatasetID,
    ) {
        mock_dataset_entry_repository
            .expect_delete_dataset_entry()
            .with(eq(dataset_id))
            .times(1)
            .returning(|_| Ok(()));
    }

    fn add_save_dataset_entry_expectation(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
        dataset_id: DatasetID,
        owner_account_id: AccountID,
        dataset_name: DatasetName,
    ) {
        let expected_entry = DatasetEntry::new(
            dataset_id,
            owner_account_id,
            dataset_name,
            frozen_time_point(),
        );

        mock_dataset_entry_repository
            .expect_save_dataset_entry()
            .with(eq(expected_entry))
            .times(1)
            .returning(|_| Ok(()));
    }

    fn add_dataset_entries_count_expectation(
        mock_dataset_entry_repository: &mut MockDatasetEntryRepository,
    ) {
        mock_dataset_entry_repository
            .expect_dataset_entries_count()
            .times(1)
            .returning(|| Ok(0));
    }

    // Expectation: MockDatasetRepository

    fn add_get_all_datasets_expectation(
        mock_dataset_repository: &mut MockDatasetRepository,
        dataset_handles: Vec<DatasetHandle>,
    ) {
        mock_dataset_repository
            .expect_get_all_datasets()
            .times(1)
            .returning(move || {
                let stream = futures::stream::iter(dataset_handles.clone().into_iter().map(Ok));

                Box::pin(stream)
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn frozen_time_point() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
