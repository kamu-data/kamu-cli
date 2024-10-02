// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use dill::{Catalog, CatalogBuilder, Component};
use kamu::{DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_core::{
    DatasetLifecycleMessage,
    DatasetRepository,
    DatasetVisibility,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use kamu_datasets::{DatasetEntry, DatasetEntryRepository, MockDatasetEntryRepository};
use kamu_datasets_services::DatasetEntryService;
use messaging_outbox::{register_message_dispatcher, Outbox, OutboxExt, OutboxImmediateImpl};
use mockall::predicate::eq;
use opendatafabric::{AccountID, DatasetID, DatasetName};
use tempfile::TempDir;
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

    let harness = DatasetEntryServiceHarness::new(mock_dataset_entry_repository);

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

struct DatasetEntryServiceHarness {
    _catalog: Catalog,
    _temp_dir: TempDir,
    outbox: Arc<dyn Outbox>,
}

impl DatasetEntryServiceHarness {
    fn new(mock_dataset_entry_repository: MockDatasetEntryRepository) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = temp_dir.path().join("datasets");

        std::fs::create_dir(&datasets_dir).unwrap();

        let mut b = CatalogBuilder::new();

        b.add::<DatasetEntryService>();

        b.add_value(mock_dataset_entry_repository);
        b.bind::<dyn DatasetEntryRepository, MockDatasetEntryRepository>();

        let t = frozen_time_point();
        let fake_system_time_source = FakeSystemTimeSource::new(t);

        b.add_value(fake_system_time_source);
        b.bind::<dyn SystemTimeSource, FakeSystemTimeSource>();
        b.add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        );
        b.bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>();
        b.bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>();
        b.add::<InMemoryAccountRepository>();

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

        let catalog = b.build();

        Self {
            _temp_dir: temp_dir,
            outbox: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn frozen_time_point() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
