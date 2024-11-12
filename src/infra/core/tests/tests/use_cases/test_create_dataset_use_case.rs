// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use dill::{Catalog, Component};
use kamu::testing::MetadataFactory;
use kamu::{CreateDatasetUseCaseImpl, DatasetRepositoryLocalFs, DatasetRepositoryWriter};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CreateDatasetUseCase,
    DatasetLifecycleMessage,
    DatasetRepository,
    GetDatasetError,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox, OUTBOX_MESSAGE_VERSION};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let mut mock_outbox = MockOutbox::new();
    CreateUseCaseHarness::add_outbox_dataset_created_expectation(&mut mock_outbox, 1);

    let harness = CreateUseCaseHarness::new(mock_outbox);

    harness
        .use_case
        .execute(
            &alias_foo,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
            Default::default(),
        )
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CreateUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn CreateDatasetUseCase>,
}

impl CreateUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<CreateDatasetUseCaseImpl>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .build();

        let use_case = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
        }
    }

    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().unwrap();
        dataset_repo
            .find_dataset_by_ref(&alias.as_local_ref())
            .await?;
        Ok(())
    }

    fn add_outbox_dataset_created_expectation(mock_outbox: &mut MockOutbox, times: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::Created(_))
                    )
                }),
                eq(OUTBOX_MESSAGE_VERSION),
            )
            .times(times)
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
