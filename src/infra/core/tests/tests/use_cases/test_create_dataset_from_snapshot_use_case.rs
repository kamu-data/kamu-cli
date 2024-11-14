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

use dill::Catalog;
use kamu::testing::MetadataFactory;
use kamu::CreateDatasetFromSnapshotUseCaseImpl;
use kamu_core::{
    CreateDatasetFromSnapshotUseCase,
    DatasetLifecycleMessage,
    GetDatasetError,
    TenancyConfig,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName};

use crate::BaseRepoHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset_from_snapshot() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    // Expect only DatasetCreated message for "foo"
    let mut mock_outbox = MockOutbox::new();
    CreateFromSnapshotUseCaseHarness::add_outbox_dataset_created_expectation(&mut mock_outbox, 1);

    let harness = CreateFromSnapshotUseCaseHarness::new(mock_outbox);

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    harness
        .use_case
        .execute(snapshot, Default::default())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_derived_dataset_from_snapshot() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    // Expect DatasetCreated messages for "foo" and "bar"
    // Expect DatasetDependenciesUpdated message for "bar"
    let mut mock_outbox = MockOutbox::new();
    CreateFromSnapshotUseCaseHarness::add_outbox_dataset_created_expectation(&mut mock_outbox, 2);
    CreateFromSnapshotUseCaseHarness::add_outbox_dataset_dependencies_updated_expectation(
        &mut mock_outbox,
        1,
    );

    let harness = CreateFromSnapshotUseCaseHarness::new(mock_outbox);

    let snapshot_root = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_derived = MetadataFactory::dataset_snapshot()
        .name(alias_bar.clone())
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(vec![alias_foo.as_local_ref()])
                .build(),
        )
        .build();

    let options = Default::default();

    harness
        .use_case
        .execute(snapshot_root, options)
        .await
        .unwrap();
    harness
        .use_case
        .execute(snapshot_derived, options)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CreateFromSnapshotUseCaseHarness {
    base_repo_harness: BaseRepoHarness,
    _catalog: Catalog,
    use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
}

impl CreateFromSnapshotUseCaseHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let base_repo_harness = BaseRepoHarness::new(TenancyConfig::SingleTenant);

        let mut b = dill::CatalogBuilder::new_chained(base_repo_harness.catalog());

        b.add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>();

        let catalog = b.build();
        let use_case = catalog.get_one().unwrap();

        Self {
            base_repo_harness,
            _catalog: catalog,
            use_case,
        }
    }

    #[inline]
    async fn check_dataset_exists(&self, alias: &DatasetAlias) -> Result<(), GetDatasetError> {
        self.base_repo_harness.check_dataset_exists(alias).await
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
            )
            .times(times)
            .returning(|_, _| Ok(()));
    }

    fn add_outbox_dataset_dependencies_updated_expectation(
        mock_outbox: &mut MockOutbox,
        times: usize,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                        Ok(DatasetLifecycleMessage::DependenciesUpdated(_))
                    )
                }),
            )
            .times(times)
            .returning(|_, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
