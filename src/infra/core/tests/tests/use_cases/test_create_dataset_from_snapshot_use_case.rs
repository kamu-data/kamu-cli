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
use kamu::{
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
};
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac::{MockRebacRepository, RebacService};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::{
    CreateDatasetFromSnapshotUseCase,
    DatasetLifecycleMessage,
    DatasetRepository,
    GetDatasetError,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use opendatafabric::{DatasetAlias, DatasetKind, DatasetName};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_create_root_dataset_from_snapshot() {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    // Expect only DatasetCreated message for "foo"
    let mut mock_outbox = MockOutbox::new();
    CreateFromSnapshotUseCaseHarness::add_outbox_dataset_created_expectation(&mut mock_outbox, 1);

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_outbox,
        Workspace::SingleTenant,
        RebacServiceVariant::InMemory,
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_res = harness
        .use_case
        .execute(snapshot, &Default::default())
        .await
        .unwrap();

    // Properties are set for multi-tenants only
    assert_matches!(
        harness
            .rebac_service
            .get_dataset_properties(&create_res.dataset_handle.id)
            .await,
        Ok(props)
            if props.is_empty()
    );
    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
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

    let harness = CreateFromSnapshotUseCaseHarness::new(
        mock_outbox,
        Workspace::SingleTenant,
        RebacServiceVariant::InMemory,
    );

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

    let foo_create_res = harness
        .use_case
        .execute(snapshot_root, &options)
        .await
        .unwrap();
    let bar_create_res = harness
        .use_case
        .execute(snapshot_derived, &options)
        .await
        .unwrap();

    assert_matches!(harness.check_dataset_exists(&alias_foo).await, Ok(_));
    assert_matches!(harness.check_dataset_exists(&alias_bar).await, Ok(_));

    // Properties are set for multi-tenants only
    assert_matches!(
        harness
            .rebac_service
            .get_dataset_properties(&foo_create_res.dataset_handle.id)
            .await,
        Ok(props)
            if props.is_empty()
    );
    assert_matches!(
        harness
            .rebac_service
            .get_dataset_properties(&bar_create_res.dataset_handle.id)
            .await,
        Ok(props)
            if props.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone)]
enum Workspace {
    SingleTenant,
    // TODO: remove
    #[allow(dead_code)]
    MultiTenant,
}

impl Workspace {
    fn is_multi_tenant(self) -> bool {
        match self {
            Workspace::SingleTenant => false,
            Workspace::MultiTenant => true,
        }
    }
}

enum RebacServiceVariant {
    InMemory,
    // TODO: remove
    #[allow(dead_code)]
    Mocked(MockRebacRepository),
}

struct CreateFromSnapshotUseCaseHarness {
    _temp_dir: tempfile::TempDir,
    catalog: Catalog,
    use_case: Arc<dyn CreateDatasetFromSnapshotUseCase>,
    pub rebac_service: Arc<dyn RebacService>,
}

impl CreateFromSnapshotUseCaseHarness {
    fn new(
        mock_outbox: MockOutbox,
        workspace: Workspace,
        rebac_service_kind: RebacServiceVariant,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let mut b = dill::CatalogBuilder::new();

        b.add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(workspace.is_multi_tenant()),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .add::<RebacServiceImpl>();

        match rebac_service_kind {
            RebacServiceVariant::InMemory => b.add::<InMemoryRebacRepository>(),
            RebacServiceVariant::Mocked(mock_rebac_repo) => b.add_value(mock_rebac_repo),
        };

        let catalog = b.build();

        let use_case = catalog
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        let rebac_service = catalog.get_one::<dyn RebacService>().unwrap();

        Self {
            _temp_dir: tempdir,
            catalog,
            use_case,
            rebac_service,
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
