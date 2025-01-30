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

use itertools::Itertools;
use kamu::DatasetRegistrySoloUnitBridge;
use kamu_accounts::DEFAULT_ACCOUNT_NAME;
use kamu_core::DidGenerator;
use odf::dataset::testing::create_test_dataset_fron_snapshot;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter,
>(
    storage_unit: &TDatasetStorageUnit,
    account_name: Option<odf::AccountName>,
) {
    let dataset_alias =
        odf::DatasetAlias::new(account_name, odf::DatasetName::new_unchecked("foo"));

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );

    let create_result = storage_unit
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias);

    // We should see the dataset
    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
        .await
        .is_ok());

    // Now test name collision
    let create_result = storage_unit
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result.err(),
        Some(odf::dataset::CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_and_get_case_insensetive_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
    account_name: Option<odf::AccountName>,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let dataset_alias_to_create =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("Foo"));

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias_to_create.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );

    let create_result = storage_unit
        .create_dataset(
            &dataset_alias_to_create,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias_to_create);

    let account_name_uppercase = account_name.clone().map(|account_name_value| {
        odf::AccountName::new_unchecked(&account_name_value.to_ascii_uppercase())
    });

    let dataset_alias_in_another_registry = odf::DatasetAlias::new(
        account_name_uppercase,
        odf::DatasetName::new_unchecked("foO"),
    );

    // We should see the dataset
    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&dataset_alias_in_another_registry.as_local_ref())
        .await
        .is_ok());

    // Test creation another dataset for existing account with different symbols
    // registry
    let new_dataset_alias_to_create = odf::DatasetAlias::new(
        account_name
            .clone()
            .map(|a| odf::AccountName::new_unchecked(a.to_uppercase().as_str())),
        odf::DatasetName::new_unchecked("BaR"),
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(new_dataset_alias_to_create.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    // Assert dataset_name eq to new alias and account_name eq to old existing one
    assert_eq!(
        create_result.dataset_handle.alias.dataset_name,
        new_dataset_alias_to_create.dataset_name
    );
    assert_eq!(
        create_result.dataset_handle.alias.account_name,
        dataset_alias_to_create.account_name
    );

    // Now test name collision
    let create_result = storage_unit
        .create_dataset(
            &dataset_alias_in_another_registry,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result.err(),
        Some(odf::dataset::CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset_same_name_multiple_tenants<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let dataset_alias_my = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked("my")),
        odf::DatasetName::new_unchecked("foo"),
    );
    let dataset_alias_her = odf::DatasetAlias::new(
        Some(odf::AccountName::new_unchecked("her")),
        odf::DatasetName::new_unchecked("foo"),
    );

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias_my.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias_her.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );

    let snapshot_my = MetadataFactory::dataset_snapshot()
        .name(dataset_alias_my.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_her = MetadataFactory::dataset_snapshot()
        .name(dataset_alias_her.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result_my = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_my.clone(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    let create_result_her = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_her.clone(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    assert_eq!(create_result_her.dataset_handle.alias, dataset_alias_her);
    assert_eq!(create_result_my.dataset_handle.alias, dataset_alias_my);

    // We should see the datasets

    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&dataset_alias_my.as_local_ref())
        .await
        .is_ok());

    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&dataset_alias_her.as_local_ref())
        .await
        .is_ok());

    // Now test name collision
    let create_result_my = storage_unit
        .create_dataset(
            &dataset_alias_my,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    let create_result_her = storage_unit
        .create_dataset(
            &dataset_alias_her,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result_my.err(),
        Some(odf::dataset::CreateDatasetError::NameCollision(_))
    );

    assert_matches!(
        create_result_her.err(),
        Some(odf::dataset::CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset_from_snapshot<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
    account_name: Option<odf::AccountName>,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let dataset_alias =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("foo"));

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(dataset_alias.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot.clone(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    let hdl = storage_unit
        .resolve_stored_dataset_handle_by_ref(&create_result.dataset_handle.into())
        .await
        .unwrap();
    let dataset = storage_unit.get_stored_dataset_by_handle(&hdl);

    let actual_head = dataset
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(actual_head, create_result.head);

    assert_matches!(
        create_test_dataset_fron_snapshot(
            &dataset_registry,
            storage_unit.as_ref(),
            snapshot,
            did_generator.generate_dataset_id(),
            time_source.now(),
        )
        .await
        .err(),
        Some(odf::dataset::CreateDatasetFromSnapshotError::NameCollision(
            _
        ))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
    account_name: Option<odf::AccountName>,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let alias_foo =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("foo"));
    let alias_bar =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("bar"));
    let alias_baz =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("baz"));

    let snapshot_foo = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_bar = MetadataFactory::dataset_snapshot()
        .name(alias_bar.clone())
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(["foo"])
                .build(),
        )
        .build();

    let create_result_foo = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_foo,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_bar,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    assert_matches!(
        storage_unit
            .rename_dataset(
                &create_result_foo.create_dataset_result.dataset_handle,
                &alias_bar.dataset_name
            )
            .await,
        Err(odf::dataset::RenameDatasetError::NameCollision(_))
    );

    storage_unit
        .rename_dataset(
            &create_result_foo.create_dataset_result.dataset_handle,
            &alias_baz.dataset_name,
        )
        .await
        .unwrap();

    let baz_hdl = storage_unit
        .resolve_stored_dataset_handle_by_ref(&alias_baz.as_local_ref())
        .await
        .unwrap();
    let baz = storage_unit.get_stored_dataset_by_handle(&baz_hdl);

    use futures::StreamExt;
    use odf::dataset::MetadataChainExt;
    assert_eq!(baz.as_metadata_chain().iter_blocks().count().await, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset_same_name_multiple_tenants<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let account_my = odf::AccountName::new_unchecked("my");
    let account_her = odf::AccountName::new_unchecked("her");

    let dataset_alias_my_foo = odf::DatasetAlias::new(
        Some(account_my.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );
    let dataset_alias_her_bar = odf::DatasetAlias::new(
        Some(account_her.clone()),
        odf::DatasetName::new_unchecked("bar"),
    );
    let dataset_alias_my_baz = odf::DatasetAlias::new(
        Some(account_my.clone()),
        odf::DatasetName::new_unchecked("baz"),
    );

    let create_result_my_foo = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(dataset_alias_my_foo.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    let create_result_her_bar = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(dataset_alias_her_bar.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    let create_result_my_baz = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        MetadataFactory::dataset_snapshot()
            .name(dataset_alias_my_baz.clone())
            .kind(odf::DatasetKind::Root)
            .push_event(MetadataFactory::set_polling_source().build())
            .build(),
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    storage_unit
        .rename_dataset(
            &create_result_my_foo.dataset_handle,
            &odf::DatasetName::new_unchecked("bar"),
        )
        .await
        .unwrap();

    let my_bar_hdl = storage_unit
        .resolve_stored_dataset_handle_by_ref(&odf::DatasetRef::try_from("my/bar").unwrap())
        .await
        .unwrap();
    let my_bar = storage_unit.get_stored_dataset_by_handle(&my_bar_hdl);

    let her_bar_hdl = storage_unit
        .resolve_stored_dataset_handle_by_ref(&odf::DatasetRef::try_from("her/bar").unwrap())
        .await
        .unwrap();
    let her_bar = storage_unit.get_stored_dataset_by_handle(&her_bar_hdl);

    assert_eq!(
        my_bar
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap(),
        create_result_my_foo.head
    );
    assert_eq!(
        her_bar
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap(),
        create_result_her_bar.head
    );

    assert_matches!(
        storage_unit
            .rename_dataset(
                &create_result_my_baz.dataset_handle,
                &odf::DatasetName::new_unchecked("bar")
            )
            .await,
        Err(odf::dataset::RenameDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
    account_name: Option<odf::AccountName>,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let alias_foo =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("foo"));

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap()
    .create_dataset_result;

    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&alias_foo.as_local_ref())
        .await
        .is_ok());

    storage_unit
        .delete_dataset(&create_result.dataset_handle)
        .await
        .unwrap();

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&alias_foo.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_iterate_datasets<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));

    let snapshot_foo = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_bar = MetadataFactory::dataset_snapshot()
        .name("bar")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(["foo"])
                .build(),
        )
        .build();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_foo,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_bar,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    // All
    check_expected_datasets(
        vec![alias_bar.clone(), alias_foo.clone()],
        storage_unit.stored_dataset_handles(),
    )
    .await;

    // Default account
    check_expected_datasets(
        vec![alias_bar, alias_foo],
        storage_unit.stored_dataset_handles_by_owner(&DEFAULT_ACCOUNT_NAME),
    )
    .await;

    // Random account
    check_expected_datasets(
        vec![],
        storage_unit
            .stored_dataset_handles_by_owner(&odf::AccountName::new_unchecked("unknown-account")),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_iterate_datasets_multi_tenant<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter + 'static,
>(
    storage_unit: Arc<TDatasetStorageUnit>,
    did_generator: &dyn DidGenerator,
    time_source: &dyn SystemTimeSource,
) {
    let dataset_registry = DatasetRegistrySoloUnitBridge::new(storage_unit.clone());

    let account_my = odf::AccountName::new_unchecked("my");
    let account_her = odf::AccountName::new_unchecked("her");

    let alias_my_foo = odf::DatasetAlias::new(
        Some(account_my.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );
    let alias_her_foo = odf::DatasetAlias::new(
        Some(account_her.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );
    let alias_her_bar = odf::DatasetAlias::new(
        Some(account_her.clone()),
        odf::DatasetName::new_unchecked("bar"),
    );
    let alias_my_baz = odf::DatasetAlias::new(
        Some(account_my.clone()),
        odf::DatasetName::new_unchecked("baz"),
    );

    let snapshot_my_foo = MetadataFactory::dataset_snapshot()
        .name("my/foo")
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();
    let snapshot_my_baz = MetadataFactory::dataset_snapshot()
        .name("my/baz")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases([("my/foo", "foo")])
                .build(),
        )
        .build();

    let snapshot_her_foo = MetadataFactory::dataset_snapshot()
        .name("her/foo")
        .kind(odf::DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();
    let snapshot_her_bar = MetadataFactory::dataset_snapshot()
        .name("her/bar")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases([("her/foo", "foo")])
                .build(),
        )
        .build();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_my_foo,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_my_baz,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_her_foo,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    create_test_dataset_fron_snapshot(
        &dataset_registry,
        storage_unit.as_ref(),
        snapshot_her_bar,
        did_generator.generate_dataset_id(),
        time_source.now(),
    )
    .await
    .unwrap();

    check_expected_datasets(
        vec![
            alias_her_bar.clone(),
            alias_her_foo.clone(),
            alias_my_baz.clone(),
            alias_my_foo.clone(),
        ],
        storage_unit.stored_dataset_handles(),
    )
    .await;

    check_expected_datasets(
        vec![alias_my_baz, alias_my_foo],
        storage_unit.stored_dataset_handles_by_owner(&account_my),
    )
    .await;

    check_expected_datasets(
        vec![alias_her_bar, alias_her_foo],
        storage_unit.stored_dataset_handles_by_owner(&account_her),
    )
    .await;

    // Random account
    check_expected_datasets(
        vec![],
        storage_unit
            .stored_dataset_handles_by_owner(&odf::AccountName::new_unchecked("unknown-account")),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn check_expected_datasets(
    expected_aliases: Vec<odf::DatasetAlias>,
    actual_datasets_stream: odf::dataset::DatasetHandleStream<'_>,
) {
    use futures::TryStreamExt;
    let mut actual_datasets: Vec<_> = actual_datasets_stream.try_collect().await.unwrap();
    actual_datasets.sort_by_key(|d| d.alias.to_string());

    assert_eq!(
        expected_aliases,
        actual_datasets
            .iter()
            .map(|d| d.alias.clone())
            .collect_vec()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_multiple_datasets_with_same_id<
    TDatasetStorageUnit: odf::DatasetStorageUnit + odf::DatasetStorageUnitWriter,
>(
    storage_unit: &TDatasetStorageUnit,
    account_name: Option<odf::AccountName>,
) {
    let dataset_alias =
        odf::DatasetAlias::new(account_name.clone(), odf::DatasetName::new_unchecked("foo"));

    assert_matches!(
        storage_unit
            .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        odf::dataset::GetDatasetError::NotFound(_)
    );
    let seed_block =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build_typed();

    let create_result = storage_unit
        .create_dataset(&dataset_alias, seed_block.clone())
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias);

    // We should see the dataset
    assert!(storage_unit
        .resolve_stored_dataset_handle_by_ref(&dataset_alias.as_local_ref())
        .await
        .is_ok());

    let dataset_alias =
        odf::DatasetAlias::new(account_name, odf::DatasetName::new_unchecked("bar"));

    // Now test id collision with different alias
    let create_result = storage_unit
        .create_dataset(&dataset_alias, seed_block)
        .await;

    assert_matches!(
        create_result.err(),
        Some(odf::dataset::CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
