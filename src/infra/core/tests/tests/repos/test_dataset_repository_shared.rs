// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use itertools::Itertools;
use kamu::domain::*;
use kamu::testing::MetadataFactory;
use kamu::DatasetRepositoryWriter;
use kamu_accounts::DEFAULT_ACCOUNT_NAME;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    account_name: Option<AccountName>,
) {
    let dataset_alias = DatasetAlias::new(account_name, DatasetName::new_unchecked("foo"));

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let create_result = repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias);

    // We should see the dataset
    assert!(repo
        .get_dataset_by_ref(&dataset_alias.as_local_ref())
        .await
        .is_ok());

    // Now test name collision
    let create_result = repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result.err(),
        Some(CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_and_get_case_insensetive_dataset<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    account_name: Option<AccountName>,
) {
    let dataset_alias_to_create =
        DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("Foo"));

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias_to_create.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let create_result = repo
        .create_dataset(
            &dataset_alias_to_create,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias_to_create);

    let account_name_uppercase = account_name.clone().map(|account_name_value| {
        AccountName::new_unchecked(&account_name_value.to_ascii_uppercase())
    });

    let dataset_alias_in_another_registry =
        DatasetAlias::new(account_name_uppercase, DatasetName::new_unchecked("foO"));

    // We should see the dataset
    assert!(repo
        .get_dataset_by_ref(&dataset_alias_in_another_registry.as_local_ref())
        .await
        .is_ok());

    // Test creation another dataset for existing account with different symbols
    // registry
    let new_dataset_alias_to_create = DatasetAlias::new(
        account_name
            .clone()
            .map(|a| AccountName::new_unchecked(a.to_uppercase().as_str())),
        DatasetName::new_unchecked("BaR"),
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(new_dataset_alias_to_create.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = repo
        .create_dataset_from_snapshot(snapshot)
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
    let create_result = repo
        .create_dataset(
            &dataset_alias_in_another_registry,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result.err(),
        Some(CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset_same_name_multiple_tenants<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
) {
    let dataset_alias_my = DatasetAlias::new(
        Some(AccountName::new_unchecked("my")),
        DatasetName::new_unchecked("foo"),
    );
    let dataset_alias_her = DatasetAlias::new(
        Some(AccountName::new_unchecked("her")),
        DatasetName::new_unchecked("foo"),
    );

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias_my.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias_her.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let snapshot_my = MetadataFactory::dataset_snapshot()
        .name(dataset_alias_my.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_her = MetadataFactory::dataset_snapshot()
        .name(dataset_alias_her.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result_my = repo
        .create_dataset_from_snapshot(snapshot_my.clone())
        .await
        .unwrap()
        .create_dataset_result;

    let create_result_her = repo
        .create_dataset_from_snapshot(snapshot_her.clone())
        .await
        .unwrap()
        .create_dataset_result;

    assert_eq!(create_result_her.dataset_handle.alias, dataset_alias_her);
    assert_eq!(create_result_my.dataset_handle.alias, dataset_alias_my);

    // We should see the datasets

    assert!(repo
        .get_dataset_by_ref(&dataset_alias_my.as_local_ref())
        .await
        .is_ok());

    assert!(repo
        .get_dataset_by_ref(&dataset_alias_her.as_local_ref())
        .await
        .is_ok());

    // Now test name collision
    let create_result_my = repo
        .create_dataset(
            &dataset_alias_my,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    let create_result_her = repo
        .create_dataset(
            &dataset_alias_her,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await;

    assert_matches!(
        create_result_my.err(),
        Some(CreateDatasetError::NameCollision(_))
    );

    assert_matches!(
        create_result_her.err(),
        Some(CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_create_dataset_from_snapshot<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    account_name: Option<AccountName>,
) {
    let dataset_alias = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("foo"));

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(dataset_alias.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = repo
        .create_dataset_from_snapshot(snapshot.clone())
        .await
        .unwrap()
        .create_dataset_result;

    let dataset = repo
        .get_dataset_by_ref(&create_result.dataset_handle.into())
        .await
        .unwrap();

    let actual_head = dataset
        .as_metadata_chain()
        .resolve_ref(&BlockRef::Head)
        .await
        .unwrap();

    assert_eq!(actual_head, create_result.head);

    assert_matches!(
        repo.create_dataset_from_snapshot(snapshot).await.err(),
        Some(CreateDatasetFromSnapshotError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    account_name: Option<AccountName>,
) {
    let alias_foo = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("bar"));
    let alias_baz = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("baz"));

    let snapshot_foo = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_bar = MetadataFactory::dataset_snapshot()
        .name(alias_bar.clone())
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(["foo"])
                .build(),
        )
        .build();

    let create_result_foo = repo
        .create_dataset_from_snapshot(snapshot_foo)
        .await
        .unwrap();
    repo.create_dataset_from_snapshot(snapshot_bar)
        .await
        .unwrap();

    assert_matches!(
        repo.rename_dataset(
            &create_result_foo.create_dataset_result.dataset_handle,
            &alias_bar.dataset_name
        )
        .await,
        Err(RenameDatasetError::NameCollision(_))
    );

    repo.rename_dataset(
        &create_result_foo.create_dataset_result.dataset_handle,
        &alias_baz.dataset_name,
    )
    .await
    .unwrap();

    let baz = repo
        .get_dataset_by_ref(&alias_baz.as_local_ref())
        .await
        .unwrap();

    use futures::StreamExt;
    assert_eq!(baz.as_metadata_chain().iter_blocks().count().await, 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset_same_name_multiple_tenants<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
) {
    let account_my = AccountName::new_unchecked("my");
    let account_her = AccountName::new_unchecked("her");

    let dataset_alias_my_foo =
        DatasetAlias::new(Some(account_my.clone()), DatasetName::new_unchecked("foo"));
    let dataset_alias_her_bar =
        DatasetAlias::new(Some(account_her.clone()), DatasetName::new_unchecked("bar"));
    let dataset_alias_my_baz =
        DatasetAlias::new(Some(account_my.clone()), DatasetName::new_unchecked("baz"));

    let create_result_my_foo = repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias_my_foo.clone())
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap()
        .create_dataset_result;

    let create_result_her_bar = repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias_her_bar.clone())
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap()
        .create_dataset_result;

    let create_result_my_baz = repo
        .create_dataset_from_snapshot(
            MetadataFactory::dataset_snapshot()
                .name(dataset_alias_my_baz.clone())
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    repo.rename_dataset(
        &create_result_my_foo.dataset_handle,
        &DatasetName::new_unchecked("bar"),
    )
    .await
    .unwrap();

    let my_bar = repo
        .get_dataset_by_ref(&DatasetRef::try_from("my/bar").unwrap())
        .await
        .unwrap();

    let her_bar = repo
        .get_dataset_by_ref(&DatasetRef::try_from("her/bar").unwrap())
        .await
        .unwrap();

    assert_eq!(
        my_bar
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap(),
        create_result_my_foo.head
    );
    assert_eq!(
        her_bar
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .unwrap(),
        create_result_her_bar.head
    );

    assert_matches!(
        repo.rename_dataset(
            &create_result_my_baz.create_dataset_result.dataset_handle,
            &DatasetName::new_unchecked("bar")
        )
        .await,
        Err(RenameDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    create_dataset_from_snapshot: &dyn CreateDatasetFromSnapshotUseCase,
    account_name: Option<AccountName>,
) {
    let alias_foo = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("foo"));

    let snapshot = MetadataFactory::dataset_snapshot()
        .name(alias_foo.clone())
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let create_result = create_dataset_from_snapshot
        .execute(snapshot, Default::default())
        .await
        .unwrap();

    assert!(repo
        .get_dataset_by_ref(&alias_foo.as_local_ref())
        .await
        .is_ok());

    repo.delete_dataset(&create_result.dataset_handle)
        .await
        .unwrap();

    assert_matches!(
        repo.get_dataset_by_ref(&alias_foo.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_iterate_datasets<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
) {
    let alias_foo = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let alias_bar = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));

    let snapshot_foo = MetadataFactory::dataset_snapshot()
        .name("foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();

    let snapshot_bar = MetadataFactory::dataset_snapshot()
        .name("bar")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(["foo"])
                .build(),
        )
        .build();

    repo.create_dataset_from_snapshot(snapshot_foo)
        .await
        .unwrap();
    repo.create_dataset_from_snapshot(snapshot_bar)
        .await
        .unwrap();

    // All
    check_expected_datasets(
        vec![alias_bar.clone(), alias_foo.clone()],
        repo.all_dataset_handles(),
    )
    .await;

    // Default account
    check_expected_datasets(
        vec![alias_bar, alias_foo],
        repo.all_dataset_handles_by_owner(&DEFAULT_ACCOUNT_NAME),
    )
    .await;

    // Random account
    check_expected_datasets(
        vec![],
        repo.all_dataset_handles_by_owner(&AccountName::new_unchecked("unknown-account")),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_iterate_datasets_multi_tenant<
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
) {
    let account_my = AccountName::new_unchecked("my");
    let account_her = AccountName::new_unchecked("her");

    let alias_my_foo =
        DatasetAlias::new(Some(account_my.clone()), DatasetName::new_unchecked("foo"));
    let alias_her_foo =
        DatasetAlias::new(Some(account_her.clone()), DatasetName::new_unchecked("foo"));
    let alias_her_bar =
        DatasetAlias::new(Some(account_her.clone()), DatasetName::new_unchecked("bar"));
    let alias_my_baz =
        DatasetAlias::new(Some(account_my.clone()), DatasetName::new_unchecked("baz"));

    let snapshot_my_foo = MetadataFactory::dataset_snapshot()
        .name("my/foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();
    let snapshot_my_baz = MetadataFactory::dataset_snapshot()
        .name("my/baz")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases([("my/foo", "foo")])
                .build(),
        )
        .build();

    let snapshot_her_foo = MetadataFactory::dataset_snapshot()
        .name("her/foo")
        .kind(DatasetKind::Root)
        .push_event(MetadataFactory::set_polling_source().build())
        .build();
    let snapshot_her_bar = MetadataFactory::dataset_snapshot()
        .name("her/bar")
        .kind(DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs_and_aliases([("her/foo", "foo")])
                .build(),
        )
        .build();

    repo.create_dataset_from_snapshot(snapshot_my_foo)
        .await
        .unwrap();
    repo.create_dataset_from_snapshot(snapshot_my_baz)
        .await
        .unwrap();

    repo.create_dataset_from_snapshot(snapshot_her_foo)
        .await
        .unwrap();
    repo.create_dataset_from_snapshot(snapshot_her_bar)
        .await
        .unwrap();

    check_expected_datasets(
        vec![
            alias_her_bar.clone(),
            alias_her_foo.clone(),
            alias_my_baz.clone(),
            alias_my_foo.clone(),
        ],
        repo.all_dataset_handles(),
    )
    .await;

    check_expected_datasets(
        vec![alias_my_baz, alias_my_foo],
        repo.all_dataset_handles_by_owner(&account_my),
    )
    .await;

    check_expected_datasets(
        vec![alias_her_bar, alias_her_foo],
        repo.all_dataset_handles_by_owner(&account_her),
    )
    .await;

    // Random account
    check_expected_datasets(
        vec![],
        repo.all_dataset_handles_by_owner(&AccountName::new_unchecked("unknown-account")),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn check_expected_datasets(
    expected_aliases: Vec<DatasetAlias>,
    actual_datasets_stream: DatasetHandleStream<'_>,
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
    TDatasetRepository: DatasetRepository + DatasetRepositoryWriter,
>(
    repo: &TDatasetRepository,
    account_name: Option<AccountName>,
) {
    let dataset_alias = DatasetAlias::new(account_name.clone(), DatasetName::new_unchecked("foo"));

    assert_matches!(
        repo.get_dataset_by_ref(&dataset_alias.as_local_ref())
            .await
            .err()
            .unwrap(),
        GetDatasetError::NotFound(_)
    );
    let seed_block =
        MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
            .build_typed();

    let create_result = repo
        .create_dataset(&dataset_alias, seed_block.clone())
        .await
        .unwrap();

    assert_eq!(create_result.dataset_handle.alias, dataset_alias);

    // We should see the dataset
    assert!(repo
        .get_dataset_by_ref(&dataset_alias.as_local_ref())
        .await
        .is_ok());

    let dataset_alias = DatasetAlias::new(account_name, DatasetName::new_unchecked("bar"));

    // Now test id collision with different alias
    let create_result = repo.create_dataset(&dataset_alias, seed_block).await;

    assert_matches!(
        create_result.err(),
        Some(CreateDatasetError::NameCollision(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
