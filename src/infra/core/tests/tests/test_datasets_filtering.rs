// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use dill::Component;
use event_bus::EventBus;
use futures::TryStreamExt;
use kamu::testing::MetadataFactory;
use kamu::utils::datasets_filtering::{
    get_local_datasets_stream,
    matches_local_ref_pattern,
    matches_remote_ref_pattern,
};
use kamu::{DatasetRepositoryLocalFs, DependencyGraphServiceInMemory};
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_core::{auth, DatasetRepository, SystemTimeSourceDefault};
use opendatafabric::{
    AccountName,
    DatasetAlias,
    DatasetAliasRemote,
    DatasetHandle,
    DatasetID,
    DatasetKind,
    DatasetName,
    DatasetRefAny,
    DatasetRefAnyPattern,
    RepoName,
};
use tempfile::TempDir;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_matches_local_ref_pattern() {
    let dataset_id_str =
        "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let default_dataset_id = DatasetID::from_did_str(dataset_id_str).unwrap();
    let expression = "%odf%";
    let pattern = DatasetRefAnyPattern::from_str(expression).unwrap();

    let dataset_name = "net.example.com";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::new_unchecked(dataset_name),
        },
    };

    assert!(!matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_account = "account1";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_account = "account";
    let dataset_name_pattern = "net%";
    let dataset_name = "net.example.com";

    let expression = format!("{dataset_account}/{dataset_name_pattern}");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_account = "account2";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!matches_local_ref_pattern(&pattern, &dataset_handle));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_matches_remote_ref_pattern() {
    let repo_name = "repository";
    let dataset_name = "net.example.com";
    let dataset_alias_remote = DatasetAliasRemote {
        repo_name: RepoName::from_str(repo_name).unwrap(),
        account_name: None,
        dataset_name: DatasetName::from_str(dataset_name).unwrap(),
    };

    let expression = "repository1/net.example%";
    let pattern = DatasetRefAnyPattern::from_str(expression).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let account_name = "account";
    let dataset_alias_remote = DatasetAliasRemote {
        repo_name: RepoName::from_str(repo_name).unwrap(),
        account_name: Some(AccountName::from_str(account_name).unwrap()),
        dataset_name: DatasetName::from_str(dataset_name).unwrap(),
    };

    assert!(matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account1/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_local_datasets_stream_single_tenant() {
    let dataset_filtering_harness = DatasetFilteringHarness::new(false);
    let foo_handle = dataset_filtering_harness
        .create_root_dataset(None, "foo")
        .await;
    let bar_handle = dataset_filtering_harness
        .create_root_dataset(None, "bar")
        .await;
    let baz_handle = dataset_filtering_harness
        .create_root_dataset(None, "baz")
        .await;

    let pattern = DatasetRefAnyPattern::from_str("f%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern],
        &DEFAULT_ACCOUNT_NAME,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![foo_handle.as_any_ref()]);

    let pattern = DatasetRefAnyPattern::from_str("b%").unwrap();
    let mut res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern],
        &DEFAULT_ACCOUNT_NAME,
    )
    .try_collect()
    .await
    .unwrap();
    DatasetFilteringHarness::sort_datasets_by_dataset_name(&mut res);

    assert_eq!(res, vec![bar_handle.as_any_ref(), baz_handle.as_any_ref()]);

    let pattern = DatasetRefAnyPattern::from_str("s%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern.clone()],
        &DEFAULT_ACCOUNT_NAME,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_local_datasets_stream_multi_tenant() {
    let dataset_filtering_harness = DatasetFilteringHarness::new(true);
    let account_1 = AccountName::new_unchecked("account1");
    let account_2 = AccountName::new_unchecked("account2");

    let foo_handle = dataset_filtering_harness
        .create_root_dataset(Some(account_1.clone()), "foo")
        .await;
    let bar_handle = dataset_filtering_harness
        .create_root_dataset(Some(account_2.clone()), "bar")
        .await;
    let baz_handle = dataset_filtering_harness
        .create_root_dataset(Some(account_1.clone()), "baz")
        .await;

    let pattern = DatasetRefAnyPattern::from_str("account1/f%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern],
        &account_1,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![foo_handle.as_any_ref()]);

    let pattern = DatasetRefAnyPattern::from_str("account2/b%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern],
        &account_2,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![bar_handle.as_any_ref()]);

    let pattern = DatasetRefAnyPattern::from_str("account1/b%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        dataset_filtering_harness.dataset_repo.as_ref(),
        vec![pattern],
        &account_1,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![baz_handle.as_any_ref()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetFilteringHarness {
    _workdir: TempDir,
    _catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl DatasetFilteringHarness {
    fn new(is_multi_tenant: bool) -> Self {
        let workdir = tempfile::tempdir().unwrap();
        let datasets_dir = workdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<SystemTimeSourceDefault>()
            .add::<EventBus>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(is_multi_tenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .build();

        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

        Self {
            _workdir: workdir,
            _catalog: catalog,
            dataset_repo,
        }
    }

    async fn create_root_dataset(
        &self,
        account_name: Option<AccountName>,
        dataset_name: &str,
    ) -> DatasetHandle {
        self.dataset_repo
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
            .dataset_handle
    }

    fn sort_datasets_by_dataset_name(datasets: &mut [DatasetRefAny]) {
        datasets.sort_by(|a, b| {
            a.as_local_ref(|_| false)
                .unwrap()
                .alias()
                .unwrap()
                .dataset_name
                .cmp(
                    &b.as_local_ref(|_| false)
                        .unwrap()
                        .alias()
                        .unwrap()
                        .dataset_name,
                )
        });
    }
}
