// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use futures::TryStreamExt;
use kamu::testing::BaseRepoHarness;
use kamu::utils::datasets_filtering::{
    get_local_datasets_stream,
    matches_local_ref_pattern,
    matches_remote_ref_pattern,
};
use kamu_accounts::DEFAULT_ACCOUNT_NAME;
use kamu_core::TenancyConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_matches_local_ref_pattern() {
    let dataset_id_str =
        "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let default_dataset_id = odf::DatasetID::from_did_str(dataset_id_str).unwrap();
    let expression = "%odf%";
    let pattern = odf::DatasetRefAnyPattern::from_str(expression).unwrap();

    let dataset_name = "net.example.com";
    let dataset_handle = odf::DatasetHandle {
        id: default_dataset_id.clone(),
        alias: odf::DatasetAlias {
            account_name: None,
            dataset_name: odf::DatasetName::new_unchecked(dataset_name),
        },
    };

    assert!(!matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_handle = odf::DatasetHandle {
        id: default_dataset_id.clone(),
        alias: odf::DatasetAlias {
            account_name: None,
            dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_account = "account1";
    let dataset_handle = odf::DatasetHandle {
        id: default_dataset_id.clone(),
        alias: odf::DatasetAlias {
            account_name: Some(odf::AccountName::from_str(dataset_account).unwrap()),
            dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_account = "account";
    let dataset_name_pattern = "net%";
    let dataset_name = "net.example.com";

    let expression = format!("{dataset_account}/{dataset_name_pattern}");
    let pattern = odf::DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();
    let dataset_handle = odf::DatasetHandle {
        id: default_dataset_id.clone(),
        alias: odf::DatasetAlias {
            account_name: Some(odf::AccountName::from_str(dataset_account).unwrap()),
            dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(matches_local_ref_pattern(&pattern, &dataset_handle));

    let dataset_account = "account2";
    let dataset_handle = odf::DatasetHandle {
        id: default_dataset_id.clone(),
        alias: odf::DatasetAlias {
            account_name: Some(odf::AccountName::from_str(dataset_account).unwrap()),
            dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!matches_local_ref_pattern(&pattern, &dataset_handle));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_matches_remote_ref_pattern() {
    let repo_name = "repository";
    let dataset_name = "net.example.com";
    let dataset_alias_remote = odf::DatasetAliasRemote {
        repo_name: odf::RepoName::from_str(repo_name).unwrap(),
        account_name: None,
        dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
    };

    let expression = "repository1/net.example%";
    let pattern = odf::DatasetRefAnyPattern::from_str(expression).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/net.example%");
    let pattern = odf::DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account/net.example%");
    let pattern = odf::DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let account_name = "account";
    let dataset_alias_remote = odf::DatasetAliasRemote {
        repo_name: odf::RepoName::from_str(repo_name).unwrap(),
        account_name: Some(odf::AccountName::from_str(account_name).unwrap()),
        dataset_name: odf::DatasetName::from_str(dataset_name).unwrap(),
    };

    assert!(matches_remote_ref_pattern(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account1/net.example%");
    let pattern = odf::DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!matches_remote_ref_pattern(&pattern, &dataset_alias_remote));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_get_local_datasets_stream_single_tenant() {
    let harness = DatasetFilteringHarness::new(TenancyConfig::SingleTenant);

    let alias_foo = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let alias_bar = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let alias_baz = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("baz"));

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let pattern = odf::DatasetRefAnyPattern::from_str("f%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        harness.dataset_registry(),
        vec![pattern],
        &DEFAULT_ACCOUNT_NAME,
    )
    .try_collect()
    .await
    .unwrap();

    assert_eq!(res, vec![foo.dataset_handle.as_any_ref()]);

    let pattern = odf::DatasetRefAnyPattern::from_str("b%").unwrap();
    let mut res: Vec<_> = get_local_datasets_stream(
        harness.dataset_registry(),
        vec![pattern],
        &DEFAULT_ACCOUNT_NAME,
    )
    .try_collect()
    .await
    .unwrap();
    DatasetFilteringHarness::sort_datasets_by_dataset_name(&mut res);

    assert_eq!(
        res,
        vec![
            bar.dataset_handle.as_any_ref(),
            baz.dataset_handle.as_any_ref()
        ]
    );

    let pattern = odf::DatasetRefAnyPattern::from_str("s%").unwrap();
    let res: Vec<_> = get_local_datasets_stream(
        harness.dataset_registry(),
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
    let harness = DatasetFilteringHarness::new(TenancyConfig::MultiTenant);

    let account_1 = odf::AccountName::new_unchecked("account1");
    let account_2 = odf::AccountName::new_unchecked("account2");

    let alias_foo = odf::DatasetAlias::new(
        Some(account_1.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );
    let alias_bar = odf::DatasetAlias::new(
        Some(account_2.clone()),
        odf::DatasetName::new_unchecked("bar"),
    );
    let alias_baz = odf::DatasetAlias::new(
        Some(account_1.clone()),
        odf::DatasetName::new_unchecked("baz"),
    );

    let foo = harness.create_root_dataset(&alias_foo).await;
    let bar = harness.create_root_dataset(&alias_bar).await;
    let baz = harness.create_root_dataset(&alias_baz).await;

    let pattern = odf::DatasetRefAnyPattern::from_str("account1/f%").unwrap();
    let res: Vec<_> =
        get_local_datasets_stream(harness.dataset_registry(), vec![pattern], &account_1)
            .try_collect()
            .await
            .unwrap();

    assert_eq!(res, vec![foo.dataset_handle.as_any_ref()]);

    let pattern = odf::DatasetRefAnyPattern::from_str("account2/b%").unwrap();
    let res: Vec<_> =
        get_local_datasets_stream(harness.dataset_registry(), vec![pattern], &account_2)
            .try_collect()
            .await
            .unwrap();

    assert_eq!(res, vec![bar.dataset_handle.as_any_ref()]);

    let pattern = odf::DatasetRefAnyPattern::from_str("account1/b%").unwrap();
    let res: Vec<_> =
        get_local_datasets_stream(harness.dataset_registry(), vec![pattern], &account_1)
            .try_collect()
            .await
            .unwrap();

    assert_eq!(res, vec![baz.dataset_handle.as_any_ref()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseRepoHarness, base_repo_harness)]
struct DatasetFilteringHarness {
    base_repo_harness: BaseRepoHarness,
}

impl DatasetFilteringHarness {
    fn new(tenancy_config: TenancyConfig) -> Self {
        let base_repo_harness = BaseRepoHarness::builder()
            .tenancy_config(tenancy_config)
            .build();
        Self { base_repo_harness }
    }

    fn sort_datasets_by_dataset_name(datasets: &mut [odf::DatasetRefAny]) {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
