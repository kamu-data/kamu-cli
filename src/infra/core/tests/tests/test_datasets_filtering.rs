// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use kamu::utils::datasets_filtering::{match_local_dataset, match_remote_dataset};
use opendatafabric::{
    AccountName,
    DatasetAlias,
    DatasetAliasRemote,
    DatasetHandle,
    DatasetID,
    DatasetName,
    DatasetRefAnyPattern,
    RepoName,
};

#[test]
fn test_match_local_dataset() {
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

    assert!(!match_local_dataset(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(match_local_dataset(&pattern, &dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_account = "account1";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(match_local_dataset(&pattern, &dataset_handle));

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

    assert!(match_local_dataset(&pattern, &dataset_handle));

    let dataset_account = "account2";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!match_local_dataset(&pattern, &dataset_handle));
}

#[test]
fn test_match_remote_dataset() {
    let repo_name = "repository";
    let dataset_name = "net.example.com";
    let dataset_alias_remote = DatasetAliasRemote {
        repo_name: RepoName::from_str(repo_name).unwrap(),
        account_name: None,
        dataset_name: DatasetName::from_str(dataset_name).unwrap(),
    };

    let expression = "repository1/net.example%";
    let pattern = DatasetRefAnyPattern::from_str(expression).unwrap();

    assert!(!match_remote_dataset(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(match_remote_dataset(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!match_remote_dataset(&pattern, &dataset_alias_remote));

    let account_name = "account";
    let dataset_alias_remote = DatasetAliasRemote {
        repo_name: RepoName::from_str(repo_name).unwrap(),
        account_name: Some(AccountName::from_str(account_name).unwrap()),
        dataset_name: DatasetName::from_str(dataset_name).unwrap(),
    };

    assert!(match_remote_dataset(&pattern, &dataset_alias_remote));

    let expression = format!("{repo_name}/account1/net.example%");
    let pattern = DatasetRefAnyPattern::from_str(expression.as_str()).unwrap();

    assert!(!match_remote_dataset(&pattern, &dataset_alias_remote));
}
