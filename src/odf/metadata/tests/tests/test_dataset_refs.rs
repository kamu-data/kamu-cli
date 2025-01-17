// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use opendatafabric_metadata::*;
use url::Url;

#[test]
fn test_dataset_ref_pattern() {
    // Parse valid local dataset_ref
    let param = "net.example.com";
    let res = DatasetRefPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse valid multitenant local dataset_ref
    let param = "account/net.example.com";
    let res = DatasetRefPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse valid local did reference
    let param = "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let res = DatasetRefPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse invalid local dataset_ref
    let param = "invalid_ref^";
    let res = DatasetRefPattern::from_str(param).unwrap_err();

    assert_eq!(
        res.to_string(),
        format!("Value '{param}' is not a valid DatasetRefPattern"),
    );

    // Parse valid local ref with wildcard net.example.%
    let param = "net.example.%";
    let res = DatasetRefPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Pattern(DatasetAliasPattern {
            account_name: None,
            dataset_name_pattern: DatasetNamePattern::from_str(param).unwrap()
        }),
    );

    // Parse valid multitenant local ref with wildcard account/%
    let account = "account";
    let pattern = "%";
    let res = DatasetRefPattern::from_str(format!("{account}/{pattern}").as_str()).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Pattern(DatasetAliasPattern {
            account_name: Some(AccountName::from_str(account).unwrap()),
            dataset_name_pattern: DatasetNamePattern::from_str(pattern).unwrap(),
        }),
    );
}

#[test]
fn test_dataset_ref_pattern_match() {
    let dataset_id_str =
        "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let default_dataset_id = DatasetID::from_did_str(dataset_id_str).unwrap();
    let expression = "%odf%";
    let pattern = DatasetRefPattern::from_str(expression).unwrap();

    // Test match of DatasetRefPattern is Pattern type
    let dataset_name = "net.example.com";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!pattern.matches(&dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.matches(&dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_account = "account1";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.matches(&dataset_handle));

    let dataset_account = "account1";
    let dataset_name_pattern = "net%";
    let dataset_name = "net.example.com";

    let expression = format!("{dataset_account}/{dataset_name_pattern}");
    let pattern = DatasetRefPattern::from_str(expression.as_str()).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.matches(&dataset_handle));

    let dataset_account = "account2";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!pattern.matches(&dataset_handle));

    // Test match of DatasetRefPattern is Ref type
    let pattern = DatasetRefPattern::from_str(dataset_id_str).unwrap();
    let dataset_name = "net.example.com";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.matches(&dataset_handle));

    let expression = "net.example.com";
    let pattern = DatasetRefPattern::from_str(expression).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(expression).unwrap(),
        },
    };
    assert!(pattern.matches(&dataset_handle));

    let expression = "nEt.eXample%";
    let dataset_name = "net.example.com";
    let pattern = DatasetRefPattern::from_str(expression).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };
    assert!(pattern.matches(&dataset_handle));
}

#[test]
fn test_dataset_ref_any_pattern() {
    // Parse valid local dataset_ref
    let param = "net.example.com";
    let res = DatasetRefAnyPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::Ref(DatasetRefAny::LocalAlias(
            None,
            DatasetName::new_unchecked(param)
        ))
    );

    // Parse valid ambiguous dataset_ref
    let account_name = "account";
    let dataset_name = "net.example.com";
    let param = format!("{account_name}/{dataset_name}");
    let res = DatasetRefAnyPattern::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::Ref(DatasetRefAny::AmbiguousAlias(
            account_name.into(),
            DatasetName::new_unchecked(dataset_name)
        ))
    );

    // Parse valid local did reference
    let param = "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let res = DatasetRefAnyPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::Ref(DatasetRefAny::ID(
            None,
            DatasetID::from_did_str(param).unwrap()
        ))
    );

    // Parse valid remote url reference
    let param = "https://example.com";
    let res = DatasetRefAnyPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::Ref(DatasetRefAny::Url(Url::from_str(param).unwrap().into()))
    );

    // Parse invalid local dataset_ref
    let param = "invalid_ref^";
    let res = DatasetRefAnyPattern::from_str(param).unwrap_err();

    assert_eq!(
        res.to_string(),
        format!("Value '{param}' is not a valid DatasetRefAnyPattern"),
    );

    // Parse valid local ref with wildcard net.example.%
    let param = "net.example.%";
    let res = DatasetRefAnyPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::PatternLocal(DatasetNamePattern::from_str(param).unwrap()),
    );

    // Parse valid remote ambiguous ref with wildcard repo/net.example.%
    let repo_name = "repo";
    let dataset_name = "net.example.%";
    let param = format!("{repo_name}/{dataset_name}");

    let res = DatasetRefAnyPattern::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::PatternAmbiguous(
            DatasetAmbiguousPattern {
                pattern: DatasetNamePattern::from_str(repo_name)
                    .unwrap()
                    .into_inner(),
            },
            DatasetNamePattern::from_str(dataset_name).unwrap()
        ),
    );

    // Parse valid remote ref with repo and account wildcard repo/net.example.%
    let repo_name = "repo";
    let account_name = "account";
    let dataset_name = "net.example.%";
    let param = format!("{repo_name}/{account_name}/{dataset_name}");

    let res = DatasetRefAnyPattern::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefAnyPattern::PatternRemote(
            RepoName::new_unchecked(repo_name),
            AccountName::new_unchecked(account_name),
            DatasetNamePattern::from_str(dataset_name).unwrap()
        ),
    );
}

#[test]
fn test_dataset_push_target() {
    // Parse valid remote url ref
    let param = "http://net.example.com";
    let res = DatasetPushTarget::from_str(param).unwrap();

    assert_eq!(res, DatasetPushTarget::Url(Url::from_str(param).unwrap()));

    // Parse valid remote single tenant alias ref
    let repo_name = RepoName::new_unchecked("net.example.com");
    let dataset_name = DatasetName::new_unchecked("foo");

    let res = DatasetPushTarget::from_str(&format!("{repo_name}/{dataset_name}")).unwrap();

    assert_eq!(
        res,
        DatasetPushTarget::Alias(DatasetAliasRemote::new(
            repo_name.clone(),
            None,
            dataset_name.clone()
        ))
    );

    // Parse valid remote single tenant alias ref
    let account_name = AccountName::new_unchecked("bar");
    let res =
        DatasetPushTarget::from_str(&format!("{repo_name}/{account_name}/{dataset_name}")).unwrap();

    assert_eq!(
        res,
        DatasetPushTarget::Alias(DatasetAliasRemote::new(
            repo_name.clone(),
            Some(account_name.clone()),
            dataset_name.clone()
        ))
    );

    // Parse valid repository ref
    let param = "net.example.com";
    let res = DatasetPushTarget::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetPushTarget::Repository(RepoName::new_unchecked(param))
    );
}
