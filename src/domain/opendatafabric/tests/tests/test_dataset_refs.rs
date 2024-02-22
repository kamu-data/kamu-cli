// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use opendatafabric::*;
use url::Url;

#[test]
fn test_dataset_ref_pattern_local() {
    // Parse valid local dataset_ref
    let param = "net.example.com";
    let res = DatasetRefPatternLocal::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternLocal::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse valid multitenant local dataset_ref
    let param = "account/net.example.com";
    let res = DatasetRefPatternLocal::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternLocal::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse valid local did reference
    let param = "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let res = DatasetRefPatternLocal::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternLocal::Ref(DatasetRef::from_str(param).unwrap())
    );

    // Parse invalid local dataset_ref
    let param = "invalid_ref^";
    let res = DatasetRefPatternLocal::from_str(param).unwrap_err();

    assert_eq!(
        res.to_string(),
        format!("Value '{param}' is not a valid DatasetRefPatternLocal"),
    );

    // Parse valid local ref with wildcard net.example.%
    let param = "net.example.%";
    let res = DatasetRefPatternLocal::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternLocal::Pattern(DatasetPattern {
            account_name: None,
            dataset_name_pattern: DatasetNamePattern::from_str(param).unwrap()
        }),
    );

    // Parse valid multitenant local ref with wildcard account/%
    let account = "account";
    let pattern = "%";
    let res = DatasetRefPatternLocal::from_str(format!("{account}/{pattern}").as_str()).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternLocal::Pattern(DatasetPattern {
            account_name: Some(AccountName::from_str(account).unwrap()),
            dataset_name_pattern: DatasetNamePattern::from_str(pattern).unwrap(),
        }),
    );
}

#[test]
fn test_dataset_ref_pattern_local_match() {
    let dataset_id_str =
        "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let default_dataset_id = DatasetID::from_did_str(dataset_id_str).unwrap();
    let expression = "%odf%";
    let pattern = DatasetRefPatternLocal::from_str(expression).unwrap();

    // Test match of DatasetRefPatternLocal is Pattern type
    let dataset_name = "net.example.com";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!pattern.is_match(&dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.is_match(&dataset_handle));

    let dataset_name = "net.example.odf";
    let dataset_account = "account1";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.is_match(&dataset_handle));

    let dataset_account = "account1";
    let dataset_name_pattern = "net%";
    let dataset_name = "net.example.com";

    let expression = format!("{dataset_account}/{dataset_name_pattern}");
    let pattern = DatasetRefPatternLocal::from_str(expression.as_str()).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.is_match(&dataset_handle));

    let dataset_account = "account2";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: Some(AccountName::from_str(dataset_account).unwrap()),
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(!pattern.is_match(&dataset_handle));

    // Test match of DatasetRefPatternLocal is Ref type
    let pattern = DatasetRefPatternLocal::from_str(dataset_id_str).unwrap();
    let dataset_name = "net.example.com";
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(dataset_name).unwrap(),
        },
    };

    assert!(pattern.is_match(&dataset_handle));

    let expression = "net.example.com";
    let pattern = DatasetRefPatternLocal::from_str(expression).unwrap();
    let dataset_handle = DatasetHandle {
        id: default_dataset_id.clone(),
        alias: DatasetAlias {
            account_name: None,
            dataset_name: DatasetName::from_str(expression).unwrap(),
        },
    };
    assert!(pattern.is_match(&dataset_handle));
}

#[test]
fn test_dataset_ref_pattern_any() {
    // Parse valid local dataset_ref
    let param = "net.example.com";
    let res = DatasetRefPatternAny::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::RefAny(DatasetRefAny::LocalAlias(
            None,
            DatasetName::from_str(param).unwrap()
        ))
    );

    // Parse valid ambigious dataset_ref
    let account_name = "account";
    let dataset_name = "net.example.com";
    let param = format!("{account_name}/{dataset_name}");
    let res = DatasetRefPatternAny::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::RefAny(DatasetRefAny::AmbiguousAlias(
            account_name.into(),
            DatasetName::from_str(dataset_name).unwrap()
        ))
    );

    // Parse valid local did reference
    let param = "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc";
    let res = DatasetRefPatternAny::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::RefAny(DatasetRefAny::ID(
            None,
            DatasetID::from_did_str(param).unwrap()
        ))
    );

    // Parse valid remote url reference
    let param = "https://example.com";
    let res = DatasetRefPatternAny::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::RefAny(DatasetRefAny::Url(Url::from_str(param).unwrap().into()))
    );

    // Parse invalid local dataset_ref
    let param = "invalid_ref^";
    let res = DatasetRefPatternAny::from_str(param).unwrap_err();

    assert_eq!(
        res.to_string(),
        format!("Value '{param}' is not a valid DatasetRefPatternAny"),
    );

    // Parse valid local ref with wildcard net.example.%
    let param = "net.example.%";
    let res = DatasetRefPatternAny::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::Pattern(DatasetPatternAny {
            account_repo_pattern: None,
            dataset_name_pattern: DatasetNamePattern::from_str(param).unwrap()
        }),
    );

    // Parse valid remote ambiguous ref with wildcard repo/net.example.%
    let repo_name = "repo";
    let dataset_name = "net.example.%";
    let param = format!("{repo_name}/{dataset_name}");

    let res = DatasetRefPatternAny::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::Pattern(DatasetPatternAny {
            account_repo_pattern: Some(RepoAccountPattern::AmbiguousPattern(repo_name.into())),
            dataset_name_pattern: DatasetNamePattern::from_str(dataset_name).unwrap()
        }),
    );

    // Parse valid remote ref with repo and account wildcard repo/net.example.%
    let repo_name = "repo";
    let account_name = "account";
    let dataset_name = "net.example.%";
    let param = format!("{repo_name}/{account_name}/{dataset_name}");

    let res = DatasetRefPatternAny::from_str(&param).unwrap();

    assert_eq!(
        res,
        DatasetRefPatternAny::Pattern(DatasetPatternAny {
            account_repo_pattern: Some(RepoAccountPattern::RemotePattern(
                DatasetRepoPattern::from_str(repo_name).unwrap(),
                Some(DatasetAccountPattern::from_str(account_name).unwrap())
            )),
            dataset_name_pattern: DatasetNamePattern::from_str(dataset_name).unwrap()
        }),
    );
}
