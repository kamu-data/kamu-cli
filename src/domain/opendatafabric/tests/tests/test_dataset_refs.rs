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

#[test]
fn test_dataset_ref_patterns() {
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
        format!("Value '{}' is not a valid DatasetRefPattern", param),
    );

    // Parse valid local ref with wildcard net.example.%
    let param = "net.example.%";
    let res = DatasetRefPattern::from_str(param).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Pattern(None, DatasetNamePattern::from_str(param).unwrap()),
    );

    // Parse valid multitenant local ref with wildcard account/%
    let account = "account";
    let pattern = "%";
    let res = DatasetRefPattern::from_str(format!("{}/{}", account, pattern).as_str()).unwrap();

    assert_eq!(
        res,
        DatasetRefPattern::Pattern(
            Some(AccountName::from_str(account).unwrap()),
            DatasetNamePattern::from_str(pattern).unwrap(),
        ),
    );
}

#[test]
fn test_dataset_ref_pattern_match() {}
