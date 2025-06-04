// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::str::FromStr;

use kamu_auth_rebac::{
    AccountPropertyName,
    DatasetPropertyName,
    PROPERTY_GROUP_SEPARATOR,
    PropertyName,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SEP: &str = PROPERTY_GROUP_SEPARATOR;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_invalid_format() {
    let property = DatasetPropertyName::AllowsAnonymousRead;
    let group = PropertyName::Dataset(property).property_group();
    let unexpected_but_true_appendix =
        "A computer program does what you tell it to do, not what you want it to do.";

    let input = format!("{group}{SEP}{property}{SEP}{unexpected_but_true_appendix}");

    assert_matches!(
        PropertyName::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Invalid format for value: 'dataset/allows_anonymous_read/\
                A computer program does what you tell it to do, not what you want it to do.'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_unexpected_property_group() {
    let unexpected_group = "dataframe";
    let property = DatasetPropertyName::AllowsAnonymousRead;

    let input = format!("{unexpected_group}{SEP}{property}");

    assert_matches!(
        PropertyName::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Unexpected property group: 'dataframe'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_unexpected_property_name() {
    let group = PropertyName::Dataset(DatasetPropertyName::AllowsAnonymousRead).property_group();
    let unexpected_property = AccountPropertyName::IsAdmin;

    let input = format!("{group}{SEP}{unexpected_property}");

    assert_matches!(
        PropertyName::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Matching variant not found (context: group 'dataset', \
                property_name 'is_admin')"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_parse_property() {
    let inputs = [
        "dataset/allows_anonymous_read",
        "dataset/allows_public_read",
        "account/is_admin",
    ];

    for input in inputs {
        let property = match PropertyName::from_str(input) {
            Ok(property) => property,
            Err(e) => panic!("Expected parsing error: {}", e.reason()),
        };

        let property_as_str = property.to_string();

        assert_eq!(input, property_as_str);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
