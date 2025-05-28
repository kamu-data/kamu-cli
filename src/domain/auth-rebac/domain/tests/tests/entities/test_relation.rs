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

use kamu_auth_rebac::{AccountToDatasetRelation, RELATION_GROUP_SEPARATOR, Relation};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SEP: &str = RELATION_GROUP_SEPARATOR;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_invalid_format() {
    let group = Relation::account_is_a_dataset_editor().relation_group();
    let relation = AccountToDatasetRelation::Editor;
    let unexpected_but_true_appendix = "Premature optimization is the root of all evil.";

    let input = format!("{group}{SEP}{relation}{SEP}{unexpected_but_true_appendix}");

    assert_matches!(
        Relation::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Invalid format for value: 'account->dataset/editor/\
                Premature optimization is the root of all evil.'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_unexpected_relation_group() {
    let unexpected_group = "dataframe";
    let relation = AccountToDatasetRelation::Editor;

    let input = format!("{unexpected_group}{SEP}{relation}");

    assert_matches!(
        Relation::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Unexpected relation group: 'dataframe'"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_try_parse_unexpected_relation_name() {
    let group = Relation::account_is_a_dataset_editor().relation_group();
    let unexpected_relation = "uncle";

    let input = format!("{group}{SEP}{unexpected_relation}");

    assert_matches!(
        Relation::from_str(&input),
        Err(e)
            if e.reason() == "Internal error: Matching variant not found \
                (context: group 'account->dataset', relation_name 'uncle')"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_parse_relation() {
    let inputs = ["account->dataset/editor", "account->dataset/reader"];

    for input in inputs {
        let relation = match Relation::from_str(input) {
            Ok(relation) => relation,
            Err(e) => panic!("Expected parsing error: {}", e.reason()),
        };

        let relation_as_str = relation.to_string();

        assert_eq!(input, relation_as_str);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
