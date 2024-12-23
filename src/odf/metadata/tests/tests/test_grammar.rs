// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric_metadata::identity::Grammar;

#[test]
fn test_valid_patterns() {
    // Valid dataset pattern with wildcard at the end
    let param = "net.example.%";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));

    // Valid dataset pattern with wildcard at the middle
    let param = "net.e%.com";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "net-%.com";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "net%net";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    /////////////////////////////////////////////////////

    // Valiate dataset patterm with wildcard at the begining
    let param = "%.example.com";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "%e";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "%-e";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    ////////////////////////////////////////////////////////

    // Validate dataset pattern with multiple wildcards
    let param = "net%.ex%.com";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "%.ex%.com";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "%.ex%.c%";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));
    let param = "%-net.ex-%.c-%";
    let res = Grammar::match_dataset_name_pattern(param);
    assert_eq!(res, Some((param, "")));

    // Glob wildcard
    let valid_param = "%";
    let res = Grammar::match_dataset_name_pattern(valid_param);
    assert_eq!(res, Some((valid_param, "")));
    let valid_param = "%a%%";
    let res = Grammar::match_dataset_name_pattern(valid_param);
    assert_eq!(res, Some((valid_param, "")));
}

#[test]
fn test_invalid_patterns() {
    // Invalid pattern "net.example.%-" should fail and return valid part
    let valid_part = "net.example.%";
    let invalid_part = "-";
    let invalid_param = format!("{valid_part}{invalid_part}");
    let res = Grammar::match_dataset_name_pattern(&invalid_param);
    assert_eq!(res, Some((valid_part, invalid_part)));

    // Invalid pattern "net.exa^mple.%" should fail and return valid part
    let valid_part = "net.exa";
    let invalid_part = "^mple.%";
    let invalid_param = format!("{valid_part}{invalid_part}");
    let res = Grammar::match_dataset_name_pattern(&invalid_param);
    assert_eq!(res, Some((valid_part, invalid_part)));

    // Invalid patter "&net%.exa%mple.net" should fail and reutnr None
    let valid_part = "";
    let invalid_part = "&net%.exa%mple.net";
    let invalid_param = format!("{valid_part}{invalid_part}");
    let res = Grammar::match_dataset_name_pattern(&invalid_param);
    assert_eq!(res, None);

    // Pattern "did:odf:
    // fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65%""
    // should fail
    let valid_part = "did";
    let invalid_part = ":odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65%";
    let invalid_param = format!("{valid_part}{invalid_part}");
    let res = Grammar::match_dataset_name_pattern(&invalid_param);
    assert_eq!(res, Some((valid_part, invalid_part)));
}
