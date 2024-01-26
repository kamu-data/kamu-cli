// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::identity::Grammar;

#[test]
fn test_valid_patterns() {
    // Valid dataset pattern with wildcard at the end
    let param1_1 = "net.example.%";
    let res1_1 = Grammar::match_dataset_name_pattern(param1_1);
    assert_eq!(res1_1, Some((param1_1, "")));

    // Valid dataset pattern with wildcard at the middle
    let param2_1 = "net.e%.com";
    let res2_1 = Grammar::match_dataset_name_pattern(param2_1);
    assert_eq!(res2_1, Some((param2_1, "")));
    let param2_2 = "net-%.com";
    let res2_2 = Grammar::match_dataset_name_pattern(param2_2);
    assert_eq!(res2_2, Some((param2_2, "")));
    let param2_3 = "net%net";
    let res2_3 = Grammar::match_dataset_name_pattern(param2_3);
    assert_eq!(res2_3, Some((param2_3, "")));
    /////////////////////////////////////////////////////

    // Valiate dataset patterm with wildcard at the begining
    let param3_1 = "%.example.com";
    let res3_1 = Grammar::match_dataset_name_pattern(param3_1);
    assert_eq!(res3_1, Some((param3_1, "")));
    let param3_2 = "%e";
    let res3_2 = Grammar::match_dataset_name_pattern(param3_2);
    assert_eq!(res3_2, Some((param3_2, "")));
    let param3_3 = "%-e";
    let res3_3 = Grammar::match_dataset_name_pattern(param3_3);
    assert_eq!(res3_3, Some((param3_3, "")));
    ////////////////////////////////////////////////////////

    // Validate dataset pattern with multiple wildcards
    let param4_1 = "net%.ex%.com";
    let res4_1 = Grammar::match_dataset_name_pattern(param4_1);
    assert_eq!(res4_1, Some((param4_1, "")));
    let param4_2 = "%.ex%.com";
    let res4_2 = Grammar::match_dataset_name_pattern(param4_2);
    assert_eq!(res4_2, Some((param4_2, "")));
    let param4_3 = "%.ex%.c%";
    let res4_3 = Grammar::match_dataset_name_pattern(param4_3);
    assert_eq!(res4_3, Some((param4_3, "")));
    let param4_4 = "%-net.ex-%.c-%";
    let res4_4 = Grammar::match_dataset_name_pattern(param4_4);
    assert_eq!(res4_4, Some((param4_4, "")));

    // Glob wildcard
    let valid_param5_1 = "%";
    let res5_1 = Grammar::match_dataset_name_pattern(valid_param5_1);
    assert_eq!(res5_1, Some((valid_param5_1, "")));
    let valid_param5_2 = "%a%%";
    let res5_2 = Grammar::match_dataset_name_pattern(valid_param5_2);
    assert_eq!(res5_2, Some((valid_param5_2, "")));
}

#[test]
fn test_invalid_patterns() {
    // Invalid pattern "net.example.%-" should fail and return valid part
    let valid_part1_1 = "net.example.%";
    let invalid_part1_1 = "-";
    let invalid_param1_1 = format!("{}{}", valid_part1_1, invalid_part1_1);
    let res1_1 = Grammar::match_dataset_name_pattern(&invalid_param1_1);
    assert_eq!(res1_1, Some((valid_part1_1, invalid_part1_1)));

    // Invalid pattern "net.exa^mple.%" should fail and return valid part
    let valid_part1_2 = "net.exa";
    let invalid_part1_2 = "^mple.%";
    let invalid_param1_2 = format!("{}{}", valid_part1_2, invalid_part1_2);
    let res1_2 = Grammar::match_dataset_name_pattern(&invalid_param1_2);
    assert_eq!(res1_2, Some((valid_part1_2, invalid_part1_2)));

    // Invalid patter "&net%.exa%mple.net" should fail and reutnr None
    let valid_part1_3 = "";
    let invalid_part1_3 = "&net%.exa%mple.net";
    let invalid_param1_3 = format!("{}{}", valid_part1_3, invalid_part1_3);
    let res1_3 = Grammar::match_dataset_name_pattern(&invalid_param1_3);
    assert_eq!(res1_3, None);

    // Pattern "did:odf:
    // fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65%""
    // should fail
    let valid_part1_4 = "did";
    let invalid_part1_4 =
        ":odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65%";
    let invalid_param1_4 = format!("{}{}", valid_part1_4, invalid_part1_4);
    let res1_4 = Grammar::match_dataset_name_pattern(&invalid_param1_4);
    assert_eq!(res1_4, Some((valid_part1_4, invalid_part1_4)));
}
