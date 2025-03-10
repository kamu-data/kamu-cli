// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Utility to generate the placeholder list. Helpful when using dynamic SQL
/// generation.
///
/// # Examples
/// ```
/// // Output for `arguments_count`=3 & `index_offset`=0
/// "$0,$1,$2"
///
/// // Output for `arguments_count`=2 & `index_offset`=3
/// "$3,$4"
/// ```
pub fn sqlite_generate_placeholders_list(arguments_count: usize, index_offset: usize) -> String {
    (0..arguments_count)
        .map(|i| format!("${}", i + index_offset))
        .intersperse(",".to_string())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sqlite_generate_placeholders_list() {
    pretty_assertions::assert_eq!("$0,$1,$2", sqlite_generate_placeholders_list(3, 0));
    pretty_assertions::assert_eq!("$3,$4", sqlite_generate_placeholders_list(2, 3));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
