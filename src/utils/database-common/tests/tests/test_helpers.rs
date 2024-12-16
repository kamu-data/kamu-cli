// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::sqlite_generate_placeholders_list;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_sqlite_generate_placeholders() {
    for (arguments_count, index_offset, expected_result) in [
        (0, 0, ""),
        (0, 1, ""),
        (0, 42, ""),
        (3, 0, "$0,$1,$2"),
        (2, 3, "$3,$4"),
    ] {
        pretty_assertions::assert_eq!(
            expected_result,
            sqlite_generate_placeholders_list(arguments_count, index_offset)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
