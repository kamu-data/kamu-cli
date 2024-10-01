// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_gql_query_api_version(kamu: KamuCliPuppet) {
    let assert = kamu
        .execute([
            "system",
            "api-server",
            "gql-query",
            "{apiVersion}".escape_default().to_string().as_str(),
        ])
        .await
        .success();
    let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

    assert_eq!(
        stdout,
        indoc::indoc!(
            r#"
            {
              "apiVersion": "0.1"
            }
            "#
        )
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
