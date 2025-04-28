// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::KamuApiServerClient;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_selftest(kamu_api_server_client: KamuApiServerClient) {
    kamu_api_server_client
        .graphql_api_call_assert(
            "{ apiVersion }",
            Ok(indoc::indoc!(
                r#"
                {
                  "apiVersion": "1"
                }
                "#,
            )),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
