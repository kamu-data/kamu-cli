// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_generate_token(kamu: KamuCliPuppet) {
    kamu.execute(["system", "generate-token", "--login", "test"])
        .await
        .success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
