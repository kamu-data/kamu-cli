// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_cli::testing::Kamu;

////////////////////////////////////////////////////////////////////////////////

pub async fn test_generate_token(kamu: Kamu) -> Result<(), InternalError> {
    kamu.execute(["system", "generate-token", "--login", "test"])
        .await
        .int_err()
}

////////////////////////////////////////////////////////////////////////////////
