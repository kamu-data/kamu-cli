// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::AdminGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Admin;

#[Object]
impl Admin {
    #[allow(clippy::unused_async)]
    #[graphql(guard = "AdminGuard::new()")]
    async fn self_test(&self) -> Result<String> {
        Ok("OK".to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
