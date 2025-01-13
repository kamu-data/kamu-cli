// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::Status;

use crate::SessionToken;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Simplified trait that some session managers can delegate authentication to
#[async_trait::async_trait]
pub trait SessionAuth: Send + Sync {
    async fn auth_basic(&self, username: &str, password: &str) -> Result<SessionToken, Status>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
