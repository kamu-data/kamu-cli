// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tonic::Status;

use crate::{SessionAuth, SessionToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Rejects all basic auth attempts, requiring that users auth via bearer tokens
#[dill::component]
#[dill::interface(dyn SessionAuth)]
pub struct SessionAuthBearerOnly {}

#[async_trait::async_trait]
impl SessionAuth for SessionAuthBearerOnly {
    async fn auth_basic(&self, _username: &str, _password: &str) -> Result<SessionToken, Status> {
        Err(Status::unauthenticated(
            "Basic auth and anonymous access are not supported by this server. Users must \
             authenticate via Bearer token auth mechanism.",
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
