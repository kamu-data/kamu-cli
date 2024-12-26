// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::Rng;
use tonic::Status;

use crate::{SessionAuth, SessionToken};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ANON_SESSION_TOKEN_BYTES_LENGTH: usize = 16;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Enables basic auth for `anonymous` account with no password, generating them
/// a special bearer token that is used to tell multiple anonymous clients
/// apart for session identification and individual rate-limiting.
#[dill::component]
#[dill::interface(dyn SessionAuth)]
pub struct SessionAuthAnonymous {}

#[async_trait::async_trait]
impl SessionAuth for SessionAuthAnonymous {
    async fn auth_basic(&self, username: &str, password: &str) -> Result<SessionToken, Status> {
        if username != "anonymous" || !password.is_empty() {
            return Err(Status::unauthenticated(
                "Basic auth is only supported for 'anonymous' accounts with no password. \
                 Authenticated users should use Bearer token auth mechanism.",
            ));
        }

        let mut random_token_bytes = [0_u8; ANON_SESSION_TOKEN_BYTES_LENGTH];
        rand::thread_rng().fill(&mut random_token_bytes);
        let base32_token = base32::encode(base32::Alphabet::Crockford, &random_token_bytes);

        // TODO: SEC: Anonymous tokens should be validated on subsequent requests,
        // otherwise malicious clients can just generate them. This will require
        // storing tokens in a cache (e.g. alongside rate limiting data).
        let session_token = SessionToken(format!("anon_{}", &base32_token));
        Ok(session_token)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
