// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use random_names::get_random_name;

///////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID: &str = "KAMU_AUTH_GITHUB_CLIENT_ID";
pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET: &str = "KAMU_AUTH_GITHUB_CLIENT_SECRET";
pub const ENV_VAR_KAMU_JWT_SECRET: &str = "KAMU_JWT_SECRET";

///////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationConfig {
    jwt_secret: Option<String>,
    pub github_client_id: Option<String>,
    pub github_client_secret: Option<String>,
}

impl AuthenticationConfig {
    pub fn load_from_env() -> Self {
        Self {
            jwt_secret: std::env::var(ENV_VAR_KAMU_JWT_SECRET).ok(),
            github_client_id: std::env::var(ENV_VAR_KAMU_JWT_SECRET).ok(),
            github_client_secret: std::env::var(ENV_VAR_KAMU_JWT_SECRET).ok(),
        }
    }

    pub fn jwt_secret(&self) -> Cow<String> {
        if let Some(value) = &self.jwt_secret {
            Cow::Borrowed(value)
        } else {
            let random_jwt_secret = get_random_name(None, 64);

            Cow::Owned(random_jwt_secret)
        }
    }
}
