// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use random_names::get_random_name;

///////////////////////////////////////////////////////////////////////////////

const ENV_VAR_KAMU_JWT_SECRET: &str = "KAMU_JWT_SECRET";

///////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct JwtAuthenticationConfig {
    pub jwt_secret: String,
}

impl JwtAuthenticationConfig {
    pub fn new(jwt_secret: String) -> Self {
        Self { jwt_secret }
    }

    pub fn load_from_env() -> Self {
        Self {
            jwt_secret: std::env::var(ENV_VAR_KAMU_JWT_SECRET)
                .ok()
                .unwrap_or_else(|| get_random_name(None, 64)),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
