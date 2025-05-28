// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use random_strings::{AllowedSymbols, get_random_string};

use crate::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ENV_VAR_KAMU_JWT_SECRET: &str = "KAMU_JWT_SECRET";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct JwtAuthenticationConfig {
    pub jwt_secret: String,
    pub maybe_dummy_token_account: Option<Account>,
}

impl JwtAuthenticationConfig {
    pub fn new(maybe_jwt_secret: Option<String>) -> Self {
        Self {
            jwt_secret: maybe_jwt_secret
                .unwrap_or_else(|| get_random_string(None, 64, &AllowedSymbols::Alphanumeric)),
            maybe_dummy_token_account: None,
        }
    }

    pub fn load_from_env() -> Self {
        Self::new(std::env::var(ENV_VAR_KAMU_JWT_SECRET).ok())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
