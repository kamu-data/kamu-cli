// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::PredefinedAccountsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(setty::Config, setty::Default)]
pub struct AuthConfig {
    #[config(default = true)]
    pub allow_anonymous: bool,

    #[config(default)]
    pub users: PredefinedAccountsConfig,
}

impl AuthConfig {
    pub fn sample() -> Self {
        Self {
            allow_anonymous: true,
            users: PredefinedAccountsConfig::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
