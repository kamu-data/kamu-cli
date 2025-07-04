// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use crate::PredefinedAccountsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AuthConfig {
    pub allow_anonymous: Option<bool>,
    pub users: Option<PredefinedAccountsConfig>,
}

impl AuthConfig {
    pub fn sample() -> Self {
        Self {
            allow_anonymous: Some(true),
            users: Some(PredefinedAccountsConfig::sample()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            allow_anonymous: Some(false),
            users: Some(PredefinedAccountsConfig::default()),
        }
    }
}
