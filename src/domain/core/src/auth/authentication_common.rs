// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::BoxedError;
use opendatafabric::AccountName;
use serde::{Deserialize, Serialize};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

// TODO: have some length restrictions (0 < .. < limit)
pub type AccountDisplayName = String;

pub const DEFAULT_ACCOUNT_NAME: &str = "kamu";
pub const DEFAULT_AVATAR_URL: &str = "https://avatars.githubusercontent.com/u/50896974?s=200&v=4";

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccountInfo {
    pub account_name: AccountName,
    pub display_name: AccountDisplayName,
    pub avatar_url: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Invalid credentials")]
pub struct InvalidCredentialsError {
    #[source]
    source: BoxedError,
}

impl InvalidCredentialsError {
    pub fn new(source: BoxedError) -> Self {
        Self { source }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Rejected credentials: {reason}")]
pub struct RejectedCredentialsError {
    reason: String,
}

impl RejectedCredentialsError {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

///////////////////////////////////////////////////////////////////////////////
