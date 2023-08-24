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

pub const LOGIN_METHOD_GITHUB: &str = "oauth_github";
pub const LOGIN_METHOD_PASSWORD: &str = "password";

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubLoginCredentials {
    pub code: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordLoginCredentials {
    pub login: String,
    pub password: String,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountInfo {
    pub login: AccountName,
    pub name: String,
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
