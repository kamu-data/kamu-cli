// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use validator::ValidateEmail;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Email(String);

impl Email {
    pub fn parse(s: &str) -> Result<Email, InvalidEmailError> {
        if ValidateEmail::validate_email(&s) {
            Ok(Self(s.to_string()))
        } else {
            Err(InvalidEmailError { s: s.to_string() })
        }
    }

    pub fn host(&self) -> &str {
        self.0
            .split_once('@')
            .map(|(_, host)| host)
            .expect("Host is unavailable")
    }
}

impl AsRef<str> for Email {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Email {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("{s} is not a valid email")]
pub struct InvalidEmailError {
    s: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
