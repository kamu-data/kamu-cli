// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use nutype::nutype;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MIN_PASSWORD_LENGTH: usize = 8;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[nutype(
    sanitize(trim),
    validate(with = validate_password, error = PasswordValidationError),
    derive(Debug, PartialEq, Eq, Clone, Deref)
)]
pub struct Password(String);

impl std::fmt::Display for Password {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "********")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug, Clone)]
pub enum PasswordValidationError {
    TooShort,
    NonAscii,
}

impl std::fmt::Display for PasswordValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PasswordValidationError::TooShort => {
                write!(f, "Minimum password length is {MIN_PASSWORD_LENGTH}")
            }
            PasswordValidationError::NonAscii => {
                write!(f, "Password contains non-ASCII characters")
            }
        }
    }
}

fn validate_password(password: &str) -> Result<(), PasswordValidationError> {
    if password.len() < MIN_PASSWORD_LENGTH {
        return Err(PasswordValidationError::TooShort);
    }
    if !password.is_ascii() {
        return Err(PasswordValidationError::NonAscii);
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
