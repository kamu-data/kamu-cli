// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(setty::Config, setty::Default)]
pub struct DidSecretEncryptionConfig {
    #[config(default = false)]
    pub enabled: bool,

    /// The encryption key must be a 32-character alphanumeric string, which
    /// includes both uppercase and lowercase Latin letters (A-Z, a-z) and
    /// digits (0-9).
    ///
    /// To generate use:
    /// ```sh
    /// tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32; echo
    /// ```
    pub encryption_key: Option<String>,
}

impl DidSecretEncryptionConfig {
    pub fn sample() -> Self {
        Self {
            enabled: true,
            encryption_key: Some(SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY.to_owned()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        if self.enabled && self.encryption_key.is_some() {
            return true;
        }
        false
    }
}
