// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY: &str = "QfnEDcnUtGSW2pwVXaFPvZOwxyFm2BOC";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DidSecretEncryptionConfig {
    /// The encryption key must be a 32-character alphanumeric string, which
    /// includes both uppercase and lowercase Latin letters (A-Z, a-z) and
    /// digits (0-9).
    ///
    /// # Example
    /// let config = DidSecretEncryptionConfig {
    ///     encryption_key: String::from("aBcDeFgHiJkLmNoPqRsTuVwXyZ012345")
    /// };
    /// ```
    pub encryption_key: Option<String>,
    pub enabled: Option<bool>,
}

impl DidSecretEncryptionConfig {
    pub fn sample() -> Self {
        Self {
            encryption_key: Some(SAMPLE_DID_SECRET_KEY_ENCRYPTION_KEY.to_owned()),
            enabled: Some(true),
        }
    }

    pub fn is_enabled(&self) -> bool {
        if let Some(enabled) = self.enabled
            && enabled
            && self.encryption_key.is_some()
        {
            return true;
        }
        false
    }
}
