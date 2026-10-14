// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::LazyLock;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use regex::Regex;

use super::WebhookVerificationError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Label of the signature that Kamu puts on every delivery
pub(super) const SIGNATURE_LABEL: &str = "sig1";

/// The `sig1` entry of the `Signature` header: `sig1=:<base64>:`
pub(super) struct Signature {
    bytes: Vec<u8>,
}

impl Signature {
    pub fn parse(header_value: &str) -> Result<Self, WebhookVerificationError> {
        static SIGNATURE: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(\w+)=:([^:]+):").unwrap());

        let captures = SIGNATURE
            .captures_iter(header_value)
            .find(|captures| &captures[1] == SIGNATURE_LABEL)
            .ok_or(WebhookVerificationError::Malformed("Signature"))?;
        let bytes = BASE64
            .decode(&captures[2])
            .map_err(|_| WebhookVerificationError::Malformed("Signature"))?;

        Ok(Self { bytes })
    }

    /// Checks the HMAC-SHA256 of the signature base, keyed with the secret's
    /// bytes
    pub fn verify(
        &self,
        secret: &str,
        signature_base: &str,
    ) -> Result<(), WebhookVerificationError> {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        ring::hmac::verify(&key, signature_base.as_bytes(), &self.bytes)
            .map_err(|_| WebhookVerificationError::SignatureMismatch)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
