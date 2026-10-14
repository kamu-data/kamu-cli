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
use sha2::Digest as _;

use super::WebhookVerificationError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// RFC 9530 `Content-Digest` header, `sha-256` only: `sha-256=:<base64>:`
pub(super) struct ContentDigest {
    sha256: Vec<u8>,
}

impl ContentDigest {
    pub fn parse(header_value: &str) -> Result<Self, WebhookVerificationError> {
        static CONTENT_DIGEST: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^([A-Za-z0-9_-]+)=:([^:]+):$").unwrap());

        let captures = CONTENT_DIGEST
            .captures(header_value)
            .ok_or(WebhookVerificationError::Malformed("Content-Digest"))?;
        if !captures[1].eq_ignore_ascii_case("sha-256") {
            return Err(WebhookVerificationError::UnsupportedAlgorithm(
                captures[1].to_string(),
            ));
        }
        let sha256 = BASE64
            .decode(&captures[2])
            .map_err(|_| WebhookVerificationError::Malformed("Content-Digest"))?;

        Ok(Self { sha256 })
    }

    pub fn verify(&self, body: &[u8]) -> Result<(), WebhookVerificationError> {
        if sha2::Sha256::digest(body).as_slice() == self.sha256.as_slice() {
            Ok(())
        } else {
            Err(WebhookVerificationError::DigestMismatch)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
