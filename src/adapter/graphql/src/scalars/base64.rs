// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Base64-encoded binary data (url-safe, no padding)
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Base64Usnp(#[serde(with = "base64_urlsafe_nopad")] pub bytes::Bytes);

async_graphql::scalar!(
    Base64Usnp,
    "Base64Usnp",
    "Base64-encoded binary data (url-safe, no padding)"
);

impl Base64Usnp {
    #[cfg_attr(not(any(feature = "testing")), expect(dead_code))]
    pub fn into_inner(self) -> bytes::Bytes {
        self.0
    }
}

impl From<bytes::Bytes> for Base64Usnp {
    fn from(value: bytes::Bytes) -> Self {
        Self(value)
    }
}

impl AsRef<bytes::Bytes> for Base64Usnp {
    fn as_ref(&self) -> &bytes::Bytes {
        &self.0
    }
}

impl std::ops::Deref for Base64Usnp {
    type Target = bytes::Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub mod base64_urlsafe_nopad {
    use ::base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &bytes::Bytes, serializer: S) -> Result<S::Ok, S::Error> {
        let s = ::base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<bytes::Bytes, D::Error> {
        let s = String::deserialize(deserializer)?;
        let buf = ::base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;
        Ok(buf.into())
    }
}
