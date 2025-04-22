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
pub struct Base64Usnp(#[serde(with = "base64_urlsafe_nopad")] pub Vec<u8>);

async_graphql::scalar!(
    Base64Usnp,
    "Base64Usnp",
    "Base64-encoded binary data (url-safe, no padding)"
);

impl Base64Usnp {
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl From<Vec<u8>> for Base64Usnp {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl AsRef<Vec<u8>> for Base64Usnp {
    fn as_ref(&self) -> &Vec<u8> {
        &self.0
    }
}

impl std::ops::Deref for Base64Usnp {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub mod base64_urlsafe_nopad {
    use ::base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error> {
        let s = ::base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(data);
        serializer.serialize_str(&s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        ::base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}
