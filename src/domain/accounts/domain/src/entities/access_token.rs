// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use chrono::{DateTime, Utc};
use jsonwebtoken::TokenData;
use rand::{self, Rng};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

use super::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ACCESS_TOKEN_PREFIX: &str = "ka";
pub const ACCESS_TOKEN_BYTES_LENGTH: usize = 16;
pub const ENCODED_ACCESS_TOKEN_LENGTH: usize = 61;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct KamuAccessToken {
    pub prefix: String,
    pub id: Uuid,
    pub random_bytes: [u8; 16],
    pub random_bytes_hash: [u8; 32],
    pub checksum: u32,
    pub base32_token: String,
    pub composed_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl KamuAccessToken {
    pub fn new() -> Self {
        let mut random_token_bytes = [0_u8; ACCESS_TOKEN_BYTES_LENGTH];
        rand::rng().fill(&mut random_token_bytes);

        let token_checksum = crc32fast::hash(&random_token_bytes);
        let token_id = Uuid::new_v4();
        let full_token_bytes = [
            token_id.as_bytes(),
            random_token_bytes.as_slice(),
            token_checksum.to_be_bytes().as_slice(),
        ]
        .concat();
        let base32_token = base32::encode(base32::Alphabet::Crockford, &full_token_bytes);

        KamuAccessToken {
            prefix: ACCESS_TOKEN_PREFIX.to_string(),
            id: token_id,
            random_bytes: random_token_bytes,
            random_bytes_hash: odf::Multihash::from_digest_sha3_256(&random_token_bytes)
                .digest()
                .try_into()
                .unwrap(),
            checksum: token_checksum,
            composed_token: format!("{ACCESS_TOKEN_PREFIX}_{}", &base32_token),
            base32_token,
        }
    }

    pub fn decode(access_token_str: &str) -> Result<Self, DecodeTokenError> {
        if access_token_str.len() != ENCODED_ACCESS_TOKEN_LENGTH {
            return Err(DecodeTokenError::InvalidTokenLength);
        }
        if let Some((token_prefix, encoded_token)) = access_token_str.split_once('_') {
            if token_prefix != ACCESS_TOKEN_PREFIX {
                return Err(DecodeTokenError::InvalidTokenPrefix);
            }

            if let Some(decoded_token_bytes) =
                base32::decode(base32::Alphabet::Crockford, encoded_token)
            {
                let token_id = Uuid::from_bytes(
                    decoded_token_bytes[0..16]
                        .try_into()
                        .expect("Invalid uuid length"),
                );
                let token_body: [u8; 16] = decoded_token_bytes[16..32]
                    .try_into()
                    .expect("Invalid token_bytes_length");
                let token_checksum: [u8; 4] = decoded_token_bytes[32..36]
                    .try_into()
                    .expect("Invalid checksum length");
                let calculated_checksum = crc32fast::hash(&token_body);
                if calculated_checksum.to_be_bytes() != token_checksum {
                    return Err(DecodeTokenError::InvalidTokenChecksum);
                }

                return Ok(Self {
                    id: token_id,
                    prefix: token_prefix.to_string(),
                    random_bytes: token_body,
                    random_bytes_hash: odf::Multihash::from_digest_sha3_256(&token_body)
                        .digest()
                        .try_into()
                        .unwrap(),
                    checksum: calculated_checksum,
                    base32_token: encoded_token.to_string(),
                    composed_token: access_token_str.to_string(),
                });
            }
        }
        Err(DecodeTokenError::InvalidTokenFormat)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AccessToken {
    pub id: Uuid,
    pub token_name: String,
    pub token_hash: [u8; 32],
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Our claims struct, it needs to derive `Serialize` and/or `Deserialize`
#[derive(Debug, Serialize, Deserialize)]
pub struct JWTClaims {
    pub exp: usize,
    pub iat: usize,
    pub iss: String,
    pub sub: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum AccessTokenType {
    KamuAccessToken(KamuAccessToken),
    JWTToken(TokenData<JWTClaims>),
    DummyToken(Account),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug, Eq, PartialEq)]
pub enum DecodeTokenError {
    #[error("Invalid access token prefix")]
    InvalidTokenPrefix,
    #[error("Invalid access token length")]
    InvalidTokenLength,
    #[error("Invalid access token checksum")]
    InvalidTokenChecksum,
    #[error("Invalid access token format")]
    InvalidTokenFormat,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct AccessTokenRowModel {
    pub id: Uuid,
    pub token_name: String,
    pub token_hash: Vec<u8>,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub account_id: odf::AccountID,
}

#[cfg(feature = "sqlx")]
impl From<AccessTokenRowModel> for AccessToken {
    fn from(value: AccessTokenRowModel) -> Self {
        AccessToken {
            id: value.id,
            token_name: value.token_name,
            token_hash: value.token_hash.try_into().unwrap(),
            created_at: value.created_at,
            revoked_at: value.revoked_at,
            account_id: value.account_id,
        }
    }
}
