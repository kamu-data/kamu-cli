// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::*;
use kamu_accounts::{
    AccessToken,
    AccessTokenRepository,
    AccessTokenService,
    Account,
    CreateAccessTokenError,
    EncodeTokenError,
    GetAccessTokenError,
    GetAccountInfoError,
    KamuAccessToken,
};
use opendatafabric::{AccountID, Multihash};
use rand::{self, Rng};
use uuid::Uuid;

///////////////////////////////////////////////////////////////////////////////

pub const ACCESS_TOKEN_PREFIX: &str = "ka";
pub const ACCESS_TOKEN_BYTES_LENGTH: usize = 16;
pub const ENCODED_ACCESS_TOKEN_LENGTH: usize = 61;

///////////////////////////////////////////////////////////////////////////////

pub struct AccessTokenServiceImpl {
    access_token_repository: Arc<dyn AccessTokenRepository>,
}

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AccessTokenService)]
impl AccessTokenServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(access_token_repository: Arc<dyn AccessTokenRepository>) -> Self {
        Self {
            access_token_repository,
        }
    }

    pub fn generate_access_token_impl(&self) -> KamuAccessToken {
        let mut random_token_bytes = [0_u8; ACCESS_TOKEN_BYTES_LENGTH];
        rand::thread_rng().fill(&mut random_token_bytes);

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
            random_bytes_hash: Multihash::from_digest_sha3_256(&random_token_bytes),
            checksum: token_checksum,
            composed_token: format!("{ACCESS_TOKEN_PREFIX}_{}", &base32_token),
            base32_token,
        }
    }

    pub fn encode_token_impl(
        &self,
        access_token: String,
    ) -> Result<KamuAccessToken, EncodeTokenError> {
        if access_token.len() != ENCODED_ACCESS_TOKEN_LENGTH {
            return Err(EncodeTokenError::InvalidTokenLength);
        }
        if let Some((token_prefix, encoded_token)) = access_token.split_once('_') {
            if token_prefix != ACCESS_TOKEN_PREFIX {
                return Err(EncodeTokenError::InvalidTokenPrefix);
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
                    return Err(EncodeTokenError::InvalidTokenChecksum);
                }

                return Ok(KamuAccessToken {
                    id: token_id,
                    prefix: token_prefix.to_string(),
                    random_bytes: token_body,
                    random_bytes_hash: Multihash::from_digest_sha3_256(&token_body),
                    checksum: calculated_checksum,
                    base32_token: encoded_token.to_string(),
                    composed_token: access_token,
                });
            }
        }
        Err(EncodeTokenError::InvalidTokenFormat)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccessTokenService for AccessTokenServiceImpl {
    async fn create_access_token(
        &self,
        token_name: &str,
        account_id: &AccountID,
    ) -> Result<KamuAccessToken, CreateAccessTokenError> {
        let kamu_access_token = self.generate_access_token_impl();

        self.access_token_repository
            .create_access_token(&AccessToken {
                id: kamu_access_token.id,
                token_name: token_name.to_string(),
                token_hash: kamu_access_token
                    .random_bytes_hash
                    .digest()
                    .try_into()
                    .unwrap(),
                created_at: Utc::now(),
                revoked_at: None,
                account_id: account_id.clone(),
            })
            .await?;

        Ok(kamu_access_token)
    }

    async fn find_account_by_access_token(
        &self,
        _access_token: String,
    ) -> Result<Account, GetAccountInfoError> {
        unimplemented!()
    }

    fn encode_access_token(
        &self,
        access_token_str: &str,
    ) -> Result<KamuAccessToken, EncodeTokenError> {
        self.encode_token_impl(access_token_str.to_string())
    }

    fn generate_access_token(&self) -> KamuAccessToken {
        self.generate_access_token_impl()
    }

    async fn get_access_tokens(&self) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        unimplemented!()
    }
}
