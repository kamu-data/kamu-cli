// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{EvmWalletAddress, Web3AuthenticationEip4361Nonce};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq)]
pub struct Web3AuthEip4361NonceEntity {
    pub wallet_address: EvmWalletAddress,
    pub nonce: Web3AuthenticationEip4361Nonce,
    pub expires_at: DateTime<Utc>,
}

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Web3AuthEip4361NonceEntityRowModel {
    pub wallet_address: String,
    pub nonce: String,
    pub expires_at: DateTime<Utc>,
}

#[cfg(feature = "sqlx")]
impl TryFrom<Web3AuthEip4361NonceEntityRowModel> for Web3AuthEip4361NonceEntity {
    type Error = internal_error::InternalError;

    fn try_from(v: Web3AuthEip4361NonceEntityRowModel) -> Result<Self, Self::Error> {
        use internal_error::ResultIntoInternal;

        Ok(Self {
            wallet_address: crate::EvmWalletAddressConvertor::parse_checksummed(&v.wallet_address)
                .int_err()?,
            nonce: v.nonce.try_into().int_err()?,
            expires_at: v.expires_at,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
