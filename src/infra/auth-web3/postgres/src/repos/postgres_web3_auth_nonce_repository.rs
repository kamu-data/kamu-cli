// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{TransactionRef, TransactionRefT};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresWeb3AuthNonceRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[dill::component(pub)]
#[dill::interface(dyn Web3AuthNonceRepository)]
impl PostgresWeb3AuthNonceRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthNonceRepository for PostgresWeb3AuthNonceRepository {
    async fn set_nonce(&self, entity: &Web3AuthenticationNonceEntity) -> Result<(), SetNonceError> {
        todo!()
    }

    async fn get_nonce(
        &self,
        wallet: &EvmWalletAddress,
    ) -> Result<Web3AuthenticationNonceEntity, GetNonceError> {
        todo!()
    }

    async fn cleanup_expired_nonces(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredNoncesError> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
