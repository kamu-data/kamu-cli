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
use internal_error::ResultIntoInternal;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresWeb3AuthNonceRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[dill::component(pub)]
#[dill::interface(dyn Web3AuthEip4361NonceRepository)]
impl PostgresWeb3AuthNonceRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthEip4361NonceRepository for PostgresWeb3AuthNonceRepository {
    async fn set_nonce(&self, entity: &Web3AuthEip4361NonceEntity) -> Result<(), SetNonceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO web3_auth_eip4361_nonces(wallet_address, nonce, expires_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (wallet_address) DO UPDATE
                SET nonce      = excluded.nonce,
                    expires_at = excluded.expires_at
            "#,
            EvmWalletAddressConvertor::checksummed_string(&entity.wallet_address),
            entity.nonce.as_ref(),
            entity.expires_at
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_nonce(
        &self,
        wallet: &EvmWalletAddress,
    ) -> Result<Web3AuthEip4361NonceEntity, GetNonceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_nonce_row = sqlx::query_as!(
            Web3AuthEip4361NonceEntityRowModel,
            r#"
            SELECT wallet_address,
                   nonce,
                   expires_at
            FROM web3_auth_eip4361_nonces
            WHERE wallet_address = $1
            "#,
            EvmWalletAddressConvertor::checksummed_string(wallet)
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(nonce_row_row_model) = maybe_nonce_row {
            let nonce_row = nonce_row_row_model.try_into()?;

            Ok(nonce_row)
        } else {
            Err(GetNonceError::NotFound(WalletNotFoundError {
                wallet: *wallet,
            }))
        }
    }

    async fn consume_nonce(
        &self,
        wallet: &EvmWalletAddress,
        now: DateTime<Utc>,
    ) -> Result<(), ConsumeNonceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM web3_auth_eip4361_nonces
            WHERE wallet_address = $1
              AND expires_at > $2;
            "#,
            EvmWalletAddressConvertor::checksummed_string(wallet),
            now
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(ConsumeNonceError::NotFound(WalletNotFoundError {
                wallet: *wallet,
            }))
        }
    }

    async fn cleanup_expired_nonces(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredNoncesError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            DELETE
            FROM web3_auth_eip4361_nonces
            WHERE expires_at <= $1
            "#,
            now
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
