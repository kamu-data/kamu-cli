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

pub struct SqliteWeb3AuthEip4361NonceRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[dill::component(pub)]
#[dill::interface(dyn Web3AuthEip4361NonceRepository)]
impl SqliteWeb3AuthEip4361NonceRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthEip4361NonceRepository for SqliteWeb3AuthEip4361NonceRepository {
    async fn set_nonce(&self, entity: &Web3AuthEip4361NonceEntity) -> Result<(), SetNonceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let wallet_address = EvmWalletAddressConvertor::checksummed_string(&entity.wallet_address);
        let nonce = entity.nonce.as_ref();

        sqlx::query!(
            r#"
            INSERT INTO web3_auth_eip4361_nonces(wallet_address, nonce, expires_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (wallet_address) DO UPDATE
                SET nonce      = excluded.nonce,
                    expires_at = excluded.expires_at
            "#,
            wallet_address,
            nonce,
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

        let wallet_address = EvmWalletAddressConvertor::checksummed_string(wallet);

        let maybe_nonce_row = sqlx::query_as!(
            Web3AuthEip4361NonceEntityRowModel,
            r#"
            SELECT wallet_address,
                   nonce,
                   expires_at AS "expires_at: _"
            FROM web3_auth_eip4361_nonces
            WHERE wallet_address = $1
            "#,
            wallet_address
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(nonce_row_row_model) = maybe_nonce_row {
            let nonce_row = nonce_row_row_model.try_into()?;

            Ok(nonce_row)
        } else {
            Err(GetNonceError::NotFound(NonceNotFoundError {
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

        let wallet_address = EvmWalletAddressConvertor::checksummed_string(wallet);

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM web3_auth_eip4361_nonces
            WHERE wallet_address = $1
              AND DATETIME(expires_at) > DATETIME($2);
            "#,
            wallet_address,
            now
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(ConsumeNonceError::NotFound(NonceNotFoundError {
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
            WHERE datetime(expires_at) <= datetime($1)
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
