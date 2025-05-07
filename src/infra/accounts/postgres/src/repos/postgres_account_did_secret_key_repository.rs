// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_utils::DidSecretKey;
use database_common::{TransactionRef, TransactionRefT};
use internal_error::ResultIntoInternal;
use kamu_accounts::{
    AccountDidSecretKeyRepository,
    AccountDidSecretKeyRowModel,
    GetDidSecretKeysByAccountIdError,
    SaveAccountDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountDidSecretKeyRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[dill::component(pub)]
#[dill::interface(dyn AccountDidSecretKeyRepository)]
impl PostgresAccountDidSecretKeyRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountDidSecretKeyRepository for PostgresAccountDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        account_id: &odf::AccountID,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveAccountDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
                INSERT INTO account_did_secret_keys (account_id, secret_key, secret_nonce, creator_id)
                    VALUES ($1, $2, $3, $4)
                "#,
            account_id.to_string(),
            did_secret_key.secret_key,
            did_secret_key.secret_nonce,
            creator_id.to_string(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByAccountIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let did_secret_keys = sqlx::query_as!(
            AccountDidSecretKeyRowModel,
            r#"
                SELECT account_id as "account_id: _",
                       secret_key,
                       secret_nonce,
                       creator_id as "creator_id: _"
                FROM account_did_secret_keys
                WHERE creator_id = $1
                "#,
            creator_id.to_string(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(did_secret_keys.into_iter().map(Into::into).collect())
    }
}
