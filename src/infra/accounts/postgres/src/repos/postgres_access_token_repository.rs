// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use opendatafabric::AccountID;
use uuid::Uuid;

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccessTokenRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn AccessTokenRepository)]
impl PostgresAccessTokenRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccessTokenRepository for PostgresAccessTokenRepository {
    async fn create_access_token(
        &self,
        access_token: &AccessToken,
    ) -> Result<(), CreateAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(CreateAccessTokenError::Internal)?;

        sqlx::query!(
            r#"
                INSERT INTO access_tokens (id, token_name, token_hash, created_at, account_id)
                    VALUES ($1, $2, $3, $4, $5)
                "#,
            access_token.id,
            access_token.token_name,
            &access_token.token_hash,
            access_token.created_at,
            access_token.account_id.to_string(),
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    CreateAccessTokenError::Duplicate(CreateAccessTokenErrorDuplicate {
                        access_token_name: access_token.token_name.clone(),
                    })
                } else {
                    CreateAccessTokenError::Internal(e.int_err())
                }
            }
            _ => CreateAccessTokenError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let maybe_access_token_row = sqlx::query!(
            r#"
                SELECT
                    id,
                    token_name,
                    token_hash,
                    created_at,
                    revoked_at,
                    account_id as "account_id: AccountID"
                FROM access_tokens
                WHERE id = $1
                "#,
            token_id
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        if let Some(access_token_row) = maybe_access_token_row {
            println!("row_hash: {:?}", access_token_row.token_hash);
            Ok(AccessToken {
                id: access_token_row.id,
                token_name: access_token_row.token_name,
                token_hash: access_token_row.token_hash.try_into().unwrap(),
                created_at: access_token_row.created_at,
                revoked_at: access_token_row.revoked_at,
                account_id: access_token_row.account_id,
            })
        } else {
            Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: token_id.clone(),
            }))
        }
    }

    async fn get_access_tokens(&self) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let access_token_rows = sqlx::query!(
            r#"
                SELECT
                    id,
                    token_name,
                    token_hash,
                    created_at,
                    revoked_at,
                    account_id as "account_id: AccountID"
                FROM access_tokens
                "#,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        Ok(access_token_rows
            .into_iter()
            .map(|access_token_row| AccessToken {
                id: access_token_row.id,
                token_name: access_token_row.token_name,
                token_hash: access_token_row.token_hash.try_into().unwrap(),
                created_at: access_token_row.created_at,
                revoked_at: access_token_row.revoked_at,
                account_id: access_token_row.account_id,
            })
            .collect())
    }
}
