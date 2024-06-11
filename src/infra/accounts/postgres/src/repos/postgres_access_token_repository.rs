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
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use opendatafabric::{AccountID, AccountName};
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
                access_token_id: *token_id,
            }))
        }
    }

    async fn get_access_tokens(
        &self,
        pagination: &AccessTokenPaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError> {
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
                LIMIT $1 OFFSET $2
                "#,
            pagination.limit,
            pagination.offset,
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

    async fn mark_revoked(
        &self,
        token_id: &Uuid,
        revoked_time: DateTime<Utc>,
    ) -> Result<(), GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let update_result = sqlx::query!(
            r#"
                UPDATE access_tokens SET revoked_at = $1 where id = $2
            "#,
            revoked_time,
            token_id,
        )
        .execute(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        if update_result.rows_affected() == 0 {
            return Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }));
        }

        Ok(())
    }

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
    ) -> Result<Account, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let maybe_account_row = sqlx::query!(
            r#"
                SELECT
                    a.id as "id: AccountID",
                    a.account_name,
                    a.email as "email?",
                    a.display_name,
                    a.account_type as "account_type: AccountType",
                    a.avatar_url,
                    a.registered_at,
                    a.is_admin,
                    a.provider,
                    a.provider_identity_key
                FROM access_tokens at
                LEFT JOIN accounts a ON a.id = account_id
                WHERE at.id = $1 AND revoked_at IS null
                "#,
            token_id
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        if let Some(account_row) = maybe_account_row {
            Ok(Account {
                id: account_row.id,
                account_name: AccountName::new_unchecked(&account_row.account_name),
                email: account_row.email,
                display_name: account_row.display_name,
                account_type: account_row.account_type,
                avatar_url: account_row.avatar_url,
                registered_at: account_row.registered_at,
                is_admin: account_row.is_admin,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
        } else {
            Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }))
        }
    }
}
