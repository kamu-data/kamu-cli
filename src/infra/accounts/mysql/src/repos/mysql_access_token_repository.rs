// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{DatabasePaginationOpts, TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use opendatafabric::AccountID;
use sqlx::MySqlConnection;
use uuid::Uuid;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MysqlAccessTokenRepository {
    transaction: TransactionRefT<sqlx::MySql>,
}

#[component(pub)]
#[interface(dyn AccessTokenRepository)]
impl MysqlAccessTokenRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    async fn query_token_by_id(
        &self,
        token_id: &Uuid,
        connection: &mut MySqlConnection,
    ) -> Result<Option<AccessToken>, InternalError> {
        let access_token_row_maybe = sqlx::query_as!(
            AccessTokenRowModel,
            r#"
                SELECT
                    id as "id: sqlx::types::uuid::fmt::Simple",
                    token_name,
                    token_hash as "token_hash: _",
                    created_at,
                    revoked_at,
                    account_id as "account_id: _"
                FROM access_tokens
                WHERE id = ?
            "#,
            token_id.to_string()
        )
        .fetch_optional(&mut *connection)
        .await
        .int_err()?;

        Ok(access_token_row_maybe.map(Into::into))
    }
}

#[async_trait::async_trait]
impl AccessTokenRepository for MysqlAccessTokenRepository {
    async fn save_access_token(
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
                  VALUES (?, ?, ?, ?, ?)
              "#,
            access_token.id.to_string(),
            access_token.token_name,
            access_token.token_hash.to_vec(),
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

        let maybe_access_token = self
            .query_token_by_id(token_id, connection_mut)
            .await
            .map_err(GetAccessTokenError::Internal)?;

        if let Some(access_token) = maybe_access_token {
            Ok(access_token)
        } else {
            Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }))
        }
    }

    async fn get_access_tokens_count_by_account_id(
        &self,
        account_id: &AccountID,
    ) -> Result<usize, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let access_token_count = sqlx::query_scalar!(
            r#"
                SELECT
                    count(*)
                FROM access_tokens
                WHERE account_id = ?
            "#,
            account_id.to_string(),
        )
        .fetch_one(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        Ok(usize::try_from(access_token_count).unwrap_or(0))
    }

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &AccountID,
        pagination: &DatabasePaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;

        let access_token_rows = sqlx::query_as!(
            AccessTokenRowModel,
            r#"
              SELECT
                    id as "id: sqlx::types::uuid::fmt::Simple",
                    token_name,
                    token_hash as "token_hash: _",
                    created_at,
                    revoked_at,
                    account_id as "account_id: _"
              FROM access_tokens
              WHERE account_id = ?
              LIMIT ? OFFSET ?
              "#,
            account_id.to_string(),
            pagination.limit,
            pagination.offset,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
        .map_err(GetAccessTokenError::Internal)?;

        Ok(access_token_rows.into_iter().map(Into::into).collect())
    }

    async fn mark_revoked(
        &self,
        token_id: &Uuid,
        revoked_time: DateTime<Utc>,
    ) -> Result<(), RevokeTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(RevokeTokenError::Internal)?;

        let maybe_existing_token = self
            .query_token_by_id(token_id, connection_mut)
            .await
            .map_err(RevokeTokenError::Internal)?;

        if maybe_existing_token.is_none() {
            return Err(RevokeTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }));
        } else if let Some(existing_access_token) = maybe_existing_token
            && existing_access_token.revoked_at.is_some()
        {
            return Err(RevokeTokenError::AlreadyRevoked);
        }

        sqlx::query!(
            r#"
              UPDATE access_tokens SET revoked_at = ? where id = ?
          "#,
            revoked_time,
            token_id.to_string(),
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()
        .map_err(RevokeTokenError::Internal)?;

        Ok(())
    }

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
        token_hash: [u8; 32],
    ) -> Result<Account, FindAccountByTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountByTokenError::Internal)?;

        let maybe_account_row = sqlx::query_as!(
            AccountWithTokenRowModel,
            r#"
              SELECT
                  at.token_hash as "token_hash: _",
                  a.id as "id: _",
                  a.account_name,
                  a.email as "email?",
                  a.display_name,
                  a.account_type as "account_type: AccountType",
                  a.avatar_url,
                  a.registered_at,
                  a.is_admin as "is_admin: _",
                  a.provider,
                  a.provider_identity_key
              FROM access_tokens at
              INNER JOIN accounts a ON a.id = account_id
              WHERE at.id = ? AND revoked_at IS null
              "#,
            token_id.to_string()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindAccountByTokenError::Internal)?;

        if let Some(account_row) = maybe_account_row {
            if token_hash != account_row.token_hash.as_slice() {
                return Err(FindAccountByTokenError::InvalidTokenHash);
            }
            Ok(account_row.into())
        } else {
            Err(FindAccountByTokenError::NotFound(
                AccessTokenNotFoundError {
                    access_token_id: *token_id,
                },
            ))
        }
    }
}
