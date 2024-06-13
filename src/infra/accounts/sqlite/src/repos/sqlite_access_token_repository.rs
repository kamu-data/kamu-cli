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

pub struct SqliteAccessTokenRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn AccessTokenRepository)]
impl SqliteAccessTokenRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccessTokenRepository for SqliteAccessTokenRepository {
    async fn save_access_token(
        &self,
        access_token: &AccessToken,
    ) -> Result<(), CreateAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(CreateAccessTokenError::Internal)?;

        let token_id = access_token.id;
        let token_name = access_token.token_name.clone();
        let token_hash = access_token.token_hash.as_slice();
        let crated_at = access_token.created_at;
        let account_id = access_token.account_id.to_string();

        sqlx::query!(
            r#"
                INSERT INTO access_tokens (id, token_name, token_hash, created_at, account_id)
                    VALUES ($1, $2, $3, $4, $5)
                "#,
            token_id,
            token_name,
            token_hash,
            crated_at,
            account_id,
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
        let token_id_search = *token_id;

        let maybe_access_token_row = sqlx::query!(
            r#"
                SELECT
                    id as "id: Uuid",
                    token_name,
                    token_hash,
                    created_at as "created_at: DateTime<Utc>",
                    revoked_at as "revoked_at: DateTime<Utc>",
                    account_id as "account_id: AccountID"
                FROM access_tokens
                WHERE id = $1
                "#,
            token_id_search
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

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &AccountID,
        pagination: &AccessTokenPaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccessTokenError::Internal)?;
        let limit = pagination.limit;
        let offset = pagination.offset;
        let account_id_string = account_id.to_string();

        let access_token_rows = sqlx::query!(
            r#"
                SELECT
                    id as "id: Uuid",
                    token_name,
                    token_hash,
                    created_at as "created_at: DateTime<Utc>",
                    revoked_at as "revoked_at: DateTime<Utc>",
                    account_id as "account_id: AccountID"
                FROM access_tokens
                WHERE account_id = $1
                LIMIT $2 OFFSET $3
                "#,
            account_id_string,
            limit,
            offset,
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
    ) -> Result<(), RevokeTokenError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(RevokeTokenError::Internal)?;

        let existing_access_token =
            self.get_token_by_id(token_id)
                .await
                .map_err(|err| match err {
                    GetAccessTokenError::NotFound(e) => RevokeTokenError::NotFound(e),
                    GetAccessTokenError::Internal(e) => RevokeTokenError::Internal(e),
                })?;
        if existing_access_token.revoked_at.is_some() {
            return Err(RevokeTokenError::AlreadyRevoked);
        }

        sqlx::query!(
            r#"
                UPDATE access_tokens SET revoked_at = $1 where id = $2
            "#,
            revoked_time,
            token_id,
        )
        .execute(connection_mut)
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
        let token_id_search = *token_id;

        let maybe_account_row = sqlx::query!(
            r#"
                SELECT
                    at.token_hash,
                    a.id as "id: AccountID",
                    a.account_name,
                    a.email as "email?",
                    a.display_name,
                    a.account_type as "account_type: AccountType",
                    registered_at as "registered_at: DateTime<Utc>",
                    a.avatar_url,
                    a.is_admin,
                    a.provider,
                    a.provider_identity_key
                FROM access_tokens at
                JOIN accounts a ON at.account_id = a.id
                WHERE at.id = $1
                "#,
            token_id_search
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindAccountByTokenError::Internal)?;

        if let Some(account_row) = maybe_account_row {
            if token_hash != account_row.token_hash.as_slice() {
                return Err(FindAccountByTokenError::InvalidTokenHash);
            }
            Ok(Account {
                id: account_row.id,
                account_name: AccountName::new_unchecked(&account_row.account_name),
                email: account_row.email,
                display_name: account_row.display_name,
                account_type: account_row.account_type,
                avatar_url: account_row.avatar_url,
                registered_at: account_row.registered_at,
                is_admin: account_row.is_admin != 0,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
        } else {
            Err(FindAccountByTokenError::NotFound(
                AccessTokenNotFoundError {
                    access_token_id: *token_id,
                },
            ))
        }
    }
}
