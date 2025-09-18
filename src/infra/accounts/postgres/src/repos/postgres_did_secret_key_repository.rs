// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use internal_error::ResultIntoInternal;
use kamu_accounts::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DidSecretKeyRepository)]
pub struct PostgresDidSecretKeyRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DidSecretKeyRepository for PostgresDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO did_secret_keys (entity_id, entity_type, secret_key, secret_nonce)
            VALUES ($1, $2, $3, $4)
            "#,
            &entity.entity_id,
            entity.entity_type as DidEntityType,
            did_secret_key.secret_key,
            did_secret_key.secret_nonce,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn get_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<DidSecretKey, GetDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_secret_key = sqlx::query_as!(
            DidSecretKey,
            r#"
            SELECT secret_key, secret_nonce
            FROM did_secret_keys
            WHERE entity_type = $1
              AND entity_id = $2
            "#,
            entity.entity_type as DidEntityType,
            &entity.entity_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(secret_key) = maybe_secret_key {
            Ok(secret_key)
        } else {
            Err(DidSecretKeyNotFoundError::new(entity).into())
        }
    }

    async fn delete_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<(), DeleteDidSecretKeyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM did_secret_keys
            WHERE entity_type = $1
              AND entity_id = $2
            "#,
            entity.entity_type as DidEntityType,
            &entity.entity_id,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(DidSecretKeyNotFoundError::new(entity).into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
