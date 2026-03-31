// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    SecretSetEntry,
    SecretSetProjectionRepository,
};

#[component]
#[interface(dyn SecretSetProjectionRepository)]
pub struct PostgresSecretSetProjectionRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SecretSetProjectionRepository for PostgresSecretSetProjectionRepository {
    async fn replace_entries(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let resource_generation = i64::try_from(resource_generation).unwrap();

        for entry in entries {
            let insert_result = sqlx::query!(
                r#"
                INSERT INTO config_secret_set_entries (
                    entry_id,
                    resource_uid,
                    resource_generation,
                    account_id,
                    secret_key,
                    value,
                    secret_nonce,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                entry.entry_id,
                resource_uid,
                resource_generation,
                entry.account_id.to_string(),
                entry.key,
                entry.value,
                entry.secret_nonce,
                entry.updated_at,
            )
            .execute(&mut *connection_mut)
            .await;

            match insert_result {
                Ok(_) => {}
                Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                    return Err(ReplaceProjectionEntriesError::concurrent_modification());
                }
                Err(e) => return Err(e.int_err().into()),
            }
        }

        Ok(())
    }

    async fn find_entry(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let resource_generation = i64::try_from(resource_generation).unwrap();

        let row = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: Vec<u8>",
                secret_nonce as "secret_nonce: Vec<u8>",
                updated_at
            FROM config_secret_set_entries
            WHERE resource_uid = $1
              AND resource_generation = $2
              AND secret_key = $3
            "#,
            resource_uid,
            resource_generation,
            key,
        )
        .fetch_optional(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(row)
    }

    async fn get_entries(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let resource_generation = i64::try_from(resource_generation).unwrap();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: Vec<u8>",
                secret_nonce as "secret_nonce: Vec<u8>",
                updated_at
            FROM config_secret_set_entries
            WHERE resource_uid = $1
              AND resource_generation = $2
            ORDER BY secret_key
            "#,
            resource_uid,
            resource_generation,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn cleanup_entries_before_generation(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let resource_generation = i64::try_from(resource_generation).unwrap();

        sqlx::query!(
            r#"
            DELETE FROM config_secret_set_entries
            WHERE resource_uid = $1
              AND resource_generation < $2
            "#,
            resource_uid,
            resource_generation,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
