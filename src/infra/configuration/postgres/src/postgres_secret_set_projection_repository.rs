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
use kamu_resources::ResourceUID;
use odf::metadata::AsStackString;
use odf::metadata::stack_string::StackString;

#[component]
#[interface(dyn SecretSetProjectionRepository)]
pub struct PostgresSecretSetProjectionRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SecretSetProjectionRepository for PostgresSecretSetProjectionRepository {
    async fn find_entry(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let row = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: Vec<u8>",
                secret_nonce as "secret_nonce: Vec<u8>",
                created_at,
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
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: Vec<u8>",
                secret_nonce as "secret_nonce: Vec<u8>",
                created_at,
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

    async fn get_latest_entries(
        &self,
        resource_uid: &ResourceUID,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                e.entry_id as "entry_id: uuid::Uuid",
                e.account_id as "account_id: odf::AccountID",
                e.secret_key as key,
                e.value as "value: Vec<u8>",
                e.secret_nonce as "secret_nonce: Vec<u8>",
                e.created_at,
                e.updated_at
            FROM config_secret_set_entries e
            JOIN resources r ON r.resource_uid = e.resource_uid
                AND r.generation = e.resource_generation
                AND r.deleted_at IS NULL
            WHERE e.resource_uid = $1
            ORDER BY e.secret_key
            "#,
            resource_uid,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn get_latest_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: Vec<u8>",
                secret_nonce as "secret_nonce: Vec<u8>",
                created_at,
                updated_at
            FROM config_secret_set_entries
            WHERE resource_uid = $1
              AND resource_generation = (
                  SELECT MAX(resource_generation)
                  FROM config_secret_set_entries
                  WHERE resource_uid = $1
                    AND resource_generation < $2
              )
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

    async fn replace_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let entry_ids: Vec<_> = entries.iter().map(|e| e.entry_id).collect();
        let account_ids: Vec<_> = entries
            .iter()
            .map(|e| e.account_id.as_stack_string())
            .collect();
        let account_ids_strs = account_ids
            .iter()
            .map(StackString::as_str)
            .collect::<Vec<_>>();
        let keys: Vec<_> = entries.iter().map(|e| e.key.as_str()).collect();
        let values: Vec<_> = entries.iter().map(|e| &e.value).collect();
        let secret_nonces: Vec<_> = entries.iter().map(|e| &e.secret_nonce).collect();
        let created_ats: Vec<_> = entries.iter().map(|e| e.created_at).collect();
        let updated_ats: Vec<_> = entries.iter().map(|e| e.updated_at).collect();

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
                created_at,
                updated_at
            )
            SELECT e.entry_id, $1, $2, e.account_id, e.secret_key, e.value, e.secret_nonce, e.created_at, e.updated_at
            FROM UNNEST($3::uuid[], $4::text[], $5::text[], $6::bytea[], $7::bytea[], $8::timestamptz[], $9::timestamptz[])
                AS e(entry_id, account_id, secret_key, value, secret_nonce, created_at, updated_at)
            "#,
            resource_uid,
            resource_generation,
            &entry_ids,
            &account_ids_strs as &[&str],
            &keys as &[&str],
            &values as &[&Vec<u8>],
            &secret_nonces as &[&Vec<u8>],
            &created_ats,
            &updated_ats,
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

        Ok(())
    }

    async fn cleanup_entries_before_generation(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

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

    async fn delete_all_entries(&self, resource_uid: &ResourceUID) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        sqlx::query!(
            r#"
            DELETE FROM config_secret_set_entries
            WHERE resource_uid = $1
            "#,
            resource_uid,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
