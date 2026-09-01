// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    SecretSetEntry,
    SecretSetProjectionRepository,
};
use kamu_resources::ResourceID;
use uuid::Uuid;

#[component]
#[interface(dyn SecretSetProjectionRepository)]
pub struct SqliteSecretSetProjectionRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SecretSetProjectionRepository for SqliteSecretSetProjectionRepository {
    async fn find_entry(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let row = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: _",
                secret_nonce as "secret_nonce: _",
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_secret_set_entries
            WHERE resource_id = $1
              AND resource_generation = $2
              AND secret_key = $3
            "#,
            resource_id,
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
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: _",
                secret_nonce as "secret_nonce: _",
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_secret_set_entries
            WHERE resource_id = $1
              AND resource_generation = $2
            ORDER BY secret_key
            "#,
            resource_id,
            resource_generation,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn get_latest_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                e.entry_id as "entry_id: Uuid",
                e.account_id as "account_id: odf::AccountID",
                e.secret_key as key,
                e.value as "value: _",
                e.secret_nonce as "secret_nonce: _",
                e.created_at as "created_at: DateTime<Utc>",
                e.updated_at as "updated_at: DateTime<Utc>"
            FROM config_secret_set_entries e
            JOIN resources r ON r.resource_id = e.resource_id
                AND r.generation = e.resource_generation
                AND r.deleted_at IS NULL
            WHERE e.resource_id = $1
            ORDER BY e.secret_key
            "#,
            resource_id,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn get_latest_entries_before_generation(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let rows = sqlx::query_as!(
            SecretSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                secret_key as key,
                value as "value: _",
                secret_nonce as "secret_nonce: _",
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_secret_set_entries
            WHERE resource_id = $1
              AND resource_generation = (
                  SELECT MAX(resource_generation)
                  FROM config_secret_set_entries
                  WHERE resource_id = $1
                    AND resource_generation < $2
              )
            ORDER BY secret_key
            "#,
            resource_id,
            resource_generation,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn replace_entries(
        &self,
        resource_id: &ResourceID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_id: &uuid::Uuid = resource_id.as_ref();

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO config_secret_set_entries (
                entry_id,
                resource_id,
                resource_generation,
                account_id,
                secret_key,
                value,
                secret_nonce,
                created_at,
                updated_at
            )
            "#,
        );

        query_builder.push_values(entries, |mut b, entry| {
            b.push_bind(entry.entry_id);
            b.push_bind(*resource_id);
            b.push_bind(resource_generation);
            b.push_bind(entry.account_id.to_string());
            b.push_bind(&entry.key);
            b.push_bind(&entry.value);
            b.push_bind(&entry.secret_nonce);
            b.push_bind(entry.created_at);
            b.push_bind(entry.updated_at);
        });

        let insert_result = query_builder.build().execute(&mut *connection_mut).await;

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
        resource_id: &ResourceID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_id: &uuid::Uuid = resource_id.as_ref();

        sqlx::query!(
            r#"
            DELETE FROM config_secret_set_entries
            WHERE resource_id = $1
              AND resource_generation < $2
            "#,
            resource_id,
            resource_generation,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn delete_all_entries(&self, resource_ids: &[ResourceID]) -> Result<(), InternalError> {
        if resource_ids.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let ids: Vec<uuid::Uuid> = resource_ids.iter().map(|id| *id.as_ref()).collect();

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            "DELETE FROM config_secret_set_entries WHERE resource_id IN (",
        );
        let mut separated = query_builder.separated(", ");
        for id in &ids {
            separated.push_bind(*id);
        }
        query_builder.push(")");

        query_builder
            .build()
            .execute(&mut *connection_mut)
            .await
            .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
