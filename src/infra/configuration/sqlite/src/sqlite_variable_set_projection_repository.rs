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
    VariableSetEntry,
    VariableSetProjectionRepository,
};
use kamu_resources::ResourceUID;
use uuid::Uuid;

#[component]
#[interface(dyn VariableSetProjectionRepository)]
pub struct SqliteVariableSetProjectionRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VariableSetProjectionRepository for SqliteVariableSetProjectionRepository {
    async fn replace_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        entries: &[VariableSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO config_variable_set_entries (
                entry_id,
                resource_uid,
                resource_generation,
                account_id,
                variable_key,
                value,
                created_at,
                updated_at
            )
            "#,
        );

        query_builder.push_values(entries, |mut b, entry| {
            b.push_bind(entry.entry_id);
            b.push_bind(*resource_uid);
            b.push_bind(resource_generation);
            b.push_bind(entry.account_id.to_string());
            b.push_bind(&entry.key);
            b.push_bind(&entry.value);
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

    async fn find_entry(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<VariableSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let row = sqlx::query_as!(
            VariableSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                variable_key as key,
                value,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_variable_set_entries
            WHERE resource_uid = $1
              AND resource_generation = $2
              AND variable_key = $3
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
    ) -> Result<Vec<VariableSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let rows = sqlx::query_as!(
            VariableSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                variable_key as key,
                value,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_variable_set_entries
            WHERE resource_uid = $1
              AND resource_generation = $2
            ORDER BY variable_key
            "#,
            resource_uid,
            resource_generation,
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
    ) -> Result<Vec<VariableSetEntry>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        let rows = sqlx::query_as!(
            VariableSetEntry,
            r#"
            SELECT
                entry_id as "entry_id: Uuid",
                account_id as "account_id: odf::AccountID",
                variable_key as key,
                value,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>"
            FROM config_variable_set_entries
            WHERE resource_uid = $1
              AND resource_generation = (
                  SELECT MAX(resource_generation)
                  FROM config_variable_set_entries
                  WHERE resource_uid = $1
                    AND resource_generation < $2
              )
            ORDER BY variable_key
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
        resource_uid: &ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        sqlx::query!(
            r#"
            DELETE FROM config_variable_set_entries
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
