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
    VariableSetEntry,
    VariableSetProjectionRepository,
};
use kamu_resources::ResourceUID;

#[component]
#[interface(dyn VariableSetProjectionRepository)]
pub struct PostgresVariableSetProjectionRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl VariableSetProjectionRepository for PostgresVariableSetProjectionRepository {
    async fn replace_entries(
        &self,
        resource_uid: &ResourceUID,
        resource_generation: u64,
        entries: &[VariableSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let resource_generation = i64::try_from(resource_generation).unwrap();
        let resource_uid: &uuid::Uuid = resource_uid.as_ref();

        for entry in entries {
            let insert_result = sqlx::query!(
                r#"
                INSERT INTO config_variable_set_entries (
                    entry_id,
                    resource_uid,
                    resource_generation,
                    account_id,
                    variable_key,
                    value,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#,
                entry.entry_id,
                resource_uid,
                resource_generation,
                entry.account_id.to_string(),
                entry.key,
                entry.value,
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
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                variable_key as key,
                value,
                updated_at
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
                entry_id as "entry_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                variable_key as key,
                value,
                updated_at
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
