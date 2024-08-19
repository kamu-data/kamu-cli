// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use opendatafabric::DatasetID;
use uuid::Uuid;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresDatasetEnvVarRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn DatasetEnvVarRepository)]
impl PostgresDatasetEnvVarRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl DatasetEnvVarRepository for PostgresDatasetEnvVarRepository {
    async fn save_dataset_env_var(
        &self,
        dataset_env_var: &DatasetEnvVar,
    ) -> Result<(), SaveDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(SaveDatasetEnvVarError::Internal)?;

        sqlx::query!(
            r#"
                INSERT INTO dataset_env_vars (id, key, value, secret_nonce, created_at, dataset_id)
                    VALUES ($1, $2, $3, $4, $5, $6)
                "#,
            dataset_env_var.id,
            dataset_env_var.key,
            dataset_env_var.value,
            dataset_env_var.secret_nonce,
            dataset_env_var.created_at,
            dataset_env_var.dataset_id.to_string(),
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    SaveDatasetEnvVarError::Duplicate(SaveDatasetEnvVarErrorDuplicate {
                        dataset_env_var_key: dataset_env_var.key.clone(),
                        dataset_id: dataset_env_var.dataset_id.clone(),
                    })
                } else {
                    SaveDatasetEnvVarError::Internal(e.int_err())
                }
            }
            _ => SaveDatasetEnvVarError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn get_all_dataset_env_vars_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
        pagination: &PaginationOpts,
    ) -> Result<Vec<DatasetEnvVar>, GetDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEnvVarError::Internal)?;

        let dataset_env_var_rows = sqlx::query_as!(
            DatasetEnvVarRowModel,
            r#"
            SELECT
                id,
                key,
                value as "value: _",
                secret_nonce,
                created_at,
                dataset_id as "dataset_id: _"
            FROM dataset_env_vars
            WHERE dataset_id = $1
            LIMIT $2 OFFSET $3
            "#,
            dataset_id.to_string(),
            i64::try_from(pagination.limit).unwrap(),
            i64::try_from(pagination.offset).unwrap(),
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
        .map_err(GetDatasetEnvVarError::Internal)?;

        Ok(dataset_env_var_rows.into_iter().map(Into::into).collect())
    }

    async fn get_all_dataset_env_vars_count_by_dataset_id(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<usize, GetDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEnvVarError::Internal)?;

        let dataset_env_vars_count = sqlx::query_scalar!(
            r#"
            SELECT
                count(*)
            FROM dataset_env_vars
            WHERE dataset_id = $1
            "#,
            dataset_id.to_string(),
        )
        .fetch_one(connection_mut)
        .await
        .int_err()
        .map_err(GetDatasetEnvVarError::Internal)?;

        Ok(usize::try_from(dataset_env_vars_count.unwrap_or(0)).unwrap())
    }

    async fn get_dataset_env_var_by_key_and_dataset_id(
        &self,
        dataset_env_var_key: &str,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEnvVarError::Internal)?;

        let dataset_env_var_row_maybe = sqlx::query_as!(
            DatasetEnvVarRowModel,
            r#"
            SELECT
                id,
                key,
                value as "value: _",
                secret_nonce,
                created_at,
                dataset_id as "dataset_id: _"
            FROM dataset_env_vars
            WHERE dataset_id = $1
            and key = $2
            "#,
            dataset_id.to_string(),
            dataset_env_var_key,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetDatasetEnvVarError::Internal)?;

        if let Some(dataset_env_var_row) = dataset_env_var_row_maybe {
            return Ok(dataset_env_var_row.into());
        }
        Err(GetDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_key.to_string(),
            },
        ))
    }

    async fn get_dataset_env_var_by_id(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEnvVarError::Internal)?;

        let dataset_env_var_row_maybe = sqlx::query_as!(
            DatasetEnvVarRowModel,
            r#"
            SELECT
                id,
                key,
                value as "value: _",
                secret_nonce,
                created_at,
                dataset_id as "dataset_id: _"
            FROM dataset_env_vars
            WHERE id = $1
            "#,
            dataset_env_var_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetDatasetEnvVarError::Internal)?;

        if let Some(dataset_env_var_row) = dataset_env_var_row_maybe {
            return Ok(dataset_env_var_row.into());
        }
        Err(GetDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_id.to_string(),
            },
        ))
    }

    async fn delete_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(DeleteDatasetEnvVarError::Internal)?;

        let delete_result = sqlx::query!(
            r#"
                DELETE FROM dataset_env_vars where id = $1
            "#,
            dataset_env_var_id,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()
        .map_err(DeleteDatasetEnvVarError::Internal)?;

        if delete_result.rows_affected() == 0 {
            return Err(DeleteDatasetEnvVarError::NotFound(
                DatasetEnvVarNotFoundError {
                    dataset_env_var_key: dataset_env_var_id.to_string(),
                },
            ));
        }
        Ok(())
    }

    async fn modify_dataset_env_var(
        &self,
        dataset_env_var_id: &Uuid,
        new_value: Vec<u8>,
        secret_nonce: Option<Vec<u8>>,
    ) -> Result<(), ModifyDatasetEnvVarError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(ModifyDatasetEnvVarError::Internal)?;

        let update_result = sqlx::query!(
            r#"
                UPDATE dataset_env_vars SET value = $1, secret_nonce = $2 where id = $3
            "#,
            new_value,
            secret_nonce,
            dataset_env_var_id,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()
        .map_err(ModifyDatasetEnvVarError::Internal)?;

        if update_result.rows_affected() == 0 {
            return Err(ModifyDatasetEnvVarError::NotFound(
                DatasetEnvVarNotFoundError {
                    dataset_env_var_key: dataset_env_var_id.to_string(),
                },
            ));
        }
        Ok(())
    }
}
