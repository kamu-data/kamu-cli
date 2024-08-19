// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryByNameNotFoundError,
    DatasetEntryNameCollisionError,
    DatasetEntryNotFoundError,
    DatasetEntryRepository,
    DatasetEntryRowModel,
    DeleteEntryDatasetError,
    GetDatasetEntriesByOwnerIdError,
    GetDatasetEntryByNameError,
    GetDatasetEntryError,
    SaveDatasetEntryError,
    SaveDatasetEntryErrorDuplicate,
    UpdateDatasetEntryNameError,
};
use opendatafabric::{AccountID, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteDatasetEntryRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn DatasetEntryRepository)]
impl SqliteDatasetEntryRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for SqliteDatasetEntryRepository {
    async fn get_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEntryError::Internal)?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();

        let maybe_dataset_entry_row = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   dataset_name as name,
                   created_at   as "created_at: _"
            FROM dataset_entries
            WHERE dataset_id = $1
            "#,
            dataset_id_as_str,
        )
        .fetch_optional(connection_mut)
        .await
        .map_int_err(GetDatasetEntryError::Internal)?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(dataset_entry_row.into())
        } else {
            Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into())
        }
    }

    async fn get_dataset_entry_by_name(
        &self,
        owner_id: &AccountID,
        name: &DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEntryByNameError::Internal)?;

        let stack_owner_id = owner_id.as_did_str().to_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();
        let dataset_name_as_str = name.as_str();

        let maybe_dataset_entry_row = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   dataset_name as name,
                   created_at   as "created_at: _"
            FROM dataset_entries
            WHERE owner_id = $1
              AND dataset_name = $2
            "#,
            owner_id_as_str,
            dataset_name_as_str
        )
        .fetch_optional(connection_mut)
        .await
        .map_int_err(GetDatasetEntryByNameError::Internal)?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(dataset_entry_row.into())
        } else {
            Err(DatasetEntryByNameNotFoundError::new(owner_id.clone(), name.clone()).into())
        }
    }

    async fn get_dataset_entries_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<Vec<DatasetEntry>, GetDatasetEntriesByOwnerIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetDatasetEntriesByOwnerIdError::Internal)?;

        let stack_owner_id = owner_id.as_did_str().to_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();

        let dataset_entry_rows = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   dataset_name as name,
                   created_at   as "created_at: _"
            FROM dataset_entries
            WHERE owner_id = $1
            "#,
            owner_id_as_str,
        )
        .fetch_all(connection_mut)
        .await
        .map_int_err(GetDatasetEntriesByOwnerIdError::Internal)?;

        Ok(dataset_entry_rows.into_iter().map(Into::into).collect())
    }

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(SaveDatasetEntryError::Internal)?;

        let stack_dataset_id = dataset_entry.id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();
        let stack_owner_id = dataset_entry.owner_id.as_did_str().to_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();
        let dataset_name_as_str = dataset_entry.name.as_str();

        sqlx::query!(
            r#"
            INSERT INTO dataset_entries(dataset_id, owner_id, dataset_name, created_at)
            VALUES ($1, $2, $3, $4)
            "#,
            dataset_id_as_str,
            owner_id_as_str,
            dataset_name_as_str,
            dataset_entry.created_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                let sqlite_error_message = e.message();

                if sqlite_error_message.contains("dataset_entries.dataset_name") {
                    DatasetEntryNameCollisionError::new(dataset_entry.name.clone()).into()
                } else {
                    SaveDatasetEntryErrorDuplicate::new(dataset_entry.id.clone()).into()
                }
            }
            _ => SaveDatasetEntryError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn update_dataset_entry_name(
        &self,
        dataset_id: &DatasetID,
        new_name: &DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(UpdateDatasetEntryNameError::Internal)?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();
        let new_dataset_name_as_str = new_name.as_str();

        let update_result = sqlx::query!(
            r#"
            UPDATE dataset_entries
            SET dataset_name = $2
            WHERE dataset_id = $1
            "#,
            dataset_id_as_str,
            new_dataset_name_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                DatasetEntryNameCollisionError::new(new_name.clone()).into()
            }
            _ => UpdateDatasetEntryNameError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        }

        Ok(())
    }

    async fn delete_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<(), DeleteEntryDatasetError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(DeleteEntryDatasetError::Internal)?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();
        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM dataset_entries
            WHERE dataset_id = $1
            "#,
            dataset_id_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .map_int_err(DeleteEntryDatasetError::Internal)?;

        if delete_result.rows_affected() == 0 {
            return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
