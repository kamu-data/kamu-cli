// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;
use opendatafabric::{AccountID, DatasetID, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresDatasetEntryRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
    listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
}

#[component(pub)]
#[interface(dyn DatasetEntryRepository)]
impl PostgresDatasetEntryRepository {
    pub fn new(
        transaction: TransactionRef,
        listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
    ) -> Self {
        Self {
            transaction: transaction.into(),
            listeners,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for PostgresDatasetEntryRepository {
    async fn dataset_entries_count(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let dataset_entries_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM dataset_entries
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(dataset_entries_count.unwrap_or(0)).unwrap())
    }

    async fn dataset_entries_count_by_owner_id(
        &self,
        owner_id: &AccountID,
    ) -> Result<usize, InternalError> {
        let stack_owner_id = owner_id.as_did_str().to_stack_string();

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let dataset_entries_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM dataset_entries
            WHERE owner_id = $1
            "#,
            stack_owner_id.as_str()
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(dataset_entries_count.unwrap_or(0)).unwrap())
    }

    fn get_dataset_entries(&self, pagination: PaginationOpts) -> DatasetEntryStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query_as!(
                DatasetEntryRowModel,
                r#"
                SELECT
                    dataset_id   as "id: _",
                    owner_id     as "owner_id: _",
                    dataset_name as name,
                    created_at   as "created_at: _"
                FROM dataset_entries
                ORDER BY dataset_name ASC
                LIMIT $1 OFFSET $2
                "#,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;
            while let Some(entry) = query_stream.try_next().await? {
                yield Ok(entry.into());
            }
        })
    }

    async fn get_dataset_entry(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let maybe_dataset_entry_row = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   dataset_name as name,
                   created_at   as "created_at: _"
            FROM dataset_entries
            WHERE dataset_id = $1
            ORDER BY created_at
            "#,
            stack_dataset_id.as_str(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(dataset_entry_row.into())
        } else {
            Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into())
        }
    }

    async fn get_multiple_dataset_entries(
        &self,
        dataset_ids: &[DatasetID],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let dataset_ids_search = dataset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let resolved_entries = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   dataset_name as name,
                   created_at   as "created_at: _"
            FROM dataset_entries
            WHERE dataset_id = ANY($1)
            ORDER BY dataset_id
            "#,
            &dataset_ids_search,
        )
        .map(Into::into)
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let resolved_dataset_ids: HashSet<_> = resolved_entries
            .iter()
            .map(|entry: &DatasetEntry| &entry.id)
            .cloned()
            .collect();

        let unresolved_entries = dataset_ids
            .iter()
            .filter(|id| !resolved_dataset_ids.contains(id))
            .cloned()
            .collect();

        Ok(DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        })
    }

    async fn get_dataset_entry_by_owner_and_name(
        &self,
        owner_id: &AccountID,
        name: &DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_owner_id = owner_id.as_did_str().to_stack_string();

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
            stack_owner_id.as_str(),
            name.as_str()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(dataset_entry_row.into())
        } else {
            Err(DatasetEntryByNameNotFoundError::new(owner_id.clone(), name.clone()).into())
        }
    }

    fn get_dataset_entries_by_owner_id(
        &self,
        owner_id: &AccountID,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'_> {
        let stack_owner_id = owner_id.as_did_str().to_stack_string();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query_as!(
                DatasetEntryRowModel,
                r#"
                SELECT dataset_id   as "id: _",
                    owner_id     as "owner_id: _",
                    dataset_name as name,
                    created_at   as "created_at: _"
                FROM dataset_entries
                WHERE owner_id = $1
                LIMIT $2 OFFSET $3
                "#,
                stack_owner_id.as_str(),
                i64::try_from(pagination.limit).unwrap(),
                i64::try_from(pagination.offset).unwrap(),
            )
            .fetch(connection_mut);

            use futures::TryStreamExt;
            while let Some(row) = query_stream.try_next().await.int_err()? {
                yield Ok(row.into());
            }

        })
    }

    async fn save_dataset_entry(
        &self,
        dataset_entry: &DatasetEntry,
    ) -> Result<(), SaveDatasetEntryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_entry.id.as_did_str().to_stack_string();
        let stack_owner_id = dataset_entry.owner_id.as_did_str().to_stack_string();

        sqlx::query!(
            r#"
            INSERT INTO dataset_entries(dataset_id, owner_id, dataset_name, created_at)
                VALUES ($1, $2, $3, $4)
            "#,
            stack_dataset_id.as_str(),
            stack_owner_id.as_str(),
            dataset_entry.name.as_str(),
            dataset_entry.created_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                let postgres_error_message = e.message();
                if postgres_error_message.contains("idx_dataset_entries_owner_id_dataset_name") {
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

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE dataset_entries
                SET dataset_name = $1
                WHERE dataset_id = $2
            "#,
            new_name.as_str(),
            stack_dataset_id.as_str(),
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
        {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

            let delete_result = sqlx::query!(
                r#"
                DELETE FROM dataset_entries WHERE dataset_id = $1
                "#,
                stack_dataset_id.as_str(),
            )
            .execute(&mut *connection_mut)
            .await
            .int_err()?;

            if delete_result.rows_affected() == 0 {
                return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
            }
        }

        for listener in &self.listeners {
            listener
                .on_dataset_entry_removed(dataset_id)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
