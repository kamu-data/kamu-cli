// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

use database_common::{PaginationOpts, TransactionRefT, sqlite_generate_placeholders_list};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;
use sqlx::Row;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetEntryRepository)]
pub struct SqliteDatasetEntryRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
    removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for SqliteDatasetEntryRepository {
    async fn dataset_entries_count(&self) -> Result<usize, DatasetEntriesCountError> {
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

        Ok(usize::try_from(dataset_entries_count).unwrap_or(0))
    }

    async fn dataset_entries_count_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        let stack_owner_id = owner_id.as_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();

        let dataset_entries_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM dataset_entries
            WHERE owner_id = $1
            "#,
            owner_id_as_str
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(dataset_entries_count).unwrap())
    }

    async fn get_dataset_entries<'a>(
        &'a self,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a> {
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
                    owner_name,
                    dataset_name as name,
                    created_at   as "created_at: _",
                    kind         as "kind: _"
                FROM dataset_entries
                ORDER BY owner_name, dataset_name ASC
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
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, GetDatasetEntryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();

        let maybe_dataset_entry_row = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   owner_name,
                   dataset_name as name,
                   created_at   as "created_at: _",
                   kind         as "kind: _"
            FROM dataset_entries
            WHERE dataset_id = $1
            "#,
            dataset_id_as_str,
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

    async fn get_multiple_dataset_entries<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let query_str = format!(
            r#"
            SELECT dataset_id as id,
                   owner_id,
                   owner_name,
                   dataset_name as name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE dataset_id IN ({})
            ORDER BY owner_name, dataset_name
            "#,
            sqlite_generate_placeholders_list(dataset_ids.len(), NonZeroUsize::new(1).unwrap())
        );

        // ToDo replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let mut query = sqlx::query(&query_str);
        for dataset_id in dataset_ids {
            query = query.bind(dataset_id.to_string());
        }

        let dataset_rows = query.fetch_all(connection_mut).await.int_err()?;

        let resolved_entries: Vec<_> = dataset_rows
            .into_iter()
            .map(|row| {
                let entry_row = DatasetEntryRowModel {
                    id: row.get(0),
                    owner_id: row.get(1),
                    owner_name: row.get(2),
                    name: row.get(3),
                    created_at: row.get(4),
                    kind: row.get(5),
                };
                DatasetEntry::from(entry_row)
            })
            .collect();

        let resolved_dataset_ids: HashSet<_> = resolved_entries
            .iter()
            .map(|entry: &DatasetEntry| &entry.id)
            .cloned()
            .collect();

        let unresolved_entries = dataset_ids
            .iter()
            .filter(|id| !resolved_dataset_ids.contains(id))
            .map(|id| id.as_ref().clone())
            .collect();

        Ok(DatasetEntriesResolution {
            resolved_entries,
            unresolved_entries,
        })
    }

    async fn get_dataset_entry_by_owner_and_name(
        &self,
        owner_id: &odf::AccountID,
        name: &odf::DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        let stack_owner_id = owner_id.as_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();
        let dataset_name_as_str = name.as_str();

        let maybe_dataset_entry_row = sqlx::query_as!(
            DatasetEntryRowModel,
            r#"
            SELECT dataset_id   as "id: _",
                   owner_id     as "owner_id: _",
                   owner_name,
                   dataset_name as name,
                   created_at   as "created_at: _",
                   kind         as "kind: _"
            FROM dataset_entries
            WHERE owner_id = $1
              AND dataset_name = $2
            "#,
            owner_id_as_str,
            dataset_name_as_str
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

    async fn get_dataset_entries_by_owner_and_name<'a>(
        &self,
        owner_id_dataset_name_pairs: &'a [&'a (odf::AccountID, odf::DatasetName)],
    ) -> Result<Vec<DatasetEntry>, GetDatasetEntriesByNameError> {
        if owner_id_dataset_name_pairs.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT dataset_id   as id,
                   owner_id,
                   owner_name,
                   dataset_name as name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE (owner_id, dataset_name) IN
            "#,
        );
        query_builder.push_tuples(
            owner_id_dataset_name_pairs,
            |mut b, (owner_id, dataset_name)| {
                b.push_bind(owner_id.to_string());
                b.push_bind(dataset_name.as_str());
            },
        );
        query_builder.push("ORDER BY owner_name, dataset_name");

        let model_rows = query_builder
            .build_query_as::<DatasetEntryRowModel>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let entries = model_rows.into_iter().map(Into::into).collect();

        Ok(entries)
    }

    async fn get_dataset_entries_by_owner_id<'a>(
        &'a self,
        owner_id: &odf::AccountID,
        pagination: PaginationOpts,
    ) -> DatasetEntryStream<'a> {
        use odf::metadata::AsStackString;

        let stack_owner_id = owner_id.as_stack_string();

        let limit = i64::try_from(pagination.limit).unwrap();
        let offset = i64::try_from(pagination.offset).unwrap();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let owner_id_as_str = stack_owner_id.as_str();

            let mut query_stream = sqlx::query_as!(
                DatasetEntryRowModel,
                r#"
                SELECT
                    dataset_id   as "id: _",
                    owner_id     as "owner_id: _",
                    owner_name,
                    dataset_name as name,
                    created_at   as "created_at: _",
                    kind         as "kind: _"
                FROM dataset_entries
                WHERE owner_id = $1
                ORDER BY dataset_name
                LIMIT $2 OFFSET $3
                "#,
                owner_id_as_str,
                limit,
                offset
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

        use odf::metadata::AsStackString;

        let stack_dataset_id = dataset_entry.id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();
        let stack_owner_id = dataset_entry.owner_id.as_stack_string();
        let owner_id_as_str = stack_owner_id.as_str();
        let owner_name_as_str = dataset_entry.owner_name.as_str();
        let dataset_name_as_str = dataset_entry.name.as_str();

        let dataset_entry_kind: DatasetEntryKindRowModel = dataset_entry.kind.into();
        let dataset_entry_kind_str = dataset_entry_kind.to_string();

        sqlx::query!(
            r#"
            INSERT INTO dataset_entries(dataset_id, owner_id, owner_name, dataset_name, created_at, kind)
                VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            dataset_id_as_str,
            owner_id_as_str,
            owner_name_as_str,
            dataset_name_as_str,
            dataset_entry.created_at,
            dataset_entry_kind_str,
        )
        .execute(connection_mut)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                let sqlite_error_message = e.message();
                tracing::error!(sqlite_error_message);

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
        dataset_id: &odf::DatasetID,
        new_name: &odf::DatasetName,
    ) -> Result<(), UpdateDatasetEntryNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let dataset_id_as_str = stack_dataset_id.as_str();
        let new_dataset_name_as_str = new_name.as_str();

        let update_result = sqlx::query!(
            r#"
            UPDATE dataset_entries
                SET dataset_name = $1
                WHERE dataset_id = $2
            "#,
            new_dataset_name_as_str,
            dataset_id_as_str,
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

    async fn update_owner_entries_after_rename(
        &self,
        owner_id: &odf::AccountID,
        new_owner_name: &odf::AccountName,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;
        let new_owner_name_as_str = new_owner_name.as_str();
        let stack_owner_id = owner_id.as_stack_string();
        let stack_owner_id_as_str = stack_owner_id.as_str();

        sqlx::query!(
            r#"
            UPDATE dataset_entries
                SET owner_name = $1
                WHERE owner_id = $2
            "#,
            new_owner_name_as_str,
            stack_owner_id_as_str,
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn delete_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), DeleteEntryDatasetError> {
        {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
            let dataset_id_as_str = stack_dataset_id.as_str();
            let delete_result = sqlx::query!(
                r#"
                DELETE FROM dataset_entries WHERE dataset_id = $1
                "#,
                dataset_id_as_str,
            )
            .execute(&mut *connection_mut)
            .await
            .int_err()?;

            if delete_result.rows_affected() == 0 {
                return Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into());
            }
        }

        for listener in &self.removal_listeners {
            listener
                .on_dataset_entry_removed(dataset_id)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
