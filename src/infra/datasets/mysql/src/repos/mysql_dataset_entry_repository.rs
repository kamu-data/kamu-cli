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
use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRefT, mysql_generate_placeholders_list};
use internal_error::*;
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DatasetEntryRepository)]
pub struct MySqlDatasetEntryRepository {
    transaction: TransactionRefT<sqlx::MySql>,
    removal_listeners: Vec<Arc<dyn DatasetEntryRemovalListener>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRepository for MySqlDatasetEntryRepository {
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

        let dataset_entries_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM dataset_entries
            WHERE owner_id = ?
            "#,
            stack_owner_id.as_str()
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
                MySqlDatasetEntryRowModel,
                r#"
                SELECT dataset_id AS "dataset_id: _",
                       owner_id   AS "owner_id: _",
                       owner_name,
                       dataset_name,
                       created_at,
                       kind
                FROM dataset_entries
                ORDER BY owner_name, dataset_name
                LIMIT ? OFFSET ?
                "#,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;
            while let Some(entry) = query_stream.try_next().await? {
                yield Ok(DatasetEntry::try_from(entry)?);
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

        let maybe_dataset_entry_row = sqlx::query_as!(
            MySqlDatasetEntryRowModel,
            r#"
            SELECT dataset_id AS "dataset_id: _",
                   owner_id   AS "owner_id: _",
                   owner_name,
                   dataset_name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE dataset_id = ?
            "#,
            stack_dataset_id.as_str(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(DatasetEntry::try_from(dataset_entry_row)?)
        } else {
            Err(DatasetEntryNotFoundError::new(dataset_id.clone()).into())
        }
    }

    async fn get_multiple_dataset_entries<'a>(
        &'a self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<DatasetEntriesResolution, GetMultipleDatasetEntriesError> {
        if dataset_ids.is_empty() {
            return Ok(DatasetEntriesResolution {
                resolved_entries: Vec::new(),
                unresolved_entries: Vec::new(),
            });
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
            r#"
            SELECT dataset_id,
                   owner_id,
                   owner_name,
                   dataset_name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE dataset_id IN
            "#,
        );
        query_builder.push_tuples(dataset_ids, |mut b, dataset_id| {
            b.push_bind(dataset_id.to_string());
        });
        query_builder.push("ORDER BY owner_name, dataset_name");

        let dataset_rows = query_builder
            .build_query_as::<MySqlDatasetEntryRowModel>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let resolved_entries = dataset_rows
            .into_iter()
            .map(DatasetEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;

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
        dataset_name: &odf::DatasetName,
    ) -> Result<DatasetEntry, GetDatasetEntryByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        let stack_owner_id = owner_id.as_stack_string();

        let maybe_dataset_entry_row = sqlx::query_as!(
            MySqlDatasetEntryRowModel,
            r#"
            SELECT dataset_id AS "dataset_id: _",
                   owner_id   AS "owner_id: _",
                   owner_name,
                   dataset_name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE owner_id = ?
              AND dataset_name_lower = lower(?)
            "#,
            stack_owner_id.as_str(),
            dataset_name.as_str()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(dataset_entry_row) = maybe_dataset_entry_row {
            Ok(DatasetEntry::try_from(dataset_entry_row)?)
        } else {
            Err(DatasetEntryByNameNotFoundError::new(owner_id.clone(), dataset_name.clone()).into())
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

        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
            r#"
            SELECT dataset_id,
                   owner_id,
                   owner_name,
                   dataset_name,
                   created_at,
                   kind
            FROM dataset_entries
            WHERE (owner_id, dataset_name_lower) IN
            "#,
        );
        query_builder.push_tuples(
            owner_id_dataset_name_pairs,
            |mut b, (owner_id, dataset_name)| {
                b.push_bind(owner_id.to_string());
                b.push_bind(dataset_name.as_str().to_ascii_lowercase());
            },
        );
        query_builder.push("ORDER BY owner_name, dataset_name");

        let model_rows = query_builder
            .build_query_as::<MySqlDatasetEntryRowModel>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let entries = model_rows
            .into_iter()
            .map(DatasetEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;

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
                MySqlDatasetEntryRowModel,
                r#"
                SELECT dataset_id AS "dataset_id: _",
                       owner_id   AS "owner_id: _",
                       owner_name,
                       dataset_name,
                       created_at,
                       kind
                FROM dataset_entries
                WHERE owner_id = ?
                ORDER BY dataset_name
                LIMIT ? OFFSET ?
                "#,
                owner_id_as_str,
                limit,
                offset
            )
            .fetch(connection_mut);

            use futures::TryStreamExt;
            while let Some(row) = query_stream.try_next().await.int_err()? {
                yield Ok(DatasetEntry::try_from(row)?);
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
        let stack_owner_id = dataset_entry.owner_id.as_stack_string();

        let dataset_entry_kind: DatasetEntryKindRowModel = dataset_entry.kind.into();

        sqlx::query!(
            r#"
            INSERT INTO dataset_entries(dataset_id, owner_id, owner_name, dataset_name, created_at, kind)
                VALUES (?, ?, ?, ?, ?, ?)
            "#,
            stack_dataset_id.as_str(),
            stack_owner_id.as_str(),
            dataset_entry.owner_name.as_str(),
            dataset_entry.name.as_str(),
            dataset_entry.created_at,
            <&'static str>::from(dataset_entry_kind),
        )
        .execute(connection_mut)
        .await
        .map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                let mysql_error_message = e.message();
                tracing::error!(mysql_error_message);

                if mysql_error_message.contains("idx_dataset_entries_owner_id_dataset_name") {
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

        let update_result = sqlx::query!(
            r#"
            UPDATE dataset_entries
            SET dataset_name = ?
            WHERE dataset_id = ?
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
            SET owner_name = ?
            WHERE owner_id = ?
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
                DELETE
                FROM dataset_entries
                WHERE dataset_id = ?
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

    async fn delete_dataset_entries<'a>(
        &self,
        dataset_ids: &[Cow<'a, odf::DatasetID>],
    ) -> Result<DatasetEntriesDeletionResult, DeleteDatasetEntriesError> {
        if dataset_ids.is_empty() {
            return Ok(DatasetEntriesDeletionResult::default());
        }

        let deleted_dataset_ids = {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let dataset_ids_search = dataset_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            let placeholders = mysql_generate_placeholders_list(dataset_ids_search.len());

            // NOTE: MySQL has no DELETE ... RETURNING, so resolve the surviving
            //       rows first, then delete them in the same transaction.
            let select_query_str = format!(
                r#"
                SELECT dataset_id
                FROM dataset_entries
                WHERE dataset_id IN ({placeholders})
                "#
            );

            let mut select_query = sqlx::query(&select_query_str);
            for dataset_id in &dataset_ids_search {
                select_query = select_query.bind(dataset_id);
            }

            let existing_dataset_id_rows = select_query
                .fetch_all(&mut *connection_mut)
                .await
                .int_err()?;

            let existing_dataset_ids = existing_dataset_id_rows
                .into_iter()
                .map(|row| {
                    let dataset_id: String = sqlx::Row::get(&row, 0);
                    odf::DatasetID::from_did_str(&dataset_id).int_err()
                })
                .collect::<Result<Vec<_>, _>>()?;

            if !existing_dataset_ids.is_empty() {
                let delete_query_str = format!(
                    r#"
                    DELETE
                    FROM dataset_entries
                    WHERE dataset_id IN ({placeholders})
                    "#
                );

                let mut delete_query = sqlx::query(&delete_query_str);
                for dataset_id in &dataset_ids_search {
                    delete_query = delete_query.bind(dataset_id);
                }

                delete_query.execute(&mut *connection_mut).await.int_err()?;
            }

            existing_dataset_ids
        };

        let deletion_result = DatasetEntriesDeletionResult::from_deleted_dataset_ids(
            dataset_ids,
            deleted_dataset_ids,
        );

        if !deletion_result.deleted_dataset_ids.is_empty() {
            for listener in &self.removal_listeners {
                listener
                    .on_dataset_entries_removed(&deletion_result.deleted_dataset_ids)
                    .await
                    .int_err()?;
            }
        }

        Ok(deletion_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: MySQL cannot decode DatasetEntryKindRowModel (typed as an inlined enum)
#[derive(Debug, Clone, sqlx::FromRow)]
struct MySqlDatasetEntryRowModel {
    pub dataset_id: odf::DatasetID,
    pub owner_id: odf::AccountID,
    pub owner_name: String,
    pub dataset_name: String,
    pub created_at: DateTime<Utc>,
    pub kind: String,
}

impl TryFrom<MySqlDatasetEntryRowModel> for DatasetEntry {
    type Error = InternalError;

    fn try_from(row: MySqlDatasetEntryRowModel) -> Result<Self, Self::Error> {
        let kind: DatasetEntryKindRowModel = row.kind.as_str().try_into().int_err()?;

        Ok(DatasetEntry {
            id: row.dataset_id,
            owner_id: row.owner_id,
            owner_name: odf::AccountName::new_unchecked(&row.owner_name),
            name: odf::DatasetName::new_unchecked(&row.dataset_name),
            created_at: row.created_at,
            kind: kind.into(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
