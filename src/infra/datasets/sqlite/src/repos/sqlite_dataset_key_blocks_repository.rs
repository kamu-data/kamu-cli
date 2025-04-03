// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.

use std::num::NonZeroUsize;
use std::str::FromStr;

use database_common::{sqlite_generate_placeholders_list, TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteDatasetKeyBlockRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlockRepository)]
impl SqliteDatasetKeyBlockRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow)]
struct DatasetKeyBlockRow {
    event_type: String,
    sequence_number: i64,
    block_hash: String,
    block_payload: Vec<u8>,
}

impl DatasetKeyBlockRow {
    fn into_domain(self) -> DatasetKeyBlock {
        DatasetKeyBlock {
            event_kind: MetadataEventType::from_str(&self.event_type).unwrap(),
            sequence_number: u64::try_from(self.sequence_number).unwrap(),
            block_hash: odf::Multihash::from_multibase(&self.block_hash).unwrap(),
            block_payload: bytes::Bytes::from(self.block_payload),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlockRepository for SqliteDatasetKeyBlockRepository {
    async fn has_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        let result: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM dataset_key_blocks WHERE dataset_id = ? AND block_ref_name = ? LIMIT 1",
        )
        .bind(dataset_id_str)
        .bind(block_ref_str)
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.is_some())
    }

    async fn find_latest_block_of_kind(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        kind: MetadataEventType,
    ) -> Result<Option<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();
        let event_type_str = kind.to_string();

        let row = sqlx::query_as!(
            DatasetKeyBlockRow,
            r#"
            SELECT
                event_type,
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
            WHERE dataset_id = ? AND block_ref_name = ? AND event_type = ?
            ORDER BY sequence_number DESC
            LIMIT 1
            "#,
            dataset_id_str,
            block_ref_str,
            event_type_str
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(row.map(DatasetKeyBlockRow::into_domain))
    }

    async fn find_blocks_of_kinds_in_range(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        kinds: &[MetadataEventType],
        min_sequence: Option<u64>,
        max_sequence: u64,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let min_seq = min_sequence
            .map(|min| i64::try_from(min).unwrap())
            .unwrap_or(0);
        let max_seq = i64::try_from(max_sequence).unwrap();

        let query_str = format!(
            r#"
            SELECT
                event_type,
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
            WHERE dataset_id = ? AND block_ref_name = ?
                AND sequence_number BETWEEN ? AND ?
                AND event_type IN ({})
            ORDER BY sequence_number
            "#,
            sqlite_generate_placeholders_list(kinds.len(), NonZeroUsize::new(5).unwrap())
        );

        let mut query = sqlx::query_as::<sqlx::Sqlite, DatasetKeyBlockRow>(&query_str);
        query = query
            .bind(dataset_id.to_string())
            .bind(block_ref.as_str())
            .bind(min_seq)
            .bind(max_seq);
        for kind in kinds {
            query = query.bind(kind.to_string());
        }

        let rows = query.fetch_all(conn).await.int_err()?;

        Ok(rows
            .into_iter()
            .map(DatasetKeyBlockRow::into_domain)
            .collect())
    }

    async fn find_max_sequence_number(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Option<u64>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        let result = sqlx::query_scalar(
            r#"
            SELECT MAX(sequence_number)
                FROM dataset_key_blocks
                WHERE dataset_id = ? AND block_ref_name = ?
            "#,
        )
        .bind(dataset_id_str)
        .bind(block_ref_str)
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.flatten())
    }

    async fn save_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetKeyBlock],
    ) -> Result<(), DatasetKeyBlockSaveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let mut builder = sqlx::QueryBuilder::new(
            "INSERT INTO dataset_key_blocks (
                dataset_id,
                block_ref_name,
                event_type,
                sequence_number,
                block_hash,
                block_payload
            ) ",
        );

        builder.push_values(blocks, |mut b, block| {
            b.push_bind(dataset_id.to_string())
                .push_bind(block_ref.as_str())
                .push_bind(block.event_kind.to_string())
                .push_bind(i64::try_from(block.sequence_number).unwrap())
                .push_bind(block.block_hash.to_string())
                .push_bind(block.block_payload.as_ref());
        });

        builder.build().execute(conn).await.map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                tracing::warn!(
                    "Unique constraint violation while batch inserting key blocks: {}",
                    e.message()
                );
                DatasetKeyBlockSaveError::DuplicateSequenceNumber(
                    // We can't know which block caused it in batch insert
                    blocks.iter().map(|b| b.sequence_number).collect(),
                )
            }
            sqlx::Error::Database(e) if e.is_foreign_key_violation() => {
                tracing::warn!(
                    "Foreign key constraint failed while batch inserting key blocks: {}",
                    e.message()
                );
                DatasetKeyBlockSaveError::UnmatchedDatasetEntry(DatasetUnmatchedEntryError {
                    dataset_id: dataset_id.clone(),
                })
            }
            other => other.int_err().into(),
        })?;

        Ok(())
    }

    async fn delete_blocks_after(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        sequence_number: u64,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();
        let sequence_number = i64::try_from(sequence_number).unwrap();

        sqlx::query!(
            r#"
            DELETE FROM dataset_key_blocks
                WHERE dataset_id = ? AND block_ref_name = ? AND sequence_number > ?
            "#,
            dataset_id_str,
            block_ref_str,
            sequence_number
        )
        .execute(conn)
        .await
        .int_err()?;

        Ok(())
    }

    async fn delete_all_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        sqlx::query!(
            "DELETE FROM dataset_key_blocks WHERE dataset_id = ? AND block_ref_name = ?",
            dataset_id_str,
            block_ref_str
        )
        .execute(conn)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
