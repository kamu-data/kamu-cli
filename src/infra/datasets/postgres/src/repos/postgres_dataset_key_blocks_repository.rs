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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresDatasetKeyBlockRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn DatasetKeyBlockRepository)]
impl PostgresDatasetKeyBlockRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlockRepository for PostgresDatasetKeyBlockRepository {
    async fn has_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let result = sqlx::query_scalar!(
            r#"SELECT 1 FROM dataset_key_blocks WHERE dataset_id = $1 AND block_ref_name = $2 LIMIT 1"#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.is_some())
    }

    async fn get_all_key_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
            WHERE dataset_id = $1 AND block_ref_name = $2
            ORDER BY sequence_number ASC
            "#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|r| DatasetKeyBlock {
                event_kind: r.event_type,
                sequence_number: u64::try_from(r.sequence_number).unwrap(),
                block_hash: odf::Multihash::from_multibase(&r.block_hash).unwrap(),
                block_payload: bytes::Bytes::from(r.block_payload),
            })
            .collect())
    }

    async fn find_latest_block_of_kind(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        kind: MetadataEventType,
    ) -> Result<Option<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let record = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
                WHERE dataset_id = $1 AND block_ref_name = $2 AND event_type = ($3::text)::metadata_event_type
                ORDER BY sequence_number DESC
                LIMIT 1
            "#,
            dataset_id.to_string(),
            block_ref.as_str(),
            kind.to_string()
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(record.map(|r| DatasetKeyBlock {
            event_kind: r.event_type,
            sequence_number: u64::try_from(r.sequence_number).unwrap(),
            block_hash: odf::Multihash::from_multibase(&r.block_hash).unwrap(),
            block_payload: bytes::Bytes::from(r.block_payload),
        }))
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

        let min = min_sequence
            .map(|min| i64::try_from(min).unwrap())
            .unwrap_or(0);

        let event_kinds_search = kinds.iter().map(ToString::to_string).collect::<Vec<_>>();

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
            WHERE dataset_id = $1 AND block_ref_name = $2 AND event_type = ANY(($3::text[])::metadata_event_type[])
                AND sequence_number BETWEEN $4 AND $5
            ORDER BY sequence_number
            "#,
            dataset_id.to_string(),
            block_ref.as_str(),
            &event_kinds_search,
            min,
            i64::try_from(max_sequence).unwrap(),
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|r| DatasetKeyBlock {
                event_kind: r.event_type,
                sequence_number: u64::try_from(r.sequence_number).unwrap(),
                block_hash: odf::Multihash::from_multibase(&r.block_hash).unwrap(),
                block_payload: bytes::Bytes::from(r.block_payload),
            })
            .collect())
    }

    async fn find_max_sequence_number(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Option<u64>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let result = sqlx::query_scalar!(
            r#"
            SELECT MAX(sequence_number)
                FROM dataset_key_blocks
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.flatten().map(|n| u64::try_from(n).unwrap()))
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

        use sqlx::QueryBuilder;

        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO dataset_key_blocks (
                dataset_id,
                block_ref_name,
                event_type,
                sequence_number,
                block_hash,
                block_payload
            )",
        );

        builder.push_values(blocks, |mut b, block| {
            b.push_bind(dataset_id.to_string())
                .push_bind(block_ref.as_str())
                .push("(")
                .push_bind_unseparated(block.event_kind.to_string())
                .push_unseparated(")::metadata_event_type")
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

        sqlx::query!(
            r#"
            DELETE FROM dataset_key_blocks
                WHERE dataset_id = $1 AND block_ref_name = $2 AND sequence_number > $3
            "#,
            dataset_id.to_string(),
            block_ref.as_str(),
            i64::try_from(sequence_number).unwrap(),
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

        sqlx::query!(
            r#"
            DELETE FROM dataset_key_blocks
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
        .execute(conn)
        .await
        .int_err()?;

        Ok(())
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
