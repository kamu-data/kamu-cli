// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.

use std::str::FromStr;

use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetDataBlockRepository)]
pub struct SqliteDatasetDataBlockRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow)]
struct DatasetDataBlockRow {
    event_type: String,
    sequence_number: i64,
    block_hash_bin: Vec<u8>,
    block_payload: Vec<u8>,
}

impl DatasetDataBlockRow {
    fn into_domain(self) -> DatasetBlock {
        DatasetBlock {
            event_kind: MetadataEventType::from_str(&self.event_type).unwrap(),
            sequence_number: u64::try_from(self.sequence_number).unwrap(),
            block_hash: odf::Multihash::new(
                odf::metadata::Multicodec::Sha3_256,
                &self.block_hash_bin,
            )
            .unwrap(),
            block_payload: bytes::Bytes::from(self.block_payload),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDataBlockRepository for SqliteDatasetDataBlockRepository {
    async fn has_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        let result: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT 1
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_ref_name = $2
            LIMIT 1
            "#,
        )
        .bind(dataset_id_str)
        .bind(block_ref_str)
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.is_some())
    }

    async fn contains_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_hash_digest = block_hash.digest();

        let result: Option<i32> = sqlx::query_scalar(
            r#"
            SELECT 1
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_hash_bin = $2
            LIMIT 1
            "#,
        )
        .bind(dataset_id_str)
        .bind(block_hash_digest)
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.is_some())
    }

    async fn get_data_block(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<DatasetBlock>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_hash_digest = block_hash.digest();

        let row = sqlx::query_as!(
            DatasetDataBlockRow,
            r#"
            SELECT
                event_type,
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_hash_bin = $2
            "#,
            dataset_id_str,
            block_hash_digest
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(row.map(DatasetDataBlockRow::into_domain))
    }

    async fn get_data_block_size(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<usize>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_hash_digest = block_hash.digest();

        let result: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT LENGTH(block_payload)
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_hash_bin = $2
            "#,
        )
        .bind(dataset_id_str)
        .bind(block_hash_digest)
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.map(|size| usize::try_from(size).unwrap()))
    }

    async fn get_all_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        let rows = sqlx::query_as!(
            DatasetDataBlockRow,
            r#"
            SELECT
                event_type,
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_ref_name = $2
            ORDER BY sequence_number
            "#,
            dataset_id_str,
            block_ref_str,
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(DatasetDataBlockRow::into_domain)
            .collect())
    }

    async fn save_data_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), DatasetDataBlockSaveError> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let mut builder = sqlx::QueryBuilder::new(
            "INSERT INTO dataset_data_blocks (
                dataset_id,
                block_ref_name,
                event_type,
                sequence_number,
                block_hash_bin,
                block_payload
            ) ",
        );

        builder.push_values(blocks, |mut b, block| {
            b.push_bind(dataset_id.to_string())
                .push_bind(block_ref.as_str())
                .push_bind(block.event_kind.to_string())
                .push_bind(i64::try_from(block.sequence_number).unwrap())
                .push_bind(block.block_hash.digest())
                .push_bind(block.block_payload.as_ref());
        });

        builder.build().execute(conn).await.map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                tracing::warn!(
                    "Unique constraint violation while batch inserting key blocks: {}",
                    e.message()
                );
                DatasetDataBlockSaveError::DuplicateSequenceNumber(
                    // We can't know which block caused it in batch insert
                    blocks.iter().map(|b| b.sequence_number).collect(),
                )
            }
            sqlx::Error::Database(e) if e.is_foreign_key_violation() => {
                tracing::warn!(
                    "Foreign key constraint failed while batch inserting key blocks: {}",
                    e.message()
                );
                DatasetDataBlockSaveError::UnmatchedDatasetEntry(DatasetUnmatchedEntryError {
                    dataset_id: dataset_id.clone(),
                })
            }
            other => other.int_err().into(),
        })?;

        Ok(())
    }

    async fn delete_all_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        sqlx::query!(
            "DELETE FROM dataset_data_blocks WHERE dataset_id = $1 AND block_ref_name = $2",
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
