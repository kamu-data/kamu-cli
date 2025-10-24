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
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetDataBlockRepository)]
pub struct PostgresDatasetDataBlockRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDataBlockRepository for PostgresDatasetDataBlockRepository {
    async fn has_data_blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let result = sqlx::query_scalar!(
            r#"SELECT 1 FROM dataset_data_blocks WHERE dataset_id = $1 AND block_ref_name = $2 LIMIT 1"#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
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

        let result = sqlx::query_scalar!(
            r#"SELECT 1 FROM dataset_data_blocks WHERE dataset_id = $1 AND block_hash_bin = $2 LIMIT 1"#,
            dataset_id.to_string(),
            block_hash.digest()
        )
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

        let row = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_hash_bin = $2
            "#,
            dataset_id.to_string(),
            block_hash.digest()
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(row.map(|r| DatasetBlock {
            event_kind: r.event_type,
            sequence_number: u64::try_from(r.sequence_number).unwrap(),
            block_hash: odf::Multihash::new(odf::metadata::Multicodec::Sha3_256, &r.block_hash_bin)
                .unwrap(),
            block_payload: bytes::Bytes::from(r.block_payload),
        }))
    }

    async fn get_data_block_size(
        &self,
        dataset_id: &odf::DatasetID,
        block_hash: &odf::Multihash,
    ) -> Result<Option<usize>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let result = sqlx::query_scalar!(
            r#"SELECT LENGTH(block_payload) FROM dataset_data_blocks WHERE dataset_id = $1 AND block_hash_bin = $2"#,
            dataset_id.to_string(),
            block_hash.digest()
        )
        .fetch_optional(conn)
        .await
        .int_err()?;

        Ok(result.flatten().map(|size| usize::try_from(size).unwrap()))
    }

    async fn get_all_data_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetBlock>, DatasetDataBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_data_blocks
            WHERE dataset_id = $1 AND block_ref_name = $2
            ORDER BY sequence_number
            "#,
            dataset_id.to_string(),
            block_ref.as_str()
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|r| DatasetBlock {
                event_kind: r.event_type,
                sequence_number: u64::try_from(r.sequence_number).unwrap(),
                block_hash: odf::Multihash::new(
                    odf::metadata::Multicodec::Sha3_256,
                    &r.block_hash_bin,
                )
                .unwrap(),
                block_payload: bytes::Bytes::from(r.block_payload),
            })
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

        use sqlx::QueryBuilder;

        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO dataset_data_blocks (
                dataset_id,
                block_ref_name,
                event_type,
                sequence_number,
                block_hash_bin,
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
                .push_bind(block.block_hash.digest())
                .push_bind(block.block_payload.as_ref());
        });

        builder.build().execute(conn).await.map_err(|e| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                tracing::warn!(
                    "Unique constraint violation while batch inserting key blocks: {}",
                    e.message()
                );
                // TODO: might be a unique violation on block hash as well
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

        sqlx::query!(
            r#"
            DELETE FROM dataset_data_blocks
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
