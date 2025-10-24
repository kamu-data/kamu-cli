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
#[interface(dyn DatasetKeyBlockRepository)]
pub struct PostgresDatasetKeyBlockRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetKeyBlockRepository for PostgresDatasetKeyBlockRepository {
    async fn has_key_blocks_for_ref(
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
    ) -> Result<Vec<DatasetBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let rows = sqlx::query!(
            r#"
            SELECT
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_key_blocks
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

    async fn match_datasets_having_key_blocks(
        &self,
        dataset_ids: &[odf::DatasetID],
        block_ref: &odf::BlockRef,
        event_type: MetadataEventType,
    ) -> Result<Vec<(odf::DatasetID, DatasetBlock)>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_ids: Vec<String> = dataset_ids.iter().map(ToString::to_string).collect();

        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT ON (dataset_id)
                dataset_id,
                event_type as "event_type: MetadataEventType",
                sequence_number,
                block_hash_bin,
                block_payload
            FROM dataset_key_blocks
            WHERE
                dataset_id = ANY($1) AND
                block_ref_name = $2 AND
                event_type = ($3::text)::metadata_event_type
            ORDER BY dataset_id, sequence_number DESC
            "#,
            &dataset_ids,
            block_ref.as_str(),
            event_type.to_string()
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    odf::DatasetID::from_did_str(&r.dataset_id).unwrap(),
                    DatasetBlock {
                        event_kind: r.event_type,
                        sequence_number: u64::try_from(r.sequence_number).unwrap(),
                        block_hash: odf::Multihash::new(
                            odf::metadata::Multicodec::Sha3_256,
                            &r.block_hash_bin,
                        )
                        .unwrap(),
                        block_payload: bytes::Bytes::from(r.block_payload),
                    },
                )
            })
            .collect())
    }

    async fn save_key_blocks_batch(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
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

    async fn delete_all_key_blocks_for_ref(
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
