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

use database_common::{TransactionRef, TransactionRefT, sqlite_generate_placeholders_list};
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

    async fn get_all_key_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<Vec<DatasetKeyBlock>, DatasetKeyBlockQueryError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();

        let rows = sqlx::query_as!(
            DatasetKeyBlockRow,
            r#"
            SELECT
                event_type,
                sequence_number,
                block_hash,
                block_payload
            FROM dataset_key_blocks
            WHERE dataset_id = ? AND block_ref_name = ?
            ORDER BY sequence_number ASC
            "#,
            dataset_id_str,
            block_ref_str,
        )
        .fetch_all(conn)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(DatasetKeyBlockRow::into_domain)
            .collect())
    }

    async fn match_datasets_having_blocks(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        block_ref: &odf::BlockRef,
        event_type: MetadataEventType,
    ) -> Result<Vec<(odf::DatasetID, DatasetKeyBlock)>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_ids: Vec<String> = dataset_ids.into_iter().map(|id| id.to_string()).collect();

        use sqlx::Row;

        let query_str = format!(
            r#"
            SELECT
                dkb.dataset_id,
                dkb.event_type as "event_type: _",
                dkb.sequence_number,
                dkb.block_hash,
                dkb.block_payload
            FROM dataset_key_blocks dkb
            JOIN (
                SELECT
                    dataset_id,
                    MAX(sequence_number) AS max_seq
                FROM dataset_key_blocks
                WHERE
                    block_ref_name = $1 AND
                    event_type = $2 AND
                    dataset_id IN ({})
                GROUP BY dataset_id
            ) latest
            ON
                dkb.dataset_id = latest.dataset_id AND
                dkb.sequence_number = latest.max_seq
            WHERE
                dkb.block_ref_name = $1 AND
                dkb.event_type = $2 AND
                dkb.dataset_id IN ({})
            "#,
            // Generate same list twice to avoid duplicate binding
            sqlite_generate_placeholders_list(dataset_ids.len(), NonZeroUsize::new(3).unwrap()),
            sqlite_generate_placeholders_list(dataset_ids.len(), NonZeroUsize::new(3).unwrap()),
        );

        let mut query = sqlx::query(&query_str);
        query = query.bind(block_ref.as_str()).bind(event_type.to_string());
        for dataset_id in dataset_ids {
            query = query.bind(dataset_id);
        }

        let raw_rows = query.fetch_all(conn).await.int_err()?;

        Ok(raw_rows
            .into_iter()
            .map(|r| {
                let dataset_id = odf::DatasetID::from_did_str(r.get::<&str, _>(0)).unwrap();
                let event_type: String = r.get(1);
                let sequence_number: i64 = r.get(2);
                let block_hash: String = r.get(3);
                let block_payload: Vec<u8> = r.get(4);

                let key_block = DatasetKeyBlock {
                    event_kind: MetadataEventType::from_str(&event_type).unwrap(),
                    sequence_number: u64::try_from(sequence_number).unwrap(),
                    block_hash: odf::Multihash::from_multibase(&block_hash).unwrap(),
                    block_payload: bytes::Bytes::from(block_payload),
                };
                (dataset_id, key_block)
            })
            .collect())
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
