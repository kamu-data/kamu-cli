// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use database_common::{PaginationOpts, TransactionRefT};
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetBlockQueryRepository)]
pub struct SqliteDatasetBlockQueryRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow)]
struct DatasetBlockRow {
    event_type: String,
    sequence_number: i64,
    block_hash_bin: Vec<u8>,
    block_payload: Vec<u8>,
}

impl DatasetBlockRow {
    fn into_domain(self) -> DatasetBlock {
        DatasetBlock {
            event_kind: odf::MetadataEventType::from_str(&self.event_type).unwrap(),
            sequence_number: u64::try_from(self.sequence_number).unwrap(),
            block_hash: odf::Multihash::new(
                odf::metadata::Multicodec::Sha3_256,
                &self.block_hash_bin,
            )
            .unwrap(),
            block_payload: odf::MetadataBlockBytes::new(bytes::Bytes::from(self.block_payload)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SqliteDatasetBlockQueryRepository {
    /// Event types as a JSON array of strings, for `json_each`
    fn event_types_param(filter: &DatasetBlocksFilter) -> Option<String> {
        filter.event_types.map(|event_types| {
            let event_types: Vec<_> = event_types
                .event_types()
                .map(|event_type| event_type.to_string())
                .collect();
            serde_json::to_string(&event_types).unwrap()
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Queries with a hash prefix are separate statements, so that the digest range
// is an index condition on `(dataset_id, block_hash_bin)`. Pages are selected
// on narrow columns first, and payloads are read for the page only.
#[async_trait::async_trait]
impl DatasetBlockQueryRepository for SqliteDatasetBlockQueryRepository {
    async fn list_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<DatasetBlock>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();
        let event_types = Self::event_types_param(filter);
        let limit = i64::try_from(pagination.limit).unwrap();
        let offset = i64::try_from(pagination.offset).unwrap();

        let rows = match filter.block_hash_digest_range() {
            BlockHashDigestRange::Empty => return Ok(Vec::new()),
            BlockHashDigestRange::Any => {
                sqlx::query_as!(
                    DatasetBlockRow,
                    r#"
                    WITH page AS (
                        SELECT sequence_number
                        FROM dataset_key_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                        UNION ALL
                        SELECT sequence_number
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                        ORDER BY sequence_number DESC
                        LIMIT $4 OFFSET $5
                    )
                    SELECT
                        event_type as "event_type!",
                        sequence_number as "sequence_number!",
                        block_hash_bin as "block_hash_bin!",
                        block_payload as "block_payload!"
                    FROM (
                        SELECT event_type, sequence_number, block_hash_bin, block_payload
                        FROM dataset_key_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND sequence_number IN (SELECT sequence_number FROM page)
                        UNION ALL
                        SELECT event_type, sequence_number, block_hash_bin, block_payload
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND sequence_number IN (SELECT sequence_number FROM page)
                    )
                    ORDER BY sequence_number DESC
                    "#,
                    dataset_id_str,
                    block_ref_str,
                    event_types,
                    limit,
                    offset,
                )
                .fetch_all(conn)
                .await
            }
            BlockHashDigestRange::Bounded { start, end } => {
                sqlx::query_as!(
                    DatasetBlockRow,
                    r#"
                    WITH page AS (
                        SELECT sequence_number
                        FROM dataset_key_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                            AND block_hash_bin >= $6 AND block_hash_bin < $7
                        UNION ALL
                        SELECT sequence_number
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                            AND block_hash_bin >= $6 AND block_hash_bin < $7
                        ORDER BY sequence_number DESC
                        LIMIT $4 OFFSET $5
                    )
                    SELECT
                        event_type as "event_type!",
                        sequence_number as "sequence_number!",
                        block_hash_bin as "block_hash_bin!",
                        block_payload as "block_payload!"
                    FROM (
                        SELECT event_type, sequence_number, block_hash_bin, block_payload
                        FROM dataset_key_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND sequence_number IN (SELECT sequence_number FROM page)
                        UNION ALL
                        SELECT event_type, sequence_number, block_hash_bin, block_payload
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND sequence_number IN (SELECT sequence_number FROM page)
                    )
                    ORDER BY sequence_number DESC
                    "#,
                    dataset_id_str,
                    block_ref_str,
                    event_types,
                    limit,
                    offset,
                    start,
                    end,
                )
                .fetch_all(conn)
                .await
            }
        }
        .int_err()?;

        Ok(rows.into_iter().map(DatasetBlockRow::into_domain).collect())
    }

    async fn count_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();
        let block_ref_str = block_ref.as_str();
        let event_types = Self::event_types_param(filter);

        let count = match filter.block_hash_digest_range() {
            BlockHashDigestRange::Empty => return Ok(0),
            BlockHashDigestRange::Any => {
                sqlx::query_scalar!(
                    r#"
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM dataset_key_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                        ) + (
                            SELECT COUNT(*)
                            FROM dataset_data_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                        ) AS "count!: i64"
                    "#,
                    dataset_id_str,
                    block_ref_str,
                    event_types,
                )
                .fetch_one(conn)
                .await
            }
            BlockHashDigestRange::Bounded { start, end } => {
                sqlx::query_scalar!(
                    r#"
                    SELECT
                        (
                            SELECT COUNT(*)
                            FROM dataset_key_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                                AND block_hash_bin >= $4 AND block_hash_bin < $5
                        ) + (
                            SELECT COUNT(*)
                            FROM dataset_data_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3 IS NULL OR event_type IN (SELECT value FROM json_each($3)))
                                AND block_hash_bin >= $4 AND block_hash_bin < $5
                        ) AS "count!: i64"
                    "#,
                    dataset_id_str,
                    block_ref_str,
                    event_types,
                    start,
                    end,
                )
                .fetch_one(conn)
                .await
            }
        }
        .int_err()?;

        Ok(usize::try_from(count).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
