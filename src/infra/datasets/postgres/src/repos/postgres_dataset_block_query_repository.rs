// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRefT};
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;
use odf::MetadataEventType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetBlockQueryRepository)]
pub struct PostgresDatasetBlockQueryRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow)]
struct DatasetBlockRow {
    event_type: MetadataEventType,
    sequence_number: i64,
    block_hash_bin: Vec<u8>,
    block_payload: Vec<u8>,
}

impl DatasetBlockRow {
    fn into_domain(self) -> DatasetBlock {
        DatasetBlock {
            event_kind: self.event_type,
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

impl PostgresDatasetBlockQueryRepository {
    fn event_types_param(filter: &DatasetBlocksFilter) -> Option<Vec<String>> {
        filter.event_types.map(|event_types| {
            event_types
                .event_types()
                .map(|event_type| event_type.to_string())
                .collect()
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Queries with a hash prefix are separate statements, so that the digest range
// stays an index condition on `(dataset_id, block_hash_bin)` in generic plans.
// Pages are selected on narrow columns first, and payloads are read for the
// page only.
#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl DatasetBlockQueryRepository for PostgresDatasetBlockQueryRepository {
    #[tracing::instrument(
        name = PostgresDatasetBlockQueryRepository_list_blocks
        level = "debug",
        skip_all,
    )]
    async fn list_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<DatasetBlock>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();
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
                            AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                        UNION ALL
                        SELECT sequence_number
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                        ORDER BY sequence_number DESC
                        LIMIT $4 OFFSET $5
                    )
                    SELECT
                        event_type as "event_type!: MetadataEventType",
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
                    ) AS blocks
                    ORDER BY sequence_number DESC
                    "#,
                    dataset_id,
                    block_ref.as_str(),
                    event_types as Option<Vec<String>>,
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
                            AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                            AND block_hash_bin >= $6 AND block_hash_bin < $7
                        UNION ALL
                        SELECT sequence_number
                        FROM dataset_data_blocks
                        WHERE dataset_id = $1 AND block_ref_name = $2
                            AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                            AND block_hash_bin >= $6 AND block_hash_bin < $7
                        ORDER BY sequence_number DESC
                        LIMIT $4 OFFSET $5
                    )
                    SELECT
                        event_type as "event_type!: MetadataEventType",
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
                    ) AS blocks
                    ORDER BY sequence_number DESC
                    "#,
                    dataset_id,
                    block_ref.as_str(),
                    event_types as Option<Vec<String>>,
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

    #[tracing::instrument(
        name = PostgresDatasetBlockQueryRepository_count_blocks
        level = "debug",
        skip_all,
    )]
    async fn count_blocks(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        filter: &DatasetBlocksFilter,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let conn = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();
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
                                AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                        ) + (
                            SELECT COUNT(*)
                            FROM dataset_data_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                        ) AS "count!"
                    "#,
                    dataset_id,
                    block_ref.as_str(),
                    event_types as Option<Vec<String>>,
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
                                AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                                AND block_hash_bin >= $4 AND block_hash_bin < $5
                        ) + (
                            SELECT COUNT(*)
                            FROM dataset_data_blocks
                            WHERE dataset_id = $1 AND block_ref_name = $2
                                AND ($3::text[] IS NULL OR event_type = ANY($3::text[]::metadata_event_type[]))
                                AND block_hash_bin >= $4 AND block_hash_bin < $5
                        ) AS "count!"
                    "#,
                    dataset_id,
                    block_ref.as_str(),
                    event_types as Option<Vec<String>>,
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
