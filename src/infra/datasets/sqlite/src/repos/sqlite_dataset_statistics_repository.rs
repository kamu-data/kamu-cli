// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;
use sqlx::FromRow;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetStatisticsRepository)]
pub struct SqliteDatasetStatisticsRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(FromRow)]
struct DatasetStatisticsRow {
    last_pulled: Option<DateTime<Utc>>,
    num_records: i64,
    data_size: i64,
    checkpoints_size: i64,
    num_object_links: i64,
    object_links_size: i64,
}

#[derive(FromRow)]
struct DatasetStatisticsTotals {
    num_records: i64,
    data_size: i64,
    checkpoints_size: i64,
    num_object_links: i64,
    object_links_size: i64,
}

impl From<DatasetStatisticsTotals> for TotalStatistic {
    fn from(value: DatasetStatisticsTotals) -> Self {
        Self {
            num_records: u64::try_from(value.num_records).unwrap(),
            data_size: u64::try_from(value.data_size).unwrap(),
            checkpoints_size: u64::try_from(value.checkpoints_size).unwrap(),
            num_object_links: u64::try_from(value.num_object_links).unwrap(),
            object_links_size: u64::try_from(value.object_links_size).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetStatisticsRepository for SqliteDatasetStatisticsRepository {
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_row = sqlx::query!("SELECT 1 as count FROM dataset_statistics LIMIT 1")
            .fetch_optional(connection_mut)
            .await
            .int_err()?;

        Ok(maybe_row.is_some())
    }

    async fn get_total_statistic_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<TotalStatistic, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_string = account_id.to_string();

        let record = sqlx::query_as!(
            DatasetStatisticsTotals,
            r#"
                SELECT
                    COALESCE(SUM(num_records), 0) AS num_records,
                    COALESCE(SUM(data_size), 0) AS data_size,
                    COALESCE(SUM(checkpoints_size), 0) AS checkpoints_size,
                    COALESCE(SUM(num_object_links), 0) AS num_object_links,
                    COALESCE(SUM(object_links_size), 0) AS object_links_size
                FROM dataset_statistics ds
                    LEFT JOIN dataset_entries de on ds.dataset_id = de.dataset_id
                WHERE de.owner_id = $1
                "#,
            account_id_string
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(record.into())
    }

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_str = stack_dataset_id.as_str();
        let block_ref_str = block_ref.as_str();

        let maybe_record = sqlx::query_as!(
            DatasetStatisticsRow,
            r#"
            SELECT
                last_pulled as "last_pulled: _",
                num_records,
                data_size,
                checkpoints_size,
                num_object_links,
                object_links_size
            FROM dataset_statistics
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id_str,
            block_ref_str,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(row) = maybe_record {
            Ok(DatasetStatistics {
                last_pulled: row.last_pulled,
                num_records: u64::try_from(row.num_records).unwrap(),
                data_size: u64::try_from(row.data_size).unwrap(),
                checkpoints_size: u64::try_from(row.checkpoints_size).unwrap(),
                num_object_links: u64::try_from(row.num_object_links).unwrap(),
                object_links_size: u64::try_from(row.object_links_size).unwrap(),
            })
        } else {
            Err(DatasetStatisticsNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: block_ref.cheap_clone(),
            }
            .into())
        }
    }

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();
        let stack_dataset_id_str = stack_dataset_id.as_str();
        let block_ref_str = block_ref.as_str();

        let last_pulled = statistics.last_pulled;
        let num_records = i64::try_from(statistics.num_records).unwrap();
        let data_size = i64::try_from(statistics.data_size).unwrap();
        let checkpoints_size = i64::try_from(statistics.checkpoints_size).unwrap();
        let num_object_links = i64::try_from(statistics.num_object_links).unwrap();
        let object_links_size = i64::try_from(statistics.object_links_size).unwrap();

        sqlx::query!(
            r#"
            INSERT INTO dataset_statistics (
                dataset_id,
                block_ref_name,
                last_pulled,
                num_records,
                data_size,
                checkpoints_size,
                num_object_links,
                object_links_size
            )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (dataset_id, block_ref_name)
                DO UPDATE SET
                    last_pulled = excluded.last_pulled,
                    num_records = excluded.num_records,
                    data_size = excluded.data_size,
                    checkpoints_size = excluded.checkpoints_size,
                    num_object_links = EXCLUDED.num_object_links,
                    object_links_size = EXCLUDED.object_links_size
            "#,
            stack_dataset_id_str,
            block_ref_str,
            last_pulled,
            num_records,
            data_size,
            checkpoints_size,
            num_object_links,
            object_links_size,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
