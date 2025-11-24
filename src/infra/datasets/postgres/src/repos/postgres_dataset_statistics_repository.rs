// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn DatasetStatisticsRepository)]
pub struct PostgresDatasetStatisticsRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetStatisticsRepository for PostgresDatasetStatisticsRepository {
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_row = sqlx::query!(r#"SELECT 1 as count FROM dataset_statistics LIMIT 1"#)
            .fetch_optional(connection_mut)
            .await
            .int_err()?;

        Ok(maybe_row.is_some())
    }

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let stack_dataset_id = dataset_id.as_did_str().to_stack_string();

        let maybe_record = sqlx::query!(
            r#"
            SELECT  last_pulled,
                    num_records,
                    data_size,
                    checkpoints_size,
                    num_object_links,
                    object_links_size
            FROM dataset_statistics
                WHERE dataset_id = $1 AND block_ref_name = $2
            "#,
            stack_dataset_id.as_str(),
            block_ref.as_str(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(record) = maybe_record {
            Ok(DatasetStatistics {
                last_pulled: record.last_pulled,
                num_records: u64::try_from(record.num_records).unwrap(),
                data_size_bytes: u64::try_from(record.data_size).unwrap(),
                checkpoints_size_bytes: u64::try_from(record.checkpoints_size).unwrap(),
                num_object_links: u64::try_from(record.num_object_links).unwrap(),
                object_links_size_bytes: u64::try_from(record.object_links_size).unwrap(),
            })
        } else {
            Err(DatasetStatisticsNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: block_ref.cheap_clone(),
            }
            .into())
        }
    }

    async fn get_total_statistic_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<TotalStatistic, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let record = sqlx::query!(
            r#"
                SELECT
                    (COALESCE(SUM(ds.num_records), 0))::BIGINT         AS num_records,
                    (COALESCE(SUM(ds.data_size), 0))::BIGINT           AS data_size,
                    (COALESCE(SUM(ds.checkpoints_size), 0))::BIGINT    AS checkpoints_size,
                    (COALESCE(SUM(ds.num_object_links), 0))::BIGINT    AS num_object_links,
                    (COALESCE(SUM(ds.object_links_size), 0))::BIGINT   AS object_links_size
                FROM dataset_statistics ds
                    LEFT JOIN dataset_entries de ON ds.dataset_id = de.dataset_id
                WHERE de.owner_id = $1
            "#,
            account_id.to_string()
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(TotalStatistic {
            num_records: u64::try_from(record.num_records.unwrap()).unwrap(),
            data_size_bytes: u64::try_from(record.data_size.unwrap()).unwrap(),
            checkpoints_size_bytes: u64::try_from(record.checkpoints_size.unwrap()).unwrap(),
            num_object_links: u64::try_from(record.num_object_links.unwrap()).unwrap(),
            object_links_size_bytes: u64::try_from(record.object_links_size.unwrap()).unwrap(),
        })
    }

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

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
                    last_pulled = EXCLUDED.last_pulled,
                    num_records = EXCLUDED.num_records,
                    data_size = EXCLUDED.data_size,
                    checkpoints_size = EXCLUDED.checkpoints_size,
                    num_object_links = EXCLUDED.num_object_links,
                    object_links_size = EXCLUDED.object_links_size
            "#,
            dataset_id.to_string(),
            block_ref.as_str(),
            statistics.last_pulled,
            i64::try_from(statistics.num_records).unwrap(),
            i64::try_from(statistics.data_size_bytes).unwrap(),
            i64::try_from(statistics.checkpoints_size_bytes).unwrap(),
            i64::try_from(statistics.num_object_links).unwrap(),
            i64::try_from(statistics.object_links_size_bytes).unwrap(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
